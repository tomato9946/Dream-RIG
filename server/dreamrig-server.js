#!/usr/bin/env node
/* ============================================================
 * DREAM RIG — Multiplayer room server (LAN / dependency-free)
 *   รัน:   node dreamrig-server.js [port]        (ค่าเริ่มต้น 8787)
 *   เปิดเกม: วางไฟล์เกมชื่อ DREAM_RIG_v4-0.html (หรือตั้ง GAME_FILE)
 *           แล้วให้ทุกคนเปิด http://<IP เครื่องนี้>:8787
 *   หรือเปิดเกมจากไฟล์/APK แล้วใส่ที่อยู่  ws://<IP>:8787
 * ต้องใช้ Node 16+  (ไม่ต้อง npm install)
 *
 * หมายเหตุ LAN Auto Discovery:
 *   - บน PC ยังต้องกรอก IP หรือใช้เครื่องมือ mDNS ภายนอก
 *   - บน Android APK ใช้ Native NSD Bridge (ดูโฟลเดอร์ android/)
 *   - Server นี้ bind 0.0.0.0:8787 อยู่แล้ว รองรับการค้นหาจาก client
 * ============================================================ */
const http = require('http'), fs = require('fs'), path = require('path'), os = require('os'), crypto = require('crypto');
const PORT = +(process.argv[2] || process.env.PORT || 8787);

// รองรับทั้ง index.html และชื่อไฟล์เกมจริง
const CANDIDATES = [
  process.env.GAME_FILE,
  path.join(__dirname, 'DREAM_RIG_v5.html'),
  path.join(__dirname, 'DREAM_RIG_v4-0.html'),
  path.join(__dirname, 'index.html'),
  path.join(__dirname, 'DREAM_RIG.html'),
].filter(Boolean);
let GAME_FILE = CANDIDATES.find(f => { try { return fs.existsSync(f); } catch (e) { return false; } }) || path.join(__dirname, 'index.html');

const MAX_PRICE = 50_000_000;
const MAGIC = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

/* ---------------- minimal WebSocket ---------------- */
function frame(op, payload) {
  const len = payload.length; let h;
  if (len < 126) h = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(len, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([h, payload]);
}
class Client {
  constructor(sock) {
    this.sock = sock; this.buf = Buffer.alloc(0); this.frag = []; this.alive = true;
    this.id = crypto.randomBytes(4).toString('hex'); this.name = 'ผู้เล่น'; this.room = null;
    sock.on('data', d => { this.buf = Buffer.concat([this.buf, d]); this.parse(); });
    sock.on('close', () => this.close()); sock.on('error', () => this.close());
  }
  send(obj) { if (!this.alive) return; try { this.sock.write(frame(1, Buffer.from(JSON.stringify(obj)))); } catch (e) { this.close(); } }
  parse() {
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = !!(b[0] & 0x80), op = b[0] & 0x0f, masked = !!(b[1] & 0x80); let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > 4_000_000) return this.close();
      const need = off + (masked ? 4 : 0) + len; if (b.length < need) return;
      let p = b.subarray(off + (masked ? 4 : 0), need);
      if (masked) { const m = b.subarray(off, off + 4); p = Buffer.from(p); for (let i = 0; i < p.length; i++) p[i] ^= m[i & 3]; }
      this.buf = b.subarray(need);
      if (op === 8) return this.close();
      if (op === 9) { try { this.sock.write(frame(10, p)); } catch (e) {} continue; }
      if (op === 10) { this.alive = true; continue; }
      if (op === 1 || op === 0) {
        this.frag.push(p);
        if (fin) { const s = Buffer.concat(this.frag).toString('utf8'); this.frag = []; let m; try { m = JSON.parse(s); } catch (e) { continue; } try { onMsg(this, m); } catch (e) { console.error('msg err', e); } }
      }
    }
  }
  close() {
    if (!this.alive) return; this.alive = false;
    try { this.sock.destroy(); } catch (e) {}
    clients.delete(this); onLeave(this, 'disconnect');
  }
}
const clients = new Set();

/* ---------------- rooms ---------------- */
const rooms = new Map(); let roomSeq = 0;
const DIFFS = ['easy', 'normal', 'hard', 'nightmare'];
const clamp = (x, a, b) => Math.max(a, Math.min(b, x));
function pubRoom(r) { return { id: r.id, name: r.name, players: r.players.length, max: r.max, days: r.days, diff: r.diff, locked: !!r.pass, started: r.started }; }
function broadcastRooms() { const list = [...rooms.values()].map(pubRoom); clients.forEach(c => { if (!c.room) c.send({ t: 'rooms', rooms: list }); }); }
function roomState(r) { return { id: r.id, name: r.name, host: r.host, players: r.players.map(p => ({ id: p.id, name: p.name })), max: r.max, days: r.days, diff: r.diff, seed: r.seed, started: r.started }; }
function toRoom(r, msg, except) { r.players.forEach(p => { if (p !== except) p.send(msg); }); }
function cleanText(x, max) { return String(x == null ? '' : x).replace(/[\u0000-\u001f\u007f]/g, ' ').trim().slice(0, max); }
function addChat(r, msg) { r.chat.push(msg); if (r.chat.length > 80) r.chat.shift(); toRoom(r, Object.assign({ t: 'chat' }, msg)); }
function sysChat(r, text, kind) { addChat(r, { sys: true, kind, text, ts: Date.now() }); }
function curDay(r) { return Math.floor(r.global / 3) + 1; }

function recompute(r) {
  if (!r.started || r.over) return;
  const act = r.players.filter(p => !r.finals.has(p.id));
  if (!act.length) return finishGame(r);
  const g = Math.min(...act.map(p => r.prog.get(p.id) || 0));
  if (g !== r.global) {
    const oldDay = curDay(r); r.global = g; const nd = curDay(r);
    toRoom(r, { t: 'global', g, day: nd });
    if (nd > oldDay) expireTrades(r, nd);
  }
}
function expireTrades(r, day) {
  const keep = [];
  r.trades.forEach(tr => {
    if (tr.day < day) { const s = r.players.find(p => p.id === tr.seller); if (s) s.send({ t: 'trade_return', id: tr.id, item: tr.item }); }
    else keep.push(tr);
  });
  r.trades = keep; sendTrades(r);
}
function sendTrades(r) { toRoom(r, { t: 'trades', list: r.trades.map(x => ({ id: x.id, seller: x.seller, sellerName: x.sellerName, item: x.item, price: x.price, day: x.day })) }); }
function finishGame(r) {
  if (r.over) return; r.over = true;
  const board = [...r.finals.entries()].map(([id, f]) => ({ id, name: f.name, score: f.score })).sort((a, b) => b.score - a.score);
  toRoom(r, { t: 'game_over', board });
  setTimeout(() => { rooms.delete(r.id); r.players.forEach(p => p.room = null); broadcastRooms(); }, 60_000);
}
function closeRoom(r, reason) {
  toRoom(r, { t: 'room_closed', reason }); r.players.forEach(p => p.room = null); rooms.delete(r.id); broadcastRooms();
}
function onLeave(c, why) {
  const r = c.room; if (!r) return; c.room = null;
  if (c.id === r.host && !r.over) return closeRoom(r, 'หัวห้องออกจากห้อง ห้องถูกยุบ');
  r.players = r.players.filter(p => p !== c); r.prog.delete(c.id);
  r.trades = r.trades.filter(t => t.seller !== c.id);
  if (r.over) { return; }
  toRoom(r, { t: 'player_left', id: c.id, name: c.name, players: r.players.map(p => ({ id: p.id, name: p.name })) });
  sysChat(r, c.name + ' ออกจากเกม', 'out');
  toRoom(r, { t: 'room', room: roomState(r) }); sendTrades(r);
  if (r.players.length === 0) rooms.delete(r.id); else recompute(r);
  broadcastRooms();
}

function onMsg(c, m) {
  switch (m.t) {
    case 'hello': c.name = cleanText(m.name || 'ผู้เล่น', 20) || 'ผู้เล่น'; c.send({ t: 'welcome', id: c.id }); c.send({ t: 'rooms', rooms: [...rooms.values()].map(pubRoom) }); break;
    case 'list': c.send({ t: 'rooms', rooms: [...rooms.values()].map(pubRoom) }); break;
    case 'create': {
      if (c.room) return;
      const n = ++roomSeq; const r = {
        id: 'r' + n, name: (String(m.name || '').trim().slice(0, 20)) || ('ห้อง' + String(n).padStart(3, '0')),
        pass: String(m.pass || '').slice(0, 20), max: clamp(+m.max || 4, 2, 10), days: clamp(+m.days || 30, 2, 200),
        diff: DIFFS.includes(m.diff) ? m.diff : 'normal', host: c.id, players: [c], started: false, over: false,
        seed: (crypto.randomBytes(4).readUInt32BE(0) % 2147483646) + 1, prog: new Map(), finals: new Map(), claimed: new Set(), trades: [], global: 0, tradeSeq: 0, chat: [], claimedBy: new Map(),
      };
      rooms.set(r.id, r); c.room = r; c.send({ t: 'joined', room: roomState(r) }); broadcastRooms(); break;
    }
    case 'join': {
      if (c.room) return; const r = rooms.get(m.room);
      if (!r) return c.send({ t: 'error', msg: 'ไม่พบห้องนี้แล้ว' });
      if (r.started) return c.send({ t: 'error', msg: 'ห้องนี้เริ่มเล่นไปแล้ว' });
      if (r.players.length >= r.max) return c.send({ t: 'error', msg: 'ห้องเต็มแล้ว' });
      if (r.pass && r.pass !== String(m.pass || '')) return c.send({ t: 'error', msg: 'รหัสผ่านไม่ถูกต้อง', code: 'pass' });
      r.players.push(c); c.room = r; c.send({ t: 'joined', room: roomState(r) });
      c.send({ t: 'chat_history', list: r.chat });
      toRoom(r, { t: 'room', room: roomState(r) }, c); sysChat(r, c.name + ' เข้าร่วมเกม', 'in'); broadcastRooms(); break;
    }
    case 'leave': onLeave(c, 'leave'); c.send({ t: 'left' }); c.send({ t: 'rooms', rooms: [...rooms.values()].map(pubRoom) }); break;
    case 'start': {
      const r = c.room; if (!r || r.host !== c.id || r.started) return;
      if (r.players.length < 2) return c.send({ t: 'error', msg: 'ต้องมีผู้เล่นอย่างน้อย 2 คนถึงเริ่มได้' });
      r.started = true; r.players.forEach(p => r.prog.set(p.id, 0));
      toRoom(r, { t: 'started', room: roomState(r) }); broadcastRooms(); break;
    }
    case 'progress': {
      const r = c.room; if (!r || !r.started || r.over) return;
      const p = clamp(Math.floor(+m.p || 0), 0, r.days * 3 + 3);
      if (p > (r.prog.get(c.id) || 0)) { r.prog.set(c.id, p); recompute(r); }
      break;
    }
    case 'claim': {
      const r = c.room; if (!r || !r.started || typeof m.key !== 'string' || m.key.length > 60) return;
      if (r.claimed.has(m.key)) return c.send({ t: 'claim_result', key: m.key, ok: false });
      r.claimed.add(m.key); r.claimedBy.set(m.key, c.id); c.send({ t: 'claim_result', key: m.key, ok: true }); toRoom(r, { t: 'claimed', key: m.key }, c); break;
    }
    case 'trade_list': {
      const r = c.room; if (!r || !r.started || r.over) return;
      const price = Math.floor(+m.price); if (!(price >= 1 && price <= MAX_PRICE)) return c.send({ t: 'trade_list_fail', reason: 'ราคาต้องอยู่ระหว่าง 1 – 50,000,000' });
      if (!m.item || typeof m.item !== 'object') return;
      const tr = { id: 't' + (++r.tradeSeq), seller: c.id, sellerName: c.name, item: m.item, price, day: curDay(r) };
      r.trades.push(tr); c.send({ t: 'trade_list_ok', id: tr.id, ref: m.ref }); sendTrades(r); break;
    }
    case 'trade_buy': {
      const r = c.room; if (!r) return; const i = r.trades.findIndex(t => t.id === m.id);
      if (i < 0) return c.send({ t: 'trade_buy_fail', id: m.id, reason: 'ขอโทษนะ มีคนซื้อไปแล้ว' });
      const tr = r.trades[i]; if (tr.seller === c.id) return c.send({ t: 'trade_buy_fail', id: m.id, reason: 'ซื้อของตัวเองไม่ได้' });
      r.trades.splice(i, 1);
      c.send({ t: 'trade_buy_ok', id: tr.id, item: tr.item, price: tr.price });
      const s = r.players.find(p => p.id === tr.seller); if (s) s.send({ t: 'trade_sold', id: tr.id, price: tr.price, buyer: c.name });
      sendTrades(r); break;
    }
    case 'chat': {
      const r = c.room; if (!r) return;
      const text = cleanText(m.text, 160); if (!text) return;
      const now = Date.now(); if (c.lastChat && now - c.lastChat < 350) return; c.lastChat = now;
      addChat(r, { id: c.id, name: c.name, text, ts: now }); break;
    }
    case 'final': {
      const r = c.room; if (!r || !r.started || r.over) return;
      r.finals.set(c.id, { name: c.name, score: Math.max(0, Math.floor(+m.score || 0)) });
      r.prog.set(c.id, 1e9); toRoom(r, { t: 'final_in', n: r.finals.size, total: r.players.length });
      if (r.players.every(p => r.finals.has(p.id))) finishGame(r); else recompute(r); break;
    }
  }
}

/* ---------------- http + upgrade ---------------- */
const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url.startsWith('/?') || req.url === '/index.html' || req.url === '/DREAM_RIG_v4-0.html' || req.url === '/DREAM_RIG_v5.html') {
    fs.readFile(GAME_FILE, (e, d) => {
      if (e) {
        res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
        res.end('DREAM RIG server ทำงานอยู่ — วางไฟล์เกม (DREAM_RIG_v4-0.html หรือ index.html) ไว้ข้างไฟล์นี้\nGAME_FILE ที่ใช้: ' + GAME_FILE + '\n');
      } else {
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
        res.end(d);
      }
    });
  } else if (req.url === '/ping') { res.writeHead(200); res.end('ok'); }
  else if (req.url === '/info') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ port: PORT, rooms: [...rooms.values()].map(pubRoom), gameFile: path.basename(GAME_FILE) }));
  } else { res.writeHead(404); res.end('not found'); }
});
server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key']; if (!key) return sock.destroy();
  const acc = crypto.createHash('sha1').update(key + MAGIC).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  sock.setNoDelay(true); const c = new Client(sock); clients.add(c);
});
setInterval(() => { clients.forEach(c => { if (!c.alive) return; try { c.sock.write(frame(9, Buffer.from('p'))); } catch (e) {} }); }, 20000);
server.listen(PORT, '0.0.0.0', () => {
  console.log('DREAM RIG multiplayer server — port ' + PORT);
  console.log('GAME_FILE = ' + GAME_FILE);
  Object.values(os.networkInterfaces()).flat().filter(i => i && i.family === 'IPv4' && !i.internal).forEach(i => {
    console.log('  เปิดเกม: http://' + i.address + ':' + PORT + '   |  WS: ws://' + i.address + ':' + PORT);
  });
  console.log('  (สำหรับ APK: ใช้ Native NSD Bridge เพื่อค้นหาอัตโนมัติ)');
});
module.exports = { server };
