package com.dreamrig.mp;

import android.util.Log;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import android.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Minimal WebSocket + Room server สำหรับรันบนเครื่อง Host (Android)
 * Protocol และ message format เข้ากันกับ dreamrig-server.js
 * รองรับ: hello, list, create, join, leave, start, progress, claim, trade_*, final
 *
 * หมายเหตุ: ใช้ thread pool แยก ไม่บล็อก UI
 */
public class LocalWsServer {
    private static final String TAG = "DreamRigWs";
    private static final String MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    private final int port;
    private volatile int boundPort;
    private ServerSocket serverSocket;
    private volatile boolean running;
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final List<Client> clients = new CopyOnWriteArrayList<>();
    private final Map<String, Room> rooms = new ConcurrentHashMap<>();
    private final AtomicInteger roomSeq = new AtomicInteger(0);
    private final ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();

    public LocalWsServer(int port) {
        this.port = port;
    }

    public synchronized void start() throws IOException {
        if (running && serverSocket != null && !serverSocket.isClosed()) return;

        // พยายามใช้พอร์ตหลักก่อน หากมีโปรเซสอื่นใช้อยู่ให้เลือกพอร์ตสำรอง
        IOException lastError = null;
        int lastCandidate = Math.min(65535, port + 20);
        for (int candidate = port; candidate <= lastCandidate; candidate++) {
            try {
                ServerSocket candidateSocket = new ServerSocket();
                candidateSocket.setReuseAddress(true);
                candidateSocket.bind(new java.net.InetSocketAddress(candidate));
                serverSocket = candidateSocket;
                boundPort = candidate;
                break;
            } catch (IOException e) {
                lastError = e;
            }
        }
        if (serverSocket == null) {
            if (lastError != null) throw lastError;
            throw new IOException("No available port in range " + port + "-" + lastCandidate);
        }
        running = true;
        pool.execute(() -> {
            Log.i(TAG, "WS server listening on 0.0.0.0:" + port);
            // visible via logcat: adb logcat -s DreamRigWs
            while (running) {
                try {
                    Socket sock = serverSocket.accept();
                    sock.setTcpNoDelay(true);
                    pool.execute(() -> handleNewSocket(sock));
                } catch (Exception e) {
                    if (running) Log.e(TAG, "accept", e);
                }
            }
        });
    }

    public int getPort() { return boundPort; }

    public synchronized void stop() {
        running = false;
        try { if (serverSocket != null) serverSocket.close(); } catch (Exception ignored) {}
        for (Client c : clients) c.close();
        clients.clear();
        rooms.clear();
        pool.shutdownNow();
        sched.shutdownNow();
    }

    private void handleNewSocket(Socket sock) {
        try {
            InputStream in = sock.getInputStream();
            OutputStream out = sock.getOutputStream();

            // อ่าน HTTP headers ให้ครบจนเจอ \r\n\r\n
            ByteArrayOutputStream headerBuf = new ByteArrayOutputStream();
            byte[] tmp = new byte[1024];
            String req = null;
            while (true) {
                int n = in.read(tmp);
                if (n < 0) { sock.close(); return; }
                headerBuf.write(tmp, 0, n);
                byte[] all = headerBuf.toByteArray();
                String s = new String(all, StandardCharsets.US_ASCII);
                int idx = s.indexOf("\r\n\r\n");
                if (idx >= 0) {
                    req = s;
                    // เก็บ bytes หลัง header ไว้ให้ Client อ่านต่อ
                    int headerEnd = idx + 4;
                    int leftover = all.length - headerEnd;
                    if (leftover > 0) {
                        // push leftover into client buffer after construct
                        Client c = completeHandshake(sock, in, out, req);
                        if (c != null && leftover > 0) {
                            c.preload(all, headerEnd, leftover);
                            clients.add(c);
                            c.readLoop();
                        } else if (c != null) {
                            clients.add(c);
                            c.readLoop();
                        }
                        return;
                    }
                    break;
                }
                if (headerBuf.size() > 65536) { sock.close(); return; }
            }

            if (req == null) { sock.close(); return; }

            if (!req.toLowerCase().contains("upgrade: websocket")) {
                String body = "ok";
                String resp = "HTTP/1.1 200 OK\r\nContent-Length: " + body.length() + "\r\nConnection: close\r\n\r\n" + body;
                out.write(resp.getBytes(StandardCharsets.US_ASCII));
                sock.close();
                return;
            }

            Client c = completeHandshake(sock, in, out, req);
            if (c != null) {
                clients.add(c);
                Log.i(TAG, "client connected " + c.id);
                c.readLoop();
            }
        } catch (Exception e) {
            Log.e(TAG, "handleNewSocket", e);
            try { sock.close(); } catch (Exception ignored) {}
        }
    }

    private Client completeHandshake(Socket sock, InputStream in, OutputStream out, String req) throws Exception {
        String key = extractHeader(req, "Sec-WebSocket-Key");
        if (key == null) { sock.close(); return null; }
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] digest = md.digest((key.trim() + MAGIC).getBytes(StandardCharsets.US_ASCII));
        String accept = Base64.encodeToString(digest, Base64.NO_WRAP);
        String handshake = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n";
        out.write(handshake.getBytes(StandardCharsets.US_ASCII));
        out.flush();
        return new Client(sock, in, out);
    }

    private static String extractHeader(String req, String name) {
        for (String line : req.split("\r\n")) {
            if (line.regionMatches(true, 0, name + ":", 0, name.length() + 1)) {
                return line.substring(name.length() + 1).trim();
            }
        }
        return null;
    }

    /* ---------- Client ---------- */
    class Client {
        final String id = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String name = "ผู้เล่น";
        Room room;
        final Socket sock;
        final InputStream in;
        final OutputStream out;
        volatile boolean alive = true;
        volatile long lastChat = 0;
        private final ByteBuffer acc = ByteBuffer.allocate(512 * 1024);

        Client(Socket sock, InputStream in, OutputStream out) {
            this.sock = sock; this.in = in; this.out = out;
        }

        void send(JSONObject obj) {
            if (!alive) return;
            try {
                byte[] payload = obj.toString().getBytes(StandardCharsets.UTF_8);
                writeFrame(0x1, payload);
            } catch (Exception e) {
                close();
            }
        }

        private synchronized void writeFrame(int op, byte[] payload) throws IOException {
            int len = payload.length;
            if (len < 126) {
                out.write(0x80 | op);
                out.write(len);
            } else if (len < 65536) {
                out.write(0x80 | op);
                out.write(126);
                out.write((len >> 8) & 0xff);
                out.write(len & 0xff);
            } else {
                out.write(0x80 | op);
                out.write(127);
                for (int i = 7; i >= 0; i--) out.write((int) ((len >> (8 * i)) & 0xff));
            }
            out.write(payload);
            out.flush();
        }

        void preload(byte[] data, int off, int len) {
            if (len <= 0) return;
            // put leftover bytes into acc for parseFrames
            if (acc.remaining() < len) return;
            acc.put(data, off, len);
        }

        void readLoop() {
            byte[] buf = new byte[8192];
            try {
                // ประมวลผล leftover จาก HTTP handshake ก่อน
                if (acc.position() > 0) parseFrames();
                while (alive) {
                    int n = in.read(buf);
                    if (n < 0) break;
                    acc.put(buf, 0, n);
                    parseFrames();
                }
            } catch (Exception e) {
                Log.e(TAG, "readLoop", e);
            } finally {
                close();
            }
        }

        private void parseFrames() throws Exception {
            acc.flip();
            while (acc.remaining() >= 2) {
                acc.mark();
                int b0 = acc.get() & 0xff;
                int b1 = acc.get() & 0xff;
                boolean fin = (b0 & 0x80) != 0;
                int op = b0 & 0x0f;
                boolean masked = (b1 & 0x80) != 0;
                long len = b1 & 0x7f;
                if (len == 126) {
                    if (acc.remaining() < 2) { acc.reset(); break; }
                    len = ((acc.get() & 0xff) << 8) | (acc.get() & 0xff);
                } else if (len == 127) {
                    if (acc.remaining() < 8) { acc.reset(); break; }
                    len = 0;
                    for (int i = 0; i < 8; i++) len = (len << 8) | (acc.get() & 0xff);
                }
                int maskLen = masked ? 4 : 0;
                if (acc.remaining() < maskLen + len) { acc.reset(); break; }
                byte[] mask = null;
                if (masked) {
                    mask = new byte[4];
                    acc.get(mask);
                }
                byte[] payload = new byte[(int) len];
                acc.get(payload);
                if (masked) {
                    for (int i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
                }
                if (op == 8) { close(); return; }
                if (op == 9) { writeFrame(0xA, payload); continue; } // pong
                if (op == 0xA) continue;
                if (op == 1 || op == 0) {
                    String s = new String(payload, StandardCharsets.UTF_8);
                    try {
                        JSONObject m = new JSONObject(s);
                        onMsg(this, m);
                    } catch (Exception e) {
                        Log.w(TAG, "bad json", e);
                    }
                }
            }
            acc.compact();
        }

        void close() {
            if (!alive) return;
            alive = false;
            try { if (sock != null) sock.close(); } catch (Exception ignored) {}
            clients.remove(this);
            onLeave(this);
        }
    }

    /* ---------- Local (in-process) client ----------
     * เครื่อง Host คุยกับเซิร์ฟเวอร์ตัวเองผ่านตรงนี้ ไม่ต้องใช้ ws://127.0.0.1
     * (กันปัญหา cleartext / loopback ใน WebView) */
    public interface LocalListener {
        void onMessage(String json);
        void onClosed();
    }

    class LocalClient extends Client {
        private final LocalListener listener;
        LocalClient(LocalListener l) {
            super(null, null, null);
            this.listener = l;
        }
        @Override void send(JSONObject obj) {
            if (!alive) return;
            try { listener.onMessage(obj.toString()); } catch (Exception e) { Log.w(TAG, "local send", e); }
        }
        @Override void close() {
            boolean wasAlive = alive;
            super.close();
            if (wasAlive) { try { listener.onClosed(); } catch (Exception ignored) {} }
        }
    }

    public Client openLocalClient(LocalListener l) {
        LocalClient c = new LocalClient(l);
        clients.add(c);
        return c;
    }

    public void localReceive(Client c, String json) {
        try {
            onMsg(c, new JSONObject(json));
        } catch (Exception e) {
            Log.w(TAG, "localReceive bad json", e);
        }
    }

    public void closeClient(Client c) {
        if (c != null) c.close();
    }

    /* ---------- Rooms ---------- */
    static class Room {
        String id, name, pass, host, diff;
        int max, days;
        long seed;
        boolean started, over;
        final List<Client> players = new CopyOnWriteArrayList<>();
        final Map<String, Integer> prog = new ConcurrentHashMap<>();
        final Map<String, JSONObject> finals = new ConcurrentHashMap<>();
        final java.util.Set<String> claimed = ConcurrentHashMap.newKeySet();
        final List<JSONObject> trades = new CopyOnWriteArrayList<>();
        final List<JSONObject> chat = new CopyOnWriteArrayList<>();
        int global = 0;
        int tradeSeq = 0;
    }

    private JSONObject pubRoom(Room r) {
        try {
            JSONObject o = new JSONObject();
            o.put("id", r.id);
            o.put("name", r.name);
            o.put("players", r.players.size());
            o.put("max", r.max);
            o.put("days", r.days);
            o.put("diff", r.diff);
            o.put("locked", r.pass != null && !r.pass.isEmpty());
            o.put("started", r.started);
            return o;
        } catch (Exception e) { return new JSONObject(); }
    }

    private JSONObject roomState(Room r) {
        try {
            JSONObject o = new JSONObject();
            o.put("id", r.id);
            o.put("name", r.name);
            o.put("host", r.host);
            JSONArray pl = new JSONArray();
            for (Client p : r.players) {
                JSONObject pp = new JSONObject();
                pp.put("id", p.id);
                pp.put("name", p.name);
                pl.put(pp);
            }
            o.put("players", pl);
            o.put("max", r.max);
            o.put("days", r.days);
            o.put("diff", r.diff);
            o.put("seed", r.seed);
            o.put("started", r.started);
            return o;
        } catch (Exception e) { return new JSONObject(); }
    }

    private void broadcastRooms() {
        JSONArray list = new JSONArray();
        for (Room r : rooms.values()) list.put(pubRoom(r));
        for (Client c : clients) {
            if (c.room == null) {
                try {
                    JSONObject m = new JSONObject();
                    m.put("t", "rooms");
                    m.put("rooms", list);
                    c.send(m);
                } catch (Exception ignored) {}
            }
        }
    }

    private void toRoom(Room r, JSONObject msg, Client except) {
        for (Client p : r.players) {
            if (p != except) p.send(msg);
        }
    }

    /* ---------- helpers: ข้อความ / แชท / คำนวณวันร่วม ---------- */
    private static String cleanText(String x, int max) {
        if (x == null) return "";
        String t = x.replaceAll("[\\u0000-\\u001f\\u007f]", " ").trim();
        if (t.length() > max) t = t.substring(0, max);
        return t;
    }

    private void addChat(Room r, JSONObject msg) {
        try {
            r.chat.add(msg);
            while (r.chat.size() > 80) r.chat.remove(0);
            JSONObject out = new JSONObject(msg.toString());
            out.put("t", "chat");
            toRoom(r, out, null);
        } catch (Exception e) { Log.w(TAG, "addChat", e); }
    }

    private void sysChat(Room r, String text, String kind) {
        try {
            JSONObject m = new JSONObject();
            m.put("sys", true); m.put("kind", kind); m.put("text", text); m.put("ts", System.currentTimeMillis());
            addChat(r, m);
        } catch (Exception e) { Log.w(TAG, "sysChat", e); }
    }

    private int curDay(Room r) { return r.global / 3 + 1; }

    private void sendTrades(Room r) {
        try {
            JSONObject tradesMsg = new JSONObject();
            tradesMsg.put("t", "trades");
            tradesMsg.put("list", new JSONArray(r.trades));
            toRoom(r, tradesMsg, null);
        } catch (Exception e) { Log.w(TAG, "sendTrades", e); }
    }

    /** ของที่ลงขายไว้ข้ามวัน -> คืนเข้าคลังผู้ขาย (เหมือนฝั่ง Node) */
    private void expireTrades(Room r, int day) {
        try {
            List<JSONObject> keep = new ArrayList<>();
            for (JSONObject tr : r.trades) {
                if (tr.optInt("day", 0) < day) {
                    String seller = tr.optString("seller");
                    for (Client p : r.players) {
                        if (p.id.equals(seller)) {
                            JSONObject ret = new JSONObject();
                            ret.put("t", "trade_return");
                            ret.put("id", tr.optString("id"));
                            ret.put("item", tr.getJSONObject("item"));
                            p.send(ret);
                            break;
                        }
                    }
                } else keep.add(tr);
            }
            r.trades.clear();
            r.trades.addAll(keep);
            sendTrades(r);
        } catch (Exception e) { Log.w(TAG, "expireTrades", e); }
    }

    /** คำนวณความคืบหน้า "วันร่วม" ใหม่ (เรียกหลัง progress / final / มีคนออก) */
    private void recompute(Room r) {
        if (!r.started || r.over) return;
        try {
            int g = Integer.MAX_VALUE; int active = 0;
            for (Client pl : r.players) {
                if (r.finals.containsKey(pl.id)) continue;
                active++;
                Integer pp = r.prog.get(pl.id);
                int v = pp == null ? 0 : pp;
                if (v < g) g = v;
            }
            if (active == 0) { finishGame(r); return; }
            if (g != r.global) {
                int oldDay = curDay(r);
                r.global = g;
                int nd = curDay(r);
                JSONObject gm = new JSONObject();
                gm.put("t", "global"); gm.put("g", g); gm.put("day", nd);
                toRoom(r, gm, null);
                if (nd > oldDay) expireTrades(r, nd);
            }
        } catch (Exception e) { Log.w(TAG, "recompute", e); }
    }

    private void finishGame(final Room r) {
        if (r.over) return;
        r.over = true;
        try {
            List<JSONObject> rows = new ArrayList<>();
            for (Map.Entry<String, JSONObject> e : r.finals.entrySet()) {
                JSONObject row = new JSONObject(e.getValue().toString());
                row.put("id", e.getKey());
                rows.add(row);
            }
            rows.sort((a, b) -> Integer.compare(b.optInt("score"), a.optInt("score")));
            JSONArray arr = new JSONArray();
            for (JSONObject o : rows) arr.put(o);
            JSONObject go = new JSONObject();
            go.put("t", "game_over");
            go.put("board", arr);
            toRoom(r, go, null);
        } catch (Exception e) { Log.w(TAG, "finishGame", e); }
        // ห้องที่จบแล้วต้องถูกลบ ไม่งั้นค้างในลิสต์ตลอด
        try {
            sched.schedule(() -> {
                for (Client p : r.players) p.room = null;
                rooms.remove(r.id);
                broadcastRooms();
            }, 60, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }

    private void onLeave(Client c) {
        Room r = c.room;
        if (r == null) return;
        c.room = null;
        if (c.id.equals(r.host) && !r.over) {
            closeRoom(r, "หัวห้องออกจากห้อง ห้องถูกยุบ");
            return;
        }
        r.players.remove(c);
        r.prog.remove(c.id);
        // ลบ trades ของคนนี้
        Iterator<JSONObject> it = r.trades.iterator();
        while (it.hasNext()) {
            try {
                if (c.id.equals(it.next().optString("seller"))) it.remove();
            } catch (Exception ignored) {}
        }
        if (r.over) return;
        try {
            JSONObject left = new JSONObject();
            left.put("t", "player_left");
            left.put("id", c.id);
            left.put("name", c.name);
            JSONArray pl = new JSONArray();
            for (Client p : r.players) {
                JSONObject pp = new JSONObject();
                pp.put("id", p.id);
                pp.put("name", p.name);
                pl.put(pp);
            }
            left.put("players", pl);
            toRoom(r, left, null);
            JSONObject rs = new JSONObject();
            rs.put("t", "room");
            rs.put("room", roomState(r));
            toRoom(r, rs, null);
            sysChat(r, c.name + " ออกจากเกม", "out");
            sendTrades(r);
        } catch (Exception ignored) {}
        if (r.players.isEmpty()) rooms.remove(r.id); else recompute(r);
        broadcastRooms();
    }

    private void closeRoom(Room r, String reason) {
        try {
            JSONObject m = new JSONObject();
            m.put("t", "room_closed");
            m.put("reason", reason);
            toRoom(r, m, null);
        } catch (Exception ignored) {}
        for (Client p : r.players) p.room = null;
        rooms.remove(r.id);
        broadcastRooms();
    }

    private void onMsg(Client c, JSONObject m) {
        try {
            String t = m.optString("t", "");
            switch (t) {
                case "hello": {
                    c.name = cleanText(m.optString("name", "ผู้เล่น"), 20);
                    if (c.name.isEmpty()) c.name = "ผู้เล่น";
                    JSONObject w = new JSONObject();
                    w.put("t", "welcome");
                    w.put("id", c.id);
                    c.send(w);
                    JSONArray list = new JSONArray();
                    for (Room r : rooms.values()) list.put(pubRoom(r));
                    JSONObject roomsMsg = new JSONObject();
                    roomsMsg.put("t", "rooms");
                    roomsMsg.put("rooms", list);
                    c.send(roomsMsg);
                    break;
                }
                case "list": {
                    JSONArray list = new JSONArray();
                    for (Room r : rooms.values()) list.put(pubRoom(r));
                    JSONObject roomsMsg = new JSONObject();
                    roomsMsg.put("t", "rooms");
                    roomsMsg.put("rooms", list);
                    c.send(roomsMsg);
                    break;
                }
                case "create": {
                    if (c.room != null) return;
                    Room r = new Room();
                    int n = roomSeq.incrementAndGet();
                    r.id = "r" + n;
                    String nm = m.optString("name", "").trim();
                    r.name = nm.isEmpty() ? ("ห้อง" + String.format("%03d", n)) : (nm.length() > 20 ? nm.substring(0, 20) : nm);
                    r.pass = m.optString("pass", "");
                    if (r.pass.length() > 20) r.pass = r.pass.substring(0, 20);
                    r.max = Math.max(2, Math.min(10, m.optInt("max", 4)));
                    r.days = Math.max(2, Math.min(200, m.optInt("days", 30)));
                    String d = m.optString("diff", "normal");
                    r.diff = ("easy".equals(d) || "hard".equals(d) || "nightmare".equals(d)) ? d : "normal";
                    r.host = c.id;
                    r.players.add(c);
                    r.seed = (System.currentTimeMillis() % 2147483646L) + 1;
                    rooms.put(r.id, r);
                    c.room = r;
                    JSONObject joined = new JSONObject();
                    joined.put("t", "joined");
                    joined.put("room", roomState(r));
                    c.send(joined);
                    broadcastRooms();
                    break;
                }
                case "join": {
                    if (c.room != null) return;
                    Room r = rooms.get(m.optString("room", ""));
                    if (r == null) {
                        JSONObject err = new JSONObject();
                        err.put("t", "error");
                        err.put("msg", "ไม่พบห้องนี้แล้ว");
                        c.send(err);
                        return;
                    }
                    if (r.started) {
                        JSONObject err = new JSONObject();
                        err.put("t", "error");
                        err.put("msg", "ห้องนี้เริ่มเล่นไปแล้ว");
                        c.send(err);
                        return;
                    }
                    if (r.players.size() >= r.max) {
                        JSONObject err = new JSONObject();
                        err.put("t", "error");
                        err.put("msg", "ห้องเต็มแล้ว");
                        c.send(err);
                        return;
                    }
                    if (r.pass != null && !r.pass.isEmpty() && !r.pass.equals(m.optString("pass", ""))) {
                        JSONObject err = new JSONObject();
                        err.put("t", "error");
                        err.put("msg", "รหัสผ่านไม่ถูกต้อง");
                        err.put("code", "pass");
                        c.send(err);
                        return;
                    }
                    r.players.add(c);
                    c.room = r;
                    JSONObject joined = new JSONObject();
                    joined.put("t", "joined");
                    joined.put("room", roomState(r));
                    c.send(joined);
                    JSONObject hist = new JSONObject();
                    hist.put("t", "chat_history");
                    hist.put("list", new JSONArray(r.chat));
                    c.send(hist);
                    JSONObject rs = new JSONObject();
                    rs.put("t", "room");
                    rs.put("room", roomState(r));
                    toRoom(r, rs, c);
                    sysChat(r, c.name + " เข้าร่วมเกม", "in");
                    broadcastRooms();
                    break;
                }
                case "chat": {
                    Room r = c.room;
                    if (r == null) return;
                    String text = cleanText(m.optString("text", ""), 160);
                    if (text.isEmpty()) return;
                    long now = System.currentTimeMillis();
                    if (now - c.lastChat < 350) return;
                    c.lastChat = now;
                    JSONObject cm = new JSONObject();
                    cm.put("id", c.id); cm.put("name", c.name); cm.put("text", text); cm.put("ts", now);
                    addChat(r, cm);
                    break;
                }
                case "leave": {
                    onLeave(c);
                    JSONObject left = new JSONObject();
                    left.put("t", "left");
                    c.send(left);
                    JSONArray list = new JSONArray();
                    for (Room rr : rooms.values()) list.put(pubRoom(rr));
                    JSONObject roomsMsg = new JSONObject();
                    roomsMsg.put("t", "rooms");
                    roomsMsg.put("rooms", list);
                    c.send(roomsMsg);
                    break;
                }
                case "start": {
                    Room r = c.room;
                    if (r == null || !c.id.equals(r.host) || r.started) return;
                    if (r.players.size() < 2) {
                        JSONObject err = new JSONObject();
                        err.put("t", "error");
                        err.put("msg", "ต้องมีผู้เล่นอย่างน้อย 2 คนถึงเริ่มได้");
                        c.send(err);
                        return;
                    }
                    r.started = true;
                    for (Client p : r.players) r.prog.put(p.id, 0);
                    JSONObject st = new JSONObject();
                    st.put("t", "started");
                    st.put("room", roomState(r));
                    toRoom(r, st, null);
                    broadcastRooms();
                    break;
                }
                case "progress": {
                    Room r = c.room;
                    if (r == null || !r.started || r.over) return;
                    int p = Math.max(0, Math.min(r.days * 3 + 3, m.optInt("p", 0)));
                    Integer old = r.prog.get(c.id);
                    if (old == null || p > old) {
                        r.prog.put(c.id, p);
                        recompute(r);
                    }
                    break;
                }
                case "claim": {
                    Room r = c.room;
                    if (r == null || !r.started) return;
                    String key = m.optString("key", "");
                    if (key.isEmpty() || key.length() > 60) return;
                    JSONObject res = new JSONObject();
                    res.put("t", "claim_result");
                    res.put("key", key);
                    if (r.claimed.contains(key)) {
                        res.put("ok", false);
                        c.send(res);
                    } else {
                        r.claimed.add(key);
                        res.put("ok", true);
                        c.send(res);
                        JSONObject claimed = new JSONObject();
                        claimed.put("t", "claimed");
                        claimed.put("key", key);
                        toRoom(r, claimed, c);
                    }
                    break;
                }
                case "final": {
                    Room r = c.room;
                    if (r == null || !r.started || r.over) return;
                    JSONObject f = new JSONObject();
                    f.put("name", c.name);
                    f.put("score", Math.max(0L, m.optLong("score", 0L)));
                    r.finals.put(c.id, f);
                    r.prog.put(c.id, 1000000000);
                    JSONObject fi = new JSONObject();
                    fi.put("t", "final_in");
                    fi.put("n", r.finals.size());
                    fi.put("total", r.players.size());
                    toRoom(r, fi, null);
                    boolean all = true;
                    for (Client pl : r.players) if (!r.finals.containsKey(pl.id)) { all = false; break; }
                    if (all) finishGame(r); else recompute(r);
                    break;
                }
                // trade_list / trade_buy แบบย่อ — รองรับโครงสร้างเดิม
                case "trade_list": {
                    Room r = c.room;
                    if (r == null || !r.started || r.over) return;
                    int price = m.optInt("price", 0);
                    if (price < 1 || price > 50_000_000) {
                        JSONObject fail = new JSONObject();
                        fail.put("t", "trade_list_fail");
                        fail.put("reason", "ราคาต้องอยู่ระหว่าง 1 – 50,000,000");
                        c.send(fail);
                        return;
                    }
                    JSONObject item = m.optJSONObject("item");
                    if (item == null) return;
                    JSONObject tr = new JSONObject();
                    tr.put("id", "t" + (++r.tradeSeq));
                    tr.put("seller", c.id);
                    tr.put("sellerName", c.name);
                    tr.put("item", item);
                    tr.put("price", price);
                    tr.put("day", curDay(r));
                    r.trades.add(tr);
                    JSONObject ok = new JSONObject();
                    ok.put("t", "trade_list_ok");
                    ok.put("id", tr.getString("id"));
                    ok.put("ref", m.optString("ref", ""));
                    c.send(ok);
                    JSONObject tradesMsg = new JSONObject();
                    tradesMsg.put("t", "trades");
                    tradesMsg.put("list", new JSONArray(r.trades));
                    toRoom(r, tradesMsg, null);
                    break;
                }
                case "trade_buy": {
                    Room r = c.room;
                    if (r == null) return;
                    String tid = m.optString("id", "");
                    JSONObject found = null;
                    int idx = -1;
                    for (int i = 0; i < r.trades.size(); i++) {
                        if (tid.equals(r.trades.get(i).optString("id"))) {
                            found = r.trades.get(i);
                            idx = i;
                            break;
                        }
                    }
                    if (found == null) {
                        JSONObject fail = new JSONObject();
                        fail.put("t", "trade_buy_fail");
                        fail.put("id", tid);
                        fail.put("reason", "ขอโทษนะ มีคนซื้อไปแล้ว");
                        c.send(fail);
                        return;
                    }
                    if (c.id.equals(found.optString("seller"))) {
                        JSONObject fail = new JSONObject();
                        fail.put("t", "trade_buy_fail");
                        fail.put("id", tid);
                        fail.put("reason", "ซื้อของตัวเองไม่ได้");
                        c.send(fail);
                        return;
                    }
                    r.trades.remove(idx);
                    JSONObject ok = new JSONObject();
                    ok.put("t", "trade_buy_ok");
                    ok.put("id", tid);
                    ok.put("item", found.getJSONObject("item"));
                    ok.put("price", found.getInt("price"));
                    c.send(ok);
                    for (Client s : r.players) {
                        if (s.id.equals(found.optString("seller"))) {
                            JSONObject sold = new JSONObject();
                            sold.put("t", "trade_sold");
                            sold.put("id", tid);
                            sold.put("price", found.getInt("price"));
                            sold.put("buyer", c.name);
                            s.send(sold);
                            break;
                        }
                    }
                    JSONObject tradesMsg = new JSONObject();
                    tradesMsg.put("t", "trades");
                    tradesMsg.put("list", new JSONArray(r.trades));
                    toRoom(r, tradesMsg, null);
                    break;
                }
            }
        } catch (Exception e) {
            Log.e(TAG, "onMsg", e);
        }
    }
}
