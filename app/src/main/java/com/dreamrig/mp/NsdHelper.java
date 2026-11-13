package com.dreamrig.mp;

import android.content.Context;
import android.net.nsd.NsdManager;
import android.net.nsd.NsdServiceInfo;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Android NSD (mDNS) สำหรับประกาศและค้นหาบริการ _dreamrig._tcp.
 * ข้อมูลห้องถูกใส่ใน TXT attributes (name, players, max, days, diff, locked)
 */
public class NsdHelper {
    private static final String TAG = "DreamRigNsd";
    public static final String SERVICE_TYPE = "_dreamrig._tcp.";
    public static final String SERVICE_NAME_PREFIX = "DreamRig-";

    private final Context context;
    private final DreamRigBridge bridge;
    private NsdManager nsdManager;
    private NsdManager.RegistrationListener regListener;
    private NsdManager.DiscoveryListener discListener;
    private final Map<String, JSONObject> discovered = new ConcurrentHashMap<>();
    private String registeredName;
    private final java.util.LinkedList<NsdServiceInfo> resolveQueue = new java.util.LinkedList<>();
    private boolean resolving = false;
    private final android.os.Handler h = new android.os.Handler(android.os.Looper.getMainLooper());

    public NsdHelper(Context context, DreamRigBridge bridge) {
        this.context = context.getApplicationContext();
        this.bridge = bridge;
        this.nsdManager = (NsdManager) this.context.getSystemService(Context.NSD_SERVICE);
    }

    public void registerService(int port, String metaJson) {
        unregisterService();
        try {
            JSONObject meta = new JSONObject(metaJson);
            NsdServiceInfo info = new NsdServiceInfo();
            String name = SERVICE_NAME_PREFIX + System.currentTimeMillis();
            info.setServiceName(name);
            info.setServiceType(SERVICE_TYPE);
            info.setPort(port);
            // TXT records
            info.setAttribute("game", "DREAMRIG");
            info.setAttribute("v", "1");
            if (meta.has("name")) info.setAttribute("room", meta.optString("name", "ห้อง"));
            if (meta.has("players")) info.setAttribute("players", String.valueOf(meta.optInt("players", 1)));
            if (meta.has("max")) info.setAttribute("max", String.valueOf(meta.optInt("max", 4)));
            if (meta.has("days")) info.setAttribute("days", String.valueOf(meta.optInt("days", 30)));
            if (meta.has("diff")) info.setAttribute("diff", meta.optString("diff", "normal"));
            if (meta.has("locked")) info.setAttribute("locked", meta.optBoolean("locked", false) ? "1" : "0");
            if (meta.has("id")) info.setAttribute("id", meta.optString("id", ""));

            regListener = new NsdManager.RegistrationListener() {
                @Override public void onRegistrationFailed(NsdServiceInfo s, int errorCode) {
                    Log.e(TAG, "register failed " + errorCode);
                }
                @Override public void onUnregistrationFailed(NsdServiceInfo s, int errorCode) {}
                @Override public void onServiceRegistered(NsdServiceInfo s) {
                    registeredName = s.getServiceName();
                    Log.i(TAG, "registered: " + registeredName);
                }
                @Override public void onServiceUnregistered(NsdServiceInfo s) {
                    registeredName = null;
                }
            };
            nsdManager.registerService(info, NsdManager.PROTOCOL_DNS_SD, regListener);
        } catch (Exception e) {
            Log.e(TAG, "registerService", e);
        }
    }

    public void unregisterService() {
        if (regListener != null && nsdManager != null) {
            try { nsdManager.unregisterService(regListener); } catch (Exception ignored) {}
            regListener = null;
            registeredName = null;
        }
    }

    public void startDiscovery() {
        stopDiscovery();
        discovered.clear();
        discListener = new NsdManager.DiscoveryListener() {
            @Override public void onStartDiscoveryFailed(String serviceType, int errorCode) {
                Log.e(TAG, "startDiscovery failed " + errorCode);
            }
            @Override public void onStopDiscoveryFailed(String serviceType, int errorCode) {}
            @Override public void onDiscoveryStarted(String serviceType) {
                Log.i(TAG, "discovery started");
            }
            @Override public void onDiscoveryStopped(String serviceType) {
                Log.i(TAG, "discovery stopped");
            }
            @Override public void onServiceFound(NsdServiceInfo serviceInfo) {
                if (!SERVICE_TYPE.equals(serviceInfo.getServiceType()) &&
                    !serviceInfo.getServiceType().contains("dreamrig")) return;
                synchronized (resolveQueue) { resolveQueue.add(serviceInfo); }
                resolveNext();
            }
            @Override public void onServiceLost(NsdServiceInfo serviceInfo) {
                discovered.remove(serviceInfo.getServiceName());
                pushUpdate();
            }
        };
        nsdManager.discoverServices(SERVICE_TYPE, NsdManager.PROTOCOL_DNS_SD, discListener);
    }

    private void resolveNext() {
        final NsdServiceInfo next;
        synchronized (resolveQueue) {
            if (resolving || resolveQueue.isEmpty()) return;
            next = resolveQueue.poll();
            resolving = true;
        }
        try {
            nsdManager.resolveService(next, new NsdManager.ResolveListener() {
                                @Override public void onResolveFailed(NsdServiceInfo si, int errorCode) {
                                    Log.w(TAG, "resolve failed " + errorCode);
                                    resolveDone();
                                }
                                @Override public void onServiceResolved(NsdServiceInfo si) {
                                    try {
                                        String host = si.getHost() != null ? si.getHost().getHostAddress() : null;
                                        if (host == null) return; // finally จะเรียก resolveDone
                                        // ข้ามตัวเอง
                                        String myIp = NetworkUtil.getLocalIpAddress(context);
                                        if (host.equals(myIp)) return; // finally จะเรียก resolveDone
            
                                        JSONObject room = new JSONObject();
                                        room.put("host", host);
                                        room.put("port", si.getPort());
                                        room.put("wsUrl", "ws://" + host + ":" + si.getPort());
                                        room.put("serviceName", si.getServiceName());
                                        Map<String, byte[]> attrs = si.getAttributes();
                                        if (attrs != null) {
                                            room.put("name", attr(attrs, "room", "ห้อง"));
                                            room.put("players", Integer.parseInt(attr(attrs, "players", "1")));
                                            room.put("max", Integer.parseInt(attr(attrs, "max", "4")));
                                            room.put("days", Integer.parseInt(attr(attrs, "days", "30")));
                                            room.put("diff", attr(attrs, "diff", "normal"));
                                            room.put("locked", "1".equals(attr(attrs, "locked", "0")));
                                            room.put("id", attr(attrs, "id", si.getServiceName()));
                                        } else {
                                            room.put("name", si.getServiceName());
                                            room.put("players", 1);
                                            room.put("max", 4);
                                            room.put("days", 30);
                                            room.put("diff", "normal");
                                            room.put("locked", false);
                                            room.put("id", si.getServiceName());
                                        }
                                        room.put("started", false);
                                        discovered.put(si.getServiceName(), room);
                                        pushUpdate();
                                    } catch (Exception e) {
                                        Log.e(TAG, "onServiceResolved", e);
                                    } finally {
                                        resolveDone();
                                    }
                                }
                            });
        } catch (Exception e) {
            Log.w(TAG, "resolve start", e);
            resolveDone();
        }
    }

    private void resolveDone() {
        synchronized (resolveQueue) { resolving = false; }
        h.postDelayed(this::resolveNext, 50);
    }

    public void stopDiscovery() {
        if (discListener != null && nsdManager != null) {
            try { nsdManager.stopServiceDiscovery(discListener); } catch (Exception ignored) {}
            discListener = null;
        }
        synchronized (resolveQueue) { resolveQueue.clear(); resolving = false; }
        discovered.clear();
    }

    private void pushUpdate() {
        JSONArray arr = new JSONArray();
        for (JSONObject o : discovered.values()) arr.put(o);
        bridge.onServicesUpdated(arr);
    }

    private static String attr(Map<String, byte[]> attrs, String key, String def) {
        byte[] b = attrs.get(key);
        if (b == null) return def;
        try { return new String(b, "UTF-8"); } catch (Exception e) { return def; }
    }
}
