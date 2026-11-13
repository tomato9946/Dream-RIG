package com.dreamrig.mp;

import android.app.Activity;
import android.content.Context;
import android.net.wifi.WifiManager;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;
import android.webkit.JavascriptInterface;
import android.webkit.WebView;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * JavascriptInterface สำหรับ DREAM RIG Multiplayer LAN Auto Discovery
 *
 * วิธีผูกกับ WebView (ใน Activity ที่โหลดเกม):
 *
 *   WebView webView = findViewById(R.id.webview);
 *   webView.getSettings().setJavaScriptEnabled(true);
 *   webView.getSettings().setDomStorageEnabled(true);
 *   // สำคัญ: อนุญาต mixed content / cleartext ถ้าโหลด file://
 *   DreamRigBridge bridge = new DreamRigBridge(this, webView);
 *   webView.addJavascriptInterface(bridge, "DreamRigNative");
 *   webView.loadUrl("file:///android_asset/DREAM_RIG_v4-0.html");
 *
 * JS ฝั่งเกมจะเรียก window.DreamRigNative.xxx()
 */
public class DreamRigBridge {
    private static final String TAG = "DreamRigBridge";
    private final Activity activity;
    private final WebView webView;
    private final Handler mainHandler = new Handler(Looper.getMainLooper());
    private NsdHelper nsdHelper;
    private volatile LocalWsServer localServer;
    private WifiManager.MulticastLock multicastLock;
    private volatile LocalWsServer.Client hostClient;
    private final java.util.concurrent.ExecutorService hostExec =
            java.util.concurrent.Executors.newSingleThreadExecutor();

    public DreamRigBridge(Activity activity, WebView webView) {
        this.activity = activity;
        this.webView = webView;
        this.nsdHelper = new NsdHelper(activity, this);
    }

    /** ตรวจว่ามี native bridge หรือไม่ (JS เรียก) */
    @JavascriptInterface
    public boolean isNativeAvailable() {
        return true;
    }

    /** คืน IP ของเครื่องนี้บน Wi-Fi / Hotspot (อาจว่างถ้ายังไม่เชื่อม) */
    @JavascriptInterface
    public String getLocalIp() {
        return NetworkUtil.getLocalIpAddress(activity);
    }

    /**
     * เริ่มค้นหาบริการ DREAM RIG ใน LAN
     * ผลลัพธ์จะถูกส่งกลับ JS ผ่าน onDiscoveredRooms(json)
     */
    @JavascriptInterface
    public void startDiscovery() {
        mainHandler.post(() -> {
            acquireMulticastLock();
            nsdHelper.startDiscovery();
            evalJs("window.__dreamrigOnDiscoveryStatus && window.__dreamrigOnDiscoveryStatus('searching')");
        });
    }

    @JavascriptInterface
    public void stopDiscovery() {
        mainHandler.post(() -> {
            nsdHelper.stopDiscovery();
            releaseMulticastLock();
            evalJs("window.__dreamrigOnDiscoveryStatus && window.__dreamrigOnDiscoveryStatus('stopped')");
        });
    }

    /**
     * เริ่ม Host: เปิด Local WebSocket Server + ประกาศ NSD
     * @param port พอร์ต (ปกติ 8787)
     * @param roomMetaJson ข้อมูลห้อง (name, max, days, diff, pass) สำหรับแสดงใน discovery
     */
    @JavascriptInterface
    public void startHost(int port, String roomMetaJson) {
        mainHandler.post(() -> {
            try {
                hostClient = null;
                if (localServer != null) {
                    localServer.stop();
                    localServer = null;
                }
                logJs("startHost begin port="+port);
                localServer = new LocalWsServer(port);
                localServer.start();
                int actualPort = localServer.getPort();
                logJs("LocalWsServer started port=" + actualPort);
                acquireMulticastLock();
                String ip = NetworkUtil.getLocalIpAddress(activity);
                nsdHelper.registerService(actualPort, roomMetaJson != null ? roomMetaJson : "{}");
                logJs("NSD register requested");
                JSONObject info = new JSONObject();
                info.put("ok", true);
                info.put("ip", ip);
                info.put("port", actualPort);
                info.put("wsUrl", "ws://" + ip + ":" + actualPort);
                logJs("host ok ip="+ip+" port="+actualPort);
                evalJs("window.__dreamrigOnHostStarted && window.__dreamrigOnHostStarted(" + info.toString() + ")");
            } catch (Exception e) {
                Log.e(TAG, "startHost failed", e);
                logJs("startHost FAIL: " + e.getClass().getSimpleName() + " " + e.getMessage());
                evalJs("window.__dreamrigOnHostStarted && window.__dreamrigOnHostStarted({ok:false,error:" + JSONObject.quote(String.valueOf(e.getMessage())) + "})");
            }
        });
    }

    /**
     * เปิด "ช่องคุยในเครื่อง" ระหว่างเกมของ Host กับเซิร์ฟเวอร์ในเครื่อง (ไม่ผ่านเครือข่าย)
     * JS จะได้ข้อความกลับทาง window.__dreamrigHostRecv(str) / __dreamrigHostClosed()
     */
    @JavascriptInterface
    public boolean hostOpen() {
        try {
            if (localServer == null) { logJs("hostOpen: server not running"); return false; }
            if (hostClient != null) { localServer.closeClient(hostClient); hostClient = null; }
            hostClient = localServer.openLocalClient(new LocalWsServer.LocalListener() {
                @Override public void onMessage(String json) {
                    evalJs("window.__dreamrigHostRecv && window.__dreamrigHostRecv(" + JSONObject.quote(json) + ")");
                }
                @Override public void onClosed() {
                    evalJs("window.__dreamrigHostClosed && window.__dreamrigHostClosed()");
                }
            });
            logJs("hostOpen ok (in-process)");
            return true;
        } catch (Exception e) {
            Log.e(TAG, "hostOpen", e);
            logJs("hostOpen FAIL: " + e);
            return false;
        }
    }

    @JavascriptInterface
    public void hostSend(final String json) {
        final LocalWsServer srv = localServer;
        final LocalWsServer.Client c = hostClient;
        if (srv == null || c == null) return;
        // ลำดับข้อความต้องไม่สลับ → ใช้ thread เดียว
        hostExec.execute(() -> srv.localReceive(c, json));
    }

    @JavascriptInterface
    public void hostClose() {
        final LocalWsServer srv = localServer;
        final LocalWsServer.Client c = hostClient;
        hostClient = null;
        if (srv != null && c != null) hostExec.execute(() -> srv.closeClient(c));
    }

    /** หยุด Host server + ยกเลิกการประกาศ */
    @JavascriptInterface
    public void stopHost() {
        mainHandler.post(() -> {
            nsdHelper.unregisterService();
            hostClient = null;
            if (localServer != null) {
                localServer.stop();
                localServer = null;
            }
            releaseMulticastLock();
            evalJs("window.__dreamrigOnHostStopped && window.__dreamrigOnHostStopped()");
        });
    }

    /** เรียกจาก NsdHelper เมื่อพบ / อัปเดตรายการบริการ */
    void onServicesUpdated(JSONArray services) {
        evalJs("window.__dreamrigOnDiscoveredRooms && window.__dreamrigOnDiscoveredRooms(" + services.toString() + ")");
    }

    private void logJs(String msg) {
        Log.i(TAG, msg);
        try {
            final String safe = JSONObject.quote(msg);
            evalJs("window.__dreamrigNativeLog && window.__dreamrigNativeLog(" + safe + ")");
        } catch (Exception e) {
            Log.w(TAG, "logJs", e);
        }
    }

    private void evalJs(String script) {
        mainHandler.post(() -> {
            if (webView != null) {
                webView.evaluateJavascript(script, null);
            }
        });
    }

    private void acquireMulticastLock() {
        try {
            if (multicastLock == null) {
                WifiManager wifi = (WifiManager) activity.getApplicationContext().getSystemService(Context.WIFI_SERVICE);
                if (wifi != null) {
                    multicastLock = wifi.createMulticastLock("DreamRigNsdLock");
                    multicastLock.setReferenceCounted(true);
                }
            }
            if (multicastLock != null && !multicastLock.isHeld()) {
                multicastLock.acquire();
            }
        } catch (Exception e) {
            Log.w(TAG, "multicast lock", e);
        }
    }

    private void releaseMulticastLock() {
        try {
            if (multicastLock != null && multicastLock.isHeld()) {
                multicastLock.release();
            }
        } catch (Exception ignored) {}
    }

    public void destroy() {
        stopDiscovery();
        stopHost();
    }
}
