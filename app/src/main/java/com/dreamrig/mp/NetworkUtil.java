package com.dreamrig.mp;

import android.content.Context;
import android.net.wifi.WifiManager;
import android.util.Log;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.List;

public class NetworkUtil {
    private static final String TAG = "DreamRigNet";

    public static String getLocalIpAddress(Context context) {
        // ลองจาก WifiManager ก่อน (เร็ว)
        try {
            WifiManager wifi = (WifiManager) context.getApplicationContext().getSystemService(Context.WIFI_SERVICE);
            if (wifi != null) {
                int ip = wifi.getConnectionInfo().getIpAddress();
                if (ip != 0) {
                    return String.format("%d.%d.%d.%d",
                            (ip & 0xff), (ip >> 8 & 0xff), (ip >> 16 & 0xff), (ip >> 24 & 0xff));
                }
            }
        } catch (Exception e) {
            Log.w(TAG, "wifi ip", e);
        }
        // fallback: วน network interfaces (Hotspot ของเครื่องเราเองจะอยู่ที่ ap0 / wlan1 / swlan0)
        try {
            List<NetworkInterface> ifaces = Collections.list(NetworkInterface.getNetworkInterfaces());
            String best = "";
            for (NetworkInterface ni : ifaces) {
                if (!ni.isUp() || ni.isLoopback()) continue;
                String n = ni.getName() == null ? "" : ni.getName().toLowerCase();
                // ข้ามเน็ตมือถือ / VPN
                if (n.startsWith("rmnet") || n.startsWith("ccmni") || n.startsWith("tun") || n.startsWith("dummy") || n.startsWith("v4-") || n.startsWith("clat")) continue;
                boolean wifiLike = n.startsWith("wlan") || n.startsWith("ap") || n.startsWith("swlan") || n.startsWith("eth") || n.startsWith("p2p");
                for (InetAddress addr : Collections.list(ni.getInetAddresses())) {
                    if (addr.isLoopbackAddress()) continue;
                    String ip = addr.getHostAddress();
                    if (ip == null || ip.indexOf(':') >= 0) continue;
                    if (ip.startsWith("192.168.") || ip.startsWith("10.") || ip.startsWith("172.")) {
                        if (wifiLike) return ip;
                        if (best.isEmpty()) best = ip;
                    }
                }
            }
            if (!best.isEmpty()) return best;
        } catch (Exception e) {
            Log.w(TAG, "iface ip", e);
        }
        return "";
    }
}
