package com.dreamrig.tomato;

import android.annotation.SuppressLint;
import android.app.Activity;
import android.graphics.Color;
import android.os.Build;
import android.os.Bundle;
import android.view.View;
import android.view.Window;
import android.view.WindowManager;
import android.webkit.ConsoleMessage;
import android.webkit.WebChromeClient;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;

import com.dreamrig.mp.DreamRigBridge;

public class MainActivity extends Activity {
    private WebView webView;
    private DreamRigBridge bridge;

    @SuppressLint("SetJavaScriptEnabled")
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        // กันเนื้อหาถูกบังด้วย status/nav bar
        Window w = getWindow();
        w.setStatusBarColor(Color.parseColor("#0c0e14"));
        w.setNavigationBarColor(Color.parseColor("#0c0e14"));
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            w.setDecorFitsSystemWindows(true);
        } else {
            w.clearFlags(WindowManager.LayoutParams.FLAG_FULLSCREEN);
            final View decor = w.getDecorView();
            decor.setSystemUiVisibility(
                View.SYSTEM_UI_FLAG_LAYOUT_STABLE
                | View.SYSTEM_UI_FLAG_LIGHT_STATUS_BAR
            );
        }

        webView = new WebView(this);
        setContentView(webView);

        WebSettings s = webView.getSettings();
        s.setJavaScriptEnabled(true);
        s.setDomStorageEnabled(true);
        s.setAllowFileAccess(true);
        s.setAllowContentAccess(true);
        s.setMediaPlaybackRequiresUserGesture(false);
        s.setMixedContentMode(WebSettings.MIXED_CONTENT_ALWAYS_ALLOW);

        webView.setWebViewClient(new WebViewClient());
        webView.setWebChromeClient(new WebChromeClient() {
            @Override public boolean onConsoleMessage(ConsoleMessage m) {
                android.util.Log.d("DreamRigJS", m.message() + " (" + m.lineNumber() + ")");
                return true;
            }
        });
        bridge = new DreamRigBridge(this, webView);
        webView.addJavascriptInterface(bridge, "DreamRigNative");
        webView.loadUrl("file:///android_asset/index.html");
    }

    @Override
    protected void onPause() {
        super.onPause();
        if (webView != null) {
            webView.onPause();
            // หยุดเสียงทันทีเมื่อออกจากแอป / พับจอ
            webView.evaluateJavascript(
                "(function(){try{if(window.AudioSys){AudioSys.stopBgm();var c=AudioSys.getCtx&&AudioSys.getCtx();if(c&&c.state==='running')c.suspend();}}catch(e){}})()",
                null);
        }
    }

    @Override
    protected void onResume() {
        super.onResume();
        if (webView != null) {
            webView.onResume();
            webView.evaluateJavascript(
                "(function(){try{if(window.AudioSys&&!AudioSys.isMuted()){AudioSys.resume();AudioSys.playBgm();}}catch(e){}})()",
                null);
        }
    }

    @Override
    protected void onDestroy() {
        if (bridge != null) bridge.destroy();
        if (webView != null) webView.destroy();
        super.onDestroy();
    }

    /** ปุ่ม Back: ให้เกมจัดการก่อน (ปิดแชท/ตั้งค่า/เปิดเมนูหยุด) ถ้าไม่ใช่ค่อยออกแอป */
    @Override
    @SuppressWarnings("deprecation")
    public void onBackPressed() {
        if (webView == null) { super.onBackPressed(); return; }
        webView.evaluateJavascript("(function(){try{return window.__dreamrigBack&&window.__dreamrigBack()?'1':'0';}catch(e){return '0';}})()",
            value -> { if (value == null || !value.contains("1")) MainActivity.super.onBackPressed(); });
    }
}
