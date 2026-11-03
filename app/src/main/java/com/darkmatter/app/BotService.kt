package com.darkmatter.app

import android.annotation.SuppressLint
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.graphics.Bitmap
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.PowerManager
import android.util.Log
import android.webkit.CookieManager
import android.webkit.RenderProcessGoneDetail
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.core.app.NotificationCompat
import androidx.core.app.ServiceCompat
import androidx.core.content.ContextCompat
import androidx.webkit.WebViewCompat
import androidx.webkit.WebViewFeature
import com.chaquo.python.Python
import com.chaquo.python.android.AndroidPlatform
import org.json.JSONObject
import kotlin.concurrent.thread

/**
 * Keeps the app alive in the background (foreground service) and owns two hidden WebViews:
 *  - the Quotex page (the bot reads its live data through it)
 *  - the chart renderer (turns the engine's SVG charts into PNG pictures)
 */
class BotService : Service() {

    companion object {
        private const val TAG = "DarkMatter"
        private const val CHANNEL = "dm_status"
        private const val NOTIF_ID = 1
        const val ACTION_STOP = "com.darkmatter.app.STOP"

        @Volatile
        var port: Int = 0

        @Volatile
        var token: String = ""

        val ready: Boolean
            get() = port != 0 && token.isNotEmpty()

        fun pageUrl(): String = "http://127.0.0.1:$port/?t=$token"

        fun start(ctx: Context) {
            ContextCompat.startForegroundService(ctx, Intent(ctx, BotService::class.java))
        }
    }

    private val main = Handler(Looper.getMainLooper())
    private var renderer: WebView? = null
    private var quotex: WebView? = null
    private var wake: PowerManager.WakeLock? = null
    private var booted = false
    private var lastText = "Starting ..."

    private val module by lazy { Python.getInstance().getModule("dm_app") }

    private val ticker = object : Runnable {
        override fun run() {
            thread(name = "dm-status") {
                try {
                    lastText = module.callAttr("status_text").toString()
                } catch (e: Exception) {
                    Log.w(TAG, "status_text failed", e)
                }
                main.post { pushNotification(lastText) }
            }
            main.postDelayed(this, 5000)
        }
    }

    /** What Python calls (from its own threads) to control the Android side. */
    inner class NativeApi {
        fun openBridge(url: String) {
            main.post { openQuotex(url) }
        }

        fun closeBridge() {
            main.post { closeQuotex() }
        }

        fun reloadBridge() {
            main.post { quotex?.reload() }
        }

        fun sendToPage(text: String) {
            val js = "window.__dmSend&&window.__dmSend(" + JSONObject.quote(text) + ")"
            main.post { quotex?.evaluateJavascript(js, null) }
        }
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_STOP) {
            shutdownAll()
            return START_NOT_STICKY
        }
        createChannel()
        ServiceCompat.startForeground(
            this, NOTIF_ID, buildNotification(lastText),
            if (Build.VERSION.SDK_INT >= 34) android.content.pm.ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE else 0
        )
        if (!booted) {
            booted = true
            acquireWake()
            boot()
        }
        return START_STICKY
    }

    private fun boot() {
        thread(name = "dm-boot") {
            try {
                if (!Python.isStarted()) Python.start(AndroidPlatform(applicationContext))
                val res = module.callAttr("start", filesDir.absolutePath, NativeApi()).toString()
                val j = JSONObject(res)
                port = j.getInt("port")
                token = j.getString("token")
                main.post {
                    startRenderer()
                    main.post(ticker)
                }
            } catch (e: Throwable) {
                Log.e(TAG, "boot failed", e)
                lastText = "Could not start: " + (e.message ?: e.javaClass.simpleName)
                main.post { pushNotification(lastText) }
            }
        }
    }

    // ---------------------------------------------------------------- chart renderer
    @SuppressLint("SetJavaScriptEnabled")
    private fun startRenderer() {
        val wv = WebView(applicationContext)
        wv.settings.javaScriptEnabled = true
        wv.webViewClient = object : WebViewClient() {
            override fun onRenderProcessGone(view: WebView?, detail: RenderProcessGoneDetail?): Boolean {
                if (view === renderer) {
                    renderer = null
                    main.postDelayed({ startRenderer() }, 1500)
                }
                return true
            }
        }
        renderer = wv
        wv.loadUrl("http://127.0.0.1:$port/renderer?t=$token")
    }

    // ---------------------------------------------------------------- Quotex page
    @SuppressLint("SetJavaScriptEnabled")
    private fun openQuotex(url: String) {
        closeQuotex()
        val wv = WebView(applicationContext)
        val cm = CookieManager.getInstance()
        cm.setAcceptCookie(true)
        cm.setAcceptThirdPartyCookie(wv, true)
        wv.settings.javaScriptEnabled = true
        wv.settings.domStorageEnabled = true
        wv.settings.userAgentString = Ua.clean(wv.settings.userAgentString)
        wv.addJavascriptInterface(FrameBridge { json ->
            try {
                module.callAttr("on_frames", json)
            } catch (e: Exception) {
                Log.w(TAG, "on_frames failed", e)
            }
        }, "DMBridge")

        var docStart = false
        if (WebViewFeature.isFeatureSupported(WebViewFeature.DOCUMENT_START_SCRIPT)) {
            try {
                WebViewCompat.addDocumentStartJavaScript(wv, HOOK_JS, setOf("*"))
                docStart = true
            } catch (e: Exception) {
                Log.w(TAG, "document start script not available", e)
            }
        }
        val needLateHook = !docStart
        wv.webViewClient = object : WebViewClient() {
            override fun onPageStarted(view: WebView?, pageUrl: String?, favicon: Bitmap?) {
                if (needLateHook) view?.evaluateJavascript(HOOK_JS, null)
            }

            override fun onRenderProcessGone(view: WebView?, detail: RenderProcessGoneDetail?): Boolean {
                if (view === quotex) {
                    closeQuotex()
                    main.postDelayed({ openQuotex(url) }, 1500)
                }
                return true
            }
        }
        quotex = wv
        wv.loadUrl(url)
    }

    private fun closeQuotex() {
        val wv = quotex ?: return
        quotex = null
        try {
            wv.stopLoading()
            wv.loadUrl("about:blank")
            wv.removeJavascriptInterface("DMBridge")
            wv.destroy()
        } catch (e: Exception) {
            Log.w(TAG, "closeQuotex", e)
        }
    }

    // ---------------------------------------------------------------- notification / lifecycle
    private fun createChannel() {
        if (Build.VERSION.SDK_INT >= 26) {
            val ch = NotificationChannel(CHANNEL, "DARKMATTER status", NotificationManager.IMPORTANCE_LOW)
            getSystemService(NotificationManager::class.java).createNotificationChannel(ch)
        }
    }

    private fun buildNotification(text: String): Notification {
        val open = PendingIntent.getActivity(
            this, 0, Intent(this, MainActivity::class.java),
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
        )
        val stop = PendingIntent.getService(
            this, 1, Intent(this, BotService::class.java).setAction(ACTION_STOP),
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
        )
        return NotificationCompat.Builder(this, CHANNEL)
            .setSmallIcon(R.drawable.ic_notify)
            .setContentTitle("DARKMATTER")
            .setContentText(text)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setContentIntent(open)
            .addAction(0, "Exit", stop)
            .build()
    }

    private fun pushNotification(text: String) {
        try {
            getSystemService(NotificationManager::class.java).notify(NOTIF_ID, buildNotification(text))
        } catch (e: Exception) {
            Log.w(TAG, "notify", e)
        }
    }

    @SuppressLint("WakelockTimeout")
    private fun acquireWake() {
        try {
            val pm = getSystemService(Context.POWER_SERVICE) as PowerManager
            wake = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "darkmatter:engine").apply {
                setReferenceCounted(false)
                acquire()
            }
        } catch (e: Exception) {
            Log.w(TAG, "wake lock", e)
        }
    }

    private fun shutdownAll() {
        thread(name = "dm-exit") {
            try {
                module.callAttr("shutdown")
            } catch (e: Exception) {
                Log.w(TAG, "shutdown", e)
            }
            main.post {
                ServiceCompat.stopForeground(this, ServiceCompat.STOP_FOREGROUND_REMOVE)
                stopSelf()
                android.os.Process.killProcess(android.os.Process.myPid())
            }
        }
    }

    override fun onDestroy() {
        main.removeCallbacksAndMessages(null)
        closeQuotex()
        try {
            renderer?.destroy()
        } catch (e: Exception) {
            Log.w(TAG, "renderer destroy", e)
        }
        renderer = null
        try {
            wake?.takeIf { it.isHeld }?.release()
        } catch (e: Exception) {
            Log.w(TAG, "wake release", e)
        }
        super.onDestroy()
    }
}
