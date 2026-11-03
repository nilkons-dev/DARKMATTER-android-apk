package com.darkmatter.app

import android.Manifest
import android.annotation.SuppressLint
import android.content.Intent
import android.content.pm.PackageManager
import android.content.res.Configuration
import android.graphics.Color
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.os.PowerManager
import android.provider.Settings
import android.webkit.JavascriptInterface
import android.webkit.WebChromeClient
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.appcompat.app.AppCompatActivity
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat
import androidx.core.view.WindowCompat
import com.chaquo.python.Python
import kotlin.concurrent.thread

/** The app screen: the UI (dm_ui) served by the Python backend on 127.0.0.1, shown in a WebView. */
class MainActivity : AppCompatActivity() {

    private lateinit var web: WebView
    private val main = Handler(Looper.getMainLooper())
    private var waited = 0

    @SuppressLint("SetJavaScriptEnabled")
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        web = WebView(this)
        setContentView(web)
        web.settings.javaScriptEnabled = true
        web.settings.domStorageEnabled = true
        web.webChromeClient = WebChromeClient()
        web.webViewClient = WebViewClient()
        web.addJavascriptInterface(AppApi(), "Android")
        askNotificationPermission()
        BotService.start(this)
        waitAndLoad()
    }

    private fun waitAndLoad() {
        if (BotService.ready) {
            web.loadUrl(BotService.pageUrl())
            return
        }
        waited += 250
        if (waited > 30000) {
            web.loadData("<html><body style='font-family:sans-serif;padding:24px'>The engine did not start. " +
                    "Close the app and open it again.</body></html>", "text/html", "utf-8")
            return
        }
        main.postDelayed({ waitAndLoad() }, 250)
    }

    private fun askNotificationPermission() {
        if (Build.VERSION.SDK_INT >= 33 &&
            ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) {
            ActivityCompat.requestPermissions(this, arrayOf(Manifest.permission.POST_NOTIFICATIONS), 1)
        }
    }

    override fun onDestroy() {
        main.removeCallbacksAndMessages(null)
        web.removeJavascriptInterface("Android")
        web.destroy()
        super.onDestroy()
    }

    /** Functions the UI can call as window.Android.xxx() */
    inner class AppApi {
        @JavascriptInterface
        fun openLogin() {
            thread(name = "dm-prelogin") {
                try {
                    Python.getInstance().getModule("dm_app").callAttr("prepare_login")
                } catch (e: Exception) {
                    // the login screen still opens
                }
            }
            runOnUiThread { startActivity(Intent(this@MainActivity, LoginActivity::class.java)) }
        }

        @JavascriptInterface
        fun isNight(): Boolean =
            (resources.configuration.uiMode and Configuration.UI_MODE_NIGHT_MASK) == Configuration.UI_MODE_NIGHT_YES

        @JavascriptInterface
        fun setBars(bg: String, dark: Boolean) {
            runOnUiThread {
                try {
                    val c = Color.parseColor(bg.trim())
                    window.statusBarColor = c
                    window.navigationBarColor = c
                    val ctl = WindowCompat.getInsetsController(window, window.decorView)
                    ctl.isAppearanceLightStatusBars = !dark
                    ctl.isAppearanceLightNavigationBars = !dark
                } catch (e: Exception) {
                    // keep the default bars
                }
            }
        }

        @JavascriptInterface
        fun allowBackground() {
            runOnUiThread {
                try {
                    val pm = getSystemService(POWER_SERVICE) as PowerManager
                    if (!pm.isIgnoringBatteryOptimizations(packageName)) {
                        startActivity(
                            Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS)
                                .setData(Uri.parse("package:$packageName"))
                        )
                    }
                } catch (e: Exception) {
                    startActivity(Intent(Settings.ACTION_IGNORE_BATTERY_OPTIMIZATION_SETTINGS))
                }
            }
        }

        @JavascriptInterface
        fun exitApp() {
            runOnUiThread {
                startService(Intent(this@MainActivity, BotService::class.java).setAction(BotService.ACTION_STOP))
                finishAndRemoveTask()
            }
        }
    }
}
