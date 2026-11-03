package com.darkmatter.app

import android.annotation.SuppressLint
import android.graphics.Color
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.view.Gravity
import android.view.ViewGroup
import android.webkit.CookieManager
import android.webkit.WebChromeClient
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.Button
import android.widget.LinearLayout
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity

/** Shows the Quotex sign-in page. The login (cookies) is kept by the WebView and used by the hidden bridge page. */
class LoginActivity : AppCompatActivity() {

    companion object {
        private const val LOGIN_URL = "https://market-qx.info/en/sign-in"
    }

    private lateinit var web: WebView
    private val main = Handler(Looper.getMainLooper())
    private var closing = false

    private val probe = object : Runnable {
        override fun run() {
            if (closing) return
            val js = "(function(){try{return (location.pathname.indexOf('/trade')>=0 && window.settings && " +
                    "window.settings.token)?'1':'0'}catch(e){return '0'}})()"
            web.evaluateJavascript(js) { r ->
                if (r == "\"1\"") finishLogin() else main.postDelayed(this, 1500)
            }
        }
    }

    @SuppressLint("SetJavaScriptEnabled")
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val root = LinearLayout(this)
        root.orientation = LinearLayout.VERTICAL

        web = WebView(this)
        root.addView(web, LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f))

        val bar = LinearLayout(this)
        bar.orientation = LinearLayout.HORIZONTAL
        bar.gravity = Gravity.CENTER_VERTICAL
        bar.setPadding(24, 16, 24, 16)
        bar.setBackgroundColor(Color.parseColor("#12182A"))
        val hint = TextView(this)
        hint.text = "Log in, then tap Done"
        hint.setTextColor(Color.parseColor("#C9D3EA"))
        bar.addView(hint, LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f))
        val done = Button(this)
        done.text = "Done"
        done.setOnClickListener { finishLogin() }
        bar.addView(done)
        root.addView(bar, LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT))
        setContentView(root)

        val cm = CookieManager.getInstance()
        cm.setAcceptCookie(true)
        cm.setAcceptThirdPartyCookies(web, true)
        web.settings.javaScriptEnabled = true
        web.settings.domStorageEnabled = true
        web.settings.userAgentString = Ua.clean(web.settings.userAgentString)
        web.webChromeClient = WebChromeClient()
        web.webViewClient = WebViewClient()
        web.loadUrl(LOGIN_URL)
        main.postDelayed(probe, 3000)
    }

    private fun finishLogin() {
        if (closing) return
        closing = true
        CookieManager.getInstance().flush()
        Toast.makeText(this, "Saved. Now press Connect in the app.", Toast.LENGTH_LONG).show()
        finish()
    }

    override fun onDestroy() {
        closing = true
        main.removeCallbacksAndMessages(null)
        try {
            web.stopLoading()
            web.loadUrl("about:blank")
            web.destroy()
        } catch (e: Exception) {
            // already gone
        }
        super.onDestroy()
    }
}
