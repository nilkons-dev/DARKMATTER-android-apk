package com.darkmatter.app

import android.webkit.JavascriptInterface

/** Removes the WebView markers from the user agent so the Quotex page sees a normal mobile Chrome. */
object Ua {
    fun clean(ua: String): String = ua.replace("; wv", "").replace("Version/4.0 ", "")
}

/** Receives batches of socket frames from the Quotex page and hands them to Python. */
class FrameBridge(private val sink: (String) -> Unit) {
    @JavascriptInterface
    fun frames(json: String) {
        sink(json)
    }
}

/**
 * Injected into the Quotex page before any of its scripts run. It copies every message of the page's
 * trading socket (socket.io) to the app, in order, and lets the app send messages through that same socket.
 * This is the phone version of the HOOK_JS / CDP bridge of the original bot.
 */
const val HOOK_JS = """
(function () {
  if (window.__dmHooked) return;
  window.__dmHooked = true;
  var Orig = window.WebSocket;
  var q = [];
  var timer = null;
  var chain = Promise.resolve();
  var socks = [];
  function flush() {
    timer = null;
    if (!q.length) return;
    var out = q;
    q = [];
    try { DMBridge.frames(JSON.stringify(out)); } catch (e) {}
  }
  function push(k, d) {
    q.push([k, d]);
    if (q.length >= 150) {
      if (timer) { clearTimeout(timer); }
      flush();
    } else if (!timer) {
      timer = setTimeout(flush, 25);
    }
  }
  function b64(buf) {
    var b = new Uint8Array(buf), s = '', i = 0, n = b.length;
    for (; i < n; i += 32768) { s += String.fromCharCode.apply(null, b.subarray(i, i + 32768)); }
    return btoa(s);
  }
  function Hooked(url, protocols) {
    var ws = protocols === undefined ? new Orig(url) : new Orig(url, protocols);
    try {
      if (String(url).indexOf('socket.io') >= 0) {
        socks.push(ws);
        ws.addEventListener('open', function () { push('o', ''); });
        ws.addEventListener('close', function () { push('c', ''); });
        ws.addEventListener('message', function (ev) {
          var d = ev.data;
          if (typeof d === 'string') {
            chain = chain.then(function () { push('t', d); });
          } else if (d instanceof ArrayBuffer) {
            chain = chain.then(function () { push('b', b64(d)); });
          } else if (d && d.arrayBuffer) {
            chain = chain.then(function () { return d.arrayBuffer().then(function (buf) { push('b', b64(buf)); }); });
          }
        });
      }
    } catch (e) {}
    return ws;
  }
  Hooked.prototype = Orig.prototype;
  Hooked.CONNECTING = Orig.CONNECTING;
  Hooked.OPEN = Orig.OPEN;
  Hooked.CLOSING = Orig.CLOSING;
  Hooked.CLOSED = Orig.CLOSED;
  window.WebSocket = Hooked;
  window.__dmSend = function (msg) {
    var s = null;
    for (var i = socks.length - 1; i >= 0; i--) {
      if (socks[i].readyState === 1) { s = socks[i]; break; }
    }
    if (!s) return false;
    s.send(msg);
    return true;
  };
  setInterval(function () {
    var tok = false;
    try { tok = !!(window.settings && window.settings.token); } catch (e) {}
    push('p', JSON.stringify({ href: String(location.href), token: tok }));
  }, 3000);
})();
"""
