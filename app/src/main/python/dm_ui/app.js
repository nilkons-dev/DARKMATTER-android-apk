(function () {
  "use strict";
  var TOKEN = new URLSearchParams(location.search).get("t") || sessionStorage.getItem("dmt") || "";
  if (TOKEN) { try { sessionStorage.setItem("dmt", TOKEN); } catch (e) {} }
  var $ = function (id) { return document.getElementById(id); };
  var S = { st: null, markets: null, sel: new Set(), tab: "home", logSeq: 0, kind: "all", search: "",
            loadingMarkets: false, selInit: false, events: [], saveTimer: null, offline: 0 };

  // ---------- helpers ----------
  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined && text !== null) e.textContent = text;
    return e;
  }
  async function api(path, body, timeoutMs) {
    var ctl = new AbortController();
    var t = setTimeout(function () { ctl.abort(); }, timeoutMs || 25000);
    try {
      var opt = { headers: { "X-Token": TOKEN }, signal: ctl.signal };
      if (body !== undefined) {
        opt.method = "POST";
        opt.headers["Content-Type"] = "application/json";
        opt.body = JSON.stringify(body);
      }
      var r = await fetch(path, opt);
      var data = null;
      try { data = await r.json(); } catch (e) { data = {}; }
      return { ok: r.ok, status: r.status, data: data || {} };
    } finally { clearTimeout(t); }
  }
  var toastTimer = null;
  function toast(msg) {
    var t = $("toast"); t.textContent = msg; t.classList.remove("hidden");
    clearTimeout(toastTimer); toastTimer = setTimeout(function () { t.classList.add("hidden"); }, 3200);
  }
  function fmtSym(s) {
    s = String(s || ""); var otc = /_otc$/i.test(s); s = s.replace(/_otc$/i, "");
    if (/^[A-Za-z]{6}$/.test(s)) s = s.slice(0, 3).toUpperCase() + "/" + s.slice(3).toUpperCase();
    return s + (otc ? " (OTC)" : "");
  }
  function clock(ts, tzMin) {
    var d = new Date((Number(ts) + (Number(tzMin) || 0) * 60) * 1000);
    return d.toISOString().slice(11, 16);
  }
  function systemDark() {
    try { if (window.Android && window.Android.isNight) return !!window.Android.isNight(); } catch (e) {}
    return !!(window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches);
  }
  var lastBars = "";
  function applyTheme(v) {
    var eff = (v === "light" || v === "dark") ? v : (systemDark() ? "dark" : "light");
    document.documentElement.setAttribute("data-theme", eff);
    var bg = getComputedStyle(document.documentElement).getPropertyValue("--bg").trim();
    var key = eff + bg;
    if (key !== lastBars && bg && window.Android && window.Android.setBars) {
      lastBars = key;
      try { window.Android.setBars(bg, eff === "dark"); } catch (e) {}
    }
  }

  // ---------- tabs ----------
  function showTab(name) {
    S.tab = name;
    document.querySelectorAll(".tab").forEach(function (x) { x.classList.toggle("active", x.id === "tab-" + name); });
    document.querySelectorAll("#tabs button").forEach(function (b) { b.classList.toggle("on", b.dataset.t === name); });
    $("main").scrollTop = 0;
    if (name === "markets") loadMarkets(false);
    if (name === "log") pollLog();
    if (name === "settings") fillSettings(true);
  }
  $("tabs").addEventListener("click", function (e) {
    var b = e.target.closest("button"); if (b) showTab(b.dataset.t);
  });
  $("btnPick").onclick = function () { showTab("markets"); };

  // ---------- state ----------
  var LINK = {
    disconnected: ["Not connected", "off", "Offline"], connecting: ["Connecting...", "wait", "Connecting"],
    online: ["Connected", "on", "Online"], reconnecting: ["Reconnecting...", "wait", "Reconnecting"],
    error: ["Connection failed", "err", "Error"]
  };
  async function pollState() {
    try {
      var r = await api("/api/state", undefined, 8000);
      if (!r.ok) throw new Error("state " + r.status);
      S.offline = 0; S.st = r.data;
      applyTheme(r.data.settings.theme);
      if (!S.selInit) { S.sel = new Set(r.data.settings.markets || []); S.selInit = true; }
      renderHome();
    } catch (e) {
      S.offline++;
      if (S.offline > 2) { var p = $("pill"); p.className = "pill err"; $("pillText").textContent = "App not ready"; }
    }
  }
  function renderHome() {
    var st = S.st, L = LINK[st.link] || LINK.disconnected;
    $("pill").className = "pill " + L[1]; $("pillText").textContent = L[2];
    $("linkText").textContent = L[0];
    var sub = "";
    if (st.link === "online" && typeof st.age === "number") sub = "Last data " + (st.age < 2 ? "just now" : Math.round(st.age) + "s ago");
    $("linkSub").textContent = sub;
    var bEl = $("balance"); bEl.textContent = "";
    if (st.balance && (typeof st.balance.demo === "number" || typeof st.balance.live === "number")) {
      [["demo", "Demo"], ["live", "Live"]].forEach(function (k) {
        if (typeof st.balance[k[0]] !== "number") return;
        var line = el("div", "small", k[1] + " " + st.balance[k[0]].toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
        bEl.appendChild(line);
      });
    } else bEl.textContent = "-";
    var le = $("linkError");
    if (st.link === "error" && st.link_error) { le.textContent = st.link_error; le.classList.remove("hidden"); } else le.classList.add("hidden");
    var bc = $("btnConnect"), busy = st.link === "connecting";
    var connected = st.link === "online" || st.link === "reconnecting";
    bc.textContent = connected ? "Disconnect" : (busy ? "Connecting..." : "Connect");
    bc.disabled = busy;

    var running = st.engine === "running" || st.engine === "stopping" || st.engine === "starting";
    $("sessionCard").classList.toggle("hidden", !running);
    $("startCard").classList.toggle("hidden", running);
    if (running) {
      var x = st.session || {};
      $("sessState").textContent = st.engine === "starting" ? "LOADING" : st.engine === "stopping" ? "STOPPING" : "RUNNING";
      $("stWins").textContent = x.wins || 0; $("stLoss").textContent = x.losses || 0;
      $("stPct").textContent = x.pct === null || x.pct === undefined ? "-" : x.pct + "%";
      $("stSig").textContent = x.signals || 0;
      $("sessInfo").textContent = (x.markets || 0) + " markets" + (x.started ? " - running " + dur(st.now - x.started) : "");
      var tb = $("tradeBox");
      if (x.trade) {
        tb.classList.remove("hidden"); tb.textContent = "";
        var left = el("div"); left.appendChild(el("b", "", fmtSym(x.trade.symbol)));
        left.appendChild(el("div", "small muted", "Entry " + clock(x.trade.entry_ts, x.tz_min) + (x.trade.step ? " - MTG step " + x.trade.step : "")));
        var call = x.trade.dir > 0;
        tb.appendChild(left); tb.appendChild(el("span", "chip " + (call ? "good" : "bad"), call ? "CALL" : "PUT"));
      } else tb.classList.add("hidden");
      $("btnPartial").disabled = st.engine !== "running"; $("btnStop").disabled = st.engine !== "running";
    } else {
      var n = S.sel.size;
      $("startInfo").textContent = n ? n + (n === 1 ? " market selected" : " markets selected") : "No markets selected";
      $("btnStart").disabled = !(st.link === "online") || n === 0;
      $("btnStart").textContent = st.link === "online" ? "Start signals" : "Connect first";
      var ee = $("engineError");
      if (st.engine_error) { ee.textContent = st.engine_error; ee.classList.remove("hidden"); } else ee.classList.add("hidden");
    }
    var lr = st.last_result, lc = $("lastCard");
    if (lr && !running) {
      lc.classList.remove("hidden");
      $("lastText").textContent = lr.wins + " W / " + lr.losses + " L" + (lr.pct === null ? "" : "  -  " + lr.pct + "%") + "  -  " + lr.signals + " signals";
    } else lc.classList.add("hidden");
    $("rendererNote").textContent = st.renderer ? "Chart renderer: ready" : "Chart renderer: not running (charts will be skipped)";
    $("about").textContent = "DARKMATTER app " + st.version + " - signal engine " + st.engine_version;
  }
  function dur(sec) {
    sec = Math.max(0, Math.floor(sec)); var h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60);
    return h ? h + "h " + m + "m" : m + "m " + (sec % 60) + "s";
  }

  // ---------- home actions ----------
  $("btnConnect").onclick = async function () {
    var st = S.st; if (!st) return;
    var connected = st.link === "online" || st.link === "reconnecting";
    $("btnConnect").disabled = true;
    await api(connected ? "/api/disconnect" : "/api/connect", {});
    pollState();
  };
  function openLogin() {
    if (window.Android && window.Android.openLogin) window.Android.openLogin();
    else toast("Login works inside the Android app.");
  }
  $("btnLogin").onclick = openLogin; $("btnLogin2").onclick = openLogin;
  $("btnBg").onclick = function () {
    if (window.Android && window.Android.allowBackground) window.Android.allowBackground(); else toast("Works inside the Android app.");
  };
  $("btnExit").onclick = function () {
    if (window.Android && window.Android.exitApp) window.Android.exitApp(); else toast("Works inside the Android app.");
  };
  $("btnDisc").onclick = async function () { await api("/api/disconnect", {}); toast("Disconnected"); pollState(); };
  $("btnStart").onclick = async function () {
    $("btnStart").disabled = true;
    var r = await api("/api/start", { symbols: Array.from(S.sel) });
    if (!r.ok) toast(r.data.error || "Could not start"); else S.events = [];
    pollState();
  };
  $("btnStop").onclick = async function () { $("btnStop").disabled = true; await api("/api/stop", {}); toast("Stopping - sending the session result"); pollState(); };
  $("btnPartial").onclick = async function () {
    var r = await api("/api/partial", {}); if (!r.ok) toast(r.data.error || "No session"); else toast("Current result is in the Log tab");
  };

  // ---------- feed ----------
  async function pollEvents() {
    if (S.tab !== "home") return;
    try {
      var r = await api("/api/events", undefined, 8000); if (!r.ok) return;
      var ev = (r.data.events || []).slice(-30).reverse(), box = $("feed"), tz = S.st ? S.st.settings.tz_min : 0;
      box.textContent = "";
      if (!ev.length) { box.appendChild(el("div", "empty", "Signals and results appear here.")); return; }
      ev.forEach(function (e) {
        var row = el("div", "item"), left = el("div");
        if (e.kind === "signal") {
          left.appendChild(el("div", "t", fmtSym(e.symbol)));
          left.appendChild(el("div", "s", "Entry " + clock(e.entry_ts, tz) + (e.conf ? " - " + Math.round(e.conf) + "% confidence" : "")));
          row.appendChild(left); var c = e.dir > 0; row.appendChild(el("span", "chip " + (c ? "good" : "bad"), c ? "CALL" : "PUT"));
        } else {
          left.appendChild(el("div", "t", fmtSym(e.symbol) + " - result"));
          left.appendChild(el("div", "s", "Entry " + clock(e.entry_ts, tz) + (e.step ? " - MTG " + e.step : "")));
          row.appendChild(left); row.appendChild(el("span", "chip " + (e.win ? "good" : "bad"), e.win ? "WIN" : "LOSS"));
        }
        box.appendChild(row);
      });
    } catch (e) { /* next poll */ }
  }

  // ---------- markets ----------
  async function loadMarkets(force) {
    var msg = $("mMsg");
    if (!S.st || S.st.link !== "online") { msg.textContent = "Connect to Quotex to see the markets."; $("mList").textContent = ""; return; }
    if (S.loadingMarkets || (S.markets && !force)) { renderMarkets(); return; }
    S.loadingMarkets = true; msg.textContent = "Loading markets...";
    try {
      var r = await api("/api/markets", undefined, 30000);
      if (r.ok) { S.markets = r.data.markets || []; msg.textContent = ""; }
      else msg.textContent = r.data.error || "Could not load the markets.";
    } catch (e) { msg.textContent = "Could not load the markets."; }
    S.loadingMarkets = false; renderMarkets();
  }
  function renderMarkets() {
    var list = $("mList"); list.textContent = "";
    $("mCount").textContent = S.sel.size + " selected";
    if (!S.markets) return;
    var q = S.search.trim().toLowerCase(), shown = 0;
    S.markets.forEach(function (m) {
      if (S.kind === "live" && m.otc) return; if (S.kind === "otc" && !m.otc) return;
      if (q && (m.symbol + " " + m.name).toLowerCase().indexOf(q) < 0) return;
      shown++;
      var row = el("div", "mk" + (S.sel.has(m.symbol) ? " sel" : "") + (m.open ? "" : " closed"));
      row.appendChild(el("div", "box"));
      var nm = el("div", "nm"); nm.appendChild(el("b", "", fmtSym(m.symbol)));
      nm.appendChild(el("span", "", (m.name || "") + (m.open ? "" : " - closed"))); row.appendChild(nm);
      row.appendChild(el("div", "pay", m.payout === null || m.payout === undefined ? "-" : Math.round(m.payout) + "%"));
      row.onclick = function () {
        if (S.sel.has(m.symbol)) S.sel.delete(m.symbol); else S.sel.add(m.symbol);
        row.classList.toggle("sel"); $("mCount").textContent = S.sel.size + " selected"; saveMarkets(); renderHome();
      };
      list.appendChild(row);
    });
    if (!shown) $("mMsg").textContent = S.markets.length ? "No market matches." : "The broker returned no markets.";
    else $("mMsg").textContent = "";
  }
  function saveMarkets() {
    clearTimeout(S.saveTimer);
    S.saveTimer = setTimeout(function () { api("/api/settings", { markets: Array.from(S.sel) }); }, 600);
  }
  $("mSearch").oninput = function () { S.search = this.value; renderMarkets(); };
  $("mKind").onclick = function (e) {
    var b = e.target.closest("button"); if (!b) return; S.kind = b.dataset.k;
    document.querySelectorAll("#mKind button").forEach(function (x) { x.classList.toggle("on", x === b); }); renderMarkets();
  };
  $("mOpen").onclick = function () {
    (S.markets || []).forEach(function (m) { if (m.open) S.sel.add(m.symbol); }); renderMarkets(); saveMarkets(); renderHome();
  };
  $("mClear").onclick = function () { S.sel.clear(); renderMarkets(); saveMarkets(); renderHome(); };
  $("mRefresh").onclick = function () { loadMarkets(true); };

  // ---------- log ----------
  var logLines = [];
  async function pollLog() {
    if (S.tab !== "log") return;
    try {
      var r = await api("/api/log?since=" + S.logSeq, undefined, 8000); if (!r.ok) return;
      var add = r.data.lines || []; S.logSeq = r.data.last;
      if (!add.length) return;
      var box = $("logBox"), stick = box.scrollTop + box.clientHeight >= box.scrollHeight - 40;
      add.forEach(function (l) { logLines.push(l.text); });
      if (logLines.length > 1500) logLines = logLines.slice(-1500);
      box.textContent = logLines.join("\n");
      if (stick) box.scrollTop = box.scrollHeight;
    } catch (e) { /* next poll */ }
  }
  $("logClear").onclick = async function () { await api("/api/clearlog", {}); logLines = []; $("logBox").textContent = ""; };

  // ---------- settings ----------
  function setSwitch(id, on) { $(id).setAttribute("aria-checked", on ? "true" : "false"); }
  function setSeg(id, v) { document.querySelectorAll("#" + id + " button").forEach(function (b) { b.classList.toggle("on", String(b.dataset.v) === String(v)); }); }
  function tzText(min) {
    var s = min < 0 ? "-" : "+", a = Math.abs(min), h = Math.floor(a / 60), m = a % 60;
    return "UTC" + s + String(h).padStart(2, "0") + ":" + String(m).padStart(2, "0");
  }
  function fillSettings(force) {
    if (!S.st) return; var s = S.st.settings, ae = document.activeElement;
    function put(id, v) { var e = $(id); if (e !== ae || force) e.value = v; }
    put("f_n", s.n); put("f_owner", s.owner); put("f_tz", tzText(s.tz_min)); put("f_tok", s.tg_token); put("f_chat", s.tg_chat);
    setSeg("f_mtg", s.mtg); setSeg("themeSeg", s.theme);
    setSwitch("s_tg", s.tg_on); setSwitch("s_sig", s.chart_sig); setSwitch("s_res", s.chart_res);
  }
  var ERR_IDS = ["n", "owner", "tz_min", "mtg", "tg_on", "tg_token", "tg_chat"];
  async function saveSetting(patch) {
    var r = await api("/api/settings", patch);
    ERR_IDS.forEach(function (k) { var e = $("e_" + k); if (e) e.textContent = ""; });
    var errs = r.data.errors || {};
    Object.keys(errs).forEach(function (k) { var e = $("e_" + k); if (e) e.textContent = errs[k]; });
    if (r.data.settings && S.st) { S.st.settings = r.data.settings; applyTheme(r.data.settings.theme); }
    fillSettings(false);
    return Object.keys(errs).length === 0;
  }
  function bindText(id, key) {
    var t = null;
    $(id).addEventListener("input", function () {
      var v = this.value; clearTimeout(t);
      t = setTimeout(function () { var p = {}; p[key] = v; saveSetting(p); }, 700);
    });
    $(id).addEventListener("blur", function () { fillSettings(true); });
  }
  bindText("f_n", "n"); bindText("f_owner", "owner"); bindText("f_tz", "tz_min"); bindText("f_tok", "tg_token"); bindText("f_chat", "tg_chat");
  $("f_mtg").onclick = function (e) { var b = e.target.closest("button"); if (b) saveSetting({ mtg: Number(b.dataset.v) }); };
  $("themeSeg").onclick = function (e) { var b = e.target.closest("button"); if (b) { applyTheme(b.dataset.v); saveSetting({ theme: b.dataset.v }); } };
  function bindSwitch(id, key) {
    $(id).onclick = function () { var on = this.getAttribute("aria-checked") !== "true"; var p = {}; p[key] = on; saveSetting(p); };
  }
  bindSwitch("s_tg", "tg_on"); bindSwitch("s_sig", "chart_sig"); bindSwitch("s_res", "chart_res");

  function testMsg(ok, text) {
    var m = $("testMsg"); m.className = "alert " + (ok ? "good" : "bad"); m.textContent = text;
  }
  $("btnTgTest").onclick = async function () {
    var b = this; b.disabled = true; testMsg(true, "Sending a test message ...");
    try {
      var r = await api("/api/test/telegram", { chart: true }, 70000), d = r.data;
      if (d.ok) testMsg(true, "Telegram works" + (d.bot ? " (@" + d.bot + ")" : "") + (d.chart ? ", chart image included." : "." + (d.chart_error ? " Chart image skipped: " + d.chart_error : "")));
      else testMsg(false, d.error || "Telegram test failed");
    } catch (e) { testMsg(false, "The test took too long."); }
    b.disabled = false;
  };
  $("btnChartTest").onclick = async function () {
    var b = this; b.disabled = true; testMsg(true, "Drawing a test chart ...");
    var img = $("testImg"); img.classList.add("hidden");
    try {
      var r = await api("/api/test/chart", {}, 60000), d = r.data;
      if (d.ok) { img.src = "data:image/png;base64," + d.png; img.classList.remove("hidden"); testMsg(true, "Chart image works (" + Math.round(d.bytes / 1024) + " KB)."); }
      else testMsg(false, d.error || "Chart test failed");
    } catch (e) { testMsg(false, "The test took too long."); }
    b.disabled = false;
  };

  // ---------- start ----------
  pollState().then(function () { fillSettings(true); });
  setInterval(pollState, 1500);
  setInterval(pollLog, 1000);
  setInterval(pollEvents, 2000);
  setInterval(function () { if (S.tab === "markets" && S.st && S.st.link === "online" && !S.markets) loadMarkets(false); }, 3000);
})();
