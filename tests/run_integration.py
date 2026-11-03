# -*- coding: utf-8 -*-
"""
End-to-end test of the app backend WITHOUT a phone.

  fake Quotex page (answers like the real socket)  ->  dm_app.on_frames  ->  QuotexWS (passive, unmodified)
  -> LiveSignalSession (unmodified)  ->  AndroidChartEngine  <->  fake renderer (Pillow)  ->  fake Telegram

Run:  python3 tests/run_integration.py
"""
import base64
import io
import json
import math
import os
import random
import sys
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "app", "src", "main", "python"))
OUT = sys.__stdout__


def say(*a):
    print(*a, file=OUT, flush=True)


import darkmatter_bot as bot          # noqa: E402
import dm_app                         # noqa: E402

FAILS = []


def check(name, cond, extra=""):
    say(("PASS  " if cond else "FAIL  ") + name + (f"   {extra}" if extra and not cond else ""))
    if not cond:
        FAILS.append(name)


# ------------------------------------------------------------------ fake Telegram
TG = {"messages": [], "photos": [], "bad": []}


class TgHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n)
        method = self.path.rsplit("/", 1)[-1]
        res = {"ok": True, "result": {}}
        if method == "getMe":
            res["result"] = {"id": 1, "is_bot": True, "username": "dm_test_bot"}
        elif method == "sendMessage":
            TG["messages"].append(json.loads(raw.decode("utf-8")))
        elif method == "sendPhoto":
            i = raw.find(b"\x89PNG\r\n\x1a\n")
            TG["photos"].append({"png": i >= 0, "size": len(raw)})
            if i < 0:
                TG["bad"].append("sendPhoto without PNG")
        body = json.dumps(res).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST


# ------------------------------------------------------------------ fake Quotex page
SYMS = [("EURUSD_otc", "EUR/USD (OTC)", 86), ("GBPUSD_otc", "GBP/USD (OTC)", 82), ("USDJPY_otc", "USD/JPY (OTC)", 79),
        ("AUDCAD", "AUD/CAD", 80), ("EURGBP", "EUR/GBP", 74)]


def price_at(asset, minute):
    seed = sum(ord(c) for c in asset)
    rnd = random.Random(seed * 100003 + int(minute))
    return 1.1 + 0.004 * math.sin(minute / 23.0 + seed) + 0.0004 * math.sin(minute / 3.1) + rnd.uniform(-0.00015, 0.00015)


class FakePage:
    def __init__(self):
        self.subs, self.opened, self.lock, self.stop = set(), 0, threading.Lock(), False
        self.sent = []
        threading.Thread(target=self._ticker, daemon=True).start()

    # ---- the API the Android side offers to Python
    def openBridge(self, url):
        self.opened += 1
        threading.Thread(target=self._boot, daemon=True).start()

    def closeBridge(self):
        self.subs.clear()

    def reloadBridge(self):
        pass

    def sendToPage(self, text):
        self.sent.append(text)
        if not text.startswith("42"):
            return
        name, *arg = json.loads(text[2:])
        arg = arg[0] if arg else None
        if name == "instruments/get":
            self._emit([["t", "42" + json.dumps(["instruments/list", self._rows()])]])
        elif name == "instruments/update":
            self.subs.add(arg["asset"])
        elif name == "history/load":
            self._history(arg)

    # ---- the page's side
    def _emit(self, items):
        dm_app.on_frames(json.dumps(items))

    def _rows(self):
        rows = []
        for i, (sym, name, pay) in enumerate(SYMS, 1):
            row = [i, sym, name, "currency", 1, pay] + [0] * 8 + [1] + [0] * 10
            row[-8] = pay - 3
            rows.append(row)
        return rows

    def _boot(self):
        time.sleep(0.3)
        self._emit([["o", ""], ["p", json.dumps({"href": "https://market-qx.info/en/trade", "token": "tok"})],
                    ["t", '0{"sid":"abc"}'], ["t", "40"],
                    ["t", '42["s_authorization",{}]'],
                    ["t", '42["s_balance/update",{"demoBalance":1000.5,"liveBalance":12.0}]']])
        time.sleep(0.3)
        self._emit([["t", "42" + json.dumps(["instruments/list", self._rows()])]])

    def _history(self, a):
        asset, idx, end, off, per = a["asset"], a["index"], int(a["time"]), int(a["offset"]), a["period"]
        first = (end - off) // 60
        rows = []
        for m in range(first, end // 60):
            o, c = price_at(asset, m), price_at(asset, m + 1)
            rows.append({"time": m * 60, "open": o, "close": c, "high": max(o, c) + 0.0001,
                         "low": min(o, c) - 0.0001, "ticks": 20})
        payload = {"asset": asset, "index": idx, "period": per, "data": rows}
        self._emit([["t", '451-["history/load",{"_placeholder":true,"num":0}]'],
                    ["b", base64.b64encode(json.dumps(payload).encode()).decode()]])

    def _ticker(self):
        while not self.stop:
            time.sleep(1.0)
            if not self.subs:
                continue
            now = time.time()
            ticks = []
            for a in list(self.subs):
                m = now / 60.0
                p = price_at(a, math.floor(m)) + (price_at(a, math.floor(m) + 1) - price_at(a, math.floor(m))) * (m - math.floor(m))
                ticks.append([a, round(now, 3), round(p, 5)])
            self._emit([["t", "42" + json.dumps(["quotes", ticks])]])


# ------------------------------------------------------------------ fake chart renderer (what the WebView does)
def fake_renderer(base, token, stop):
    from PIL import Image
    H = {"X-Token": token}
    while not stop.is_set():
        try:
            req = urllib.request.Request(base + "/api/render/next", headers=H)
            with urllib.request.urlopen(req, timeout=30) as r:
                if r.status == 204:
                    continue
                job = json.loads(r.read())
        except Exception:
            time.sleep(0.2)
            continue
        out = {"id": job["id"]}
        try:
            if "<svg" not in job["svg"] or 'width="' not in job["svg"]:
                raise ValueError("not an svg")
            buf = io.BytesIO()
            Image.new("RGB", (job["w"], job["h"]), (20, 30, 50)).save(buf, "PNG")
            out["png"] = base64.b64encode(buf.getvalue()).decode()
        except Exception as e:
            out["error"] = str(e)
        req = urllib.request.Request(base + "/api/render/done", data=json.dumps(out).encode(),
                                     headers={**H, "Content-Type": "application/json"}, method="POST")
        urllib.request.urlopen(req, timeout=10).read()


# ------------------------------------------------------------------ helpers
def call(base, token, path, body=None, timeout=30):
    h = {"X-Token": token}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        h["Content-Type"] = "application/json"
    req = urllib.request.Request(base + path, data=data, headers=h, method="POST" if body is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except Exception:
            return e.code, {}


def wait(cond, timeout, step=0.5):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(step)
    return False


def main():
    import hashlib
    eng = os.path.join(HERE, "..", "app", "src", "main", "python", "darkmatter_bot.py")
    sha = hashlib.sha256(open(eng, "rb").read()).hexdigest()
    check("engine file is byte-identical to the uploaded darkmatter_bot.py",
          sha == "6219fb25e8d9c1012a6c99ade691efb1331f548f6e04ba096e7a95438f2b22af", sha)
    dm_app.on_frames("not json")          # must never raise (no bridge yet)
    # unit checks of the settings validation
    cur = dict(dm_app.DEFAULTS)
    new, errs = dm_app.clean_settings({"n": "50"}, cur)
    check("n below minimum is rejected", "n" in errs and new["n"] == cur["n"])
    new, errs = dm_app.clean_settings({"n": "250", "tz_min": "+5:30", "mtg": 2, "owner": "  My  Name "}, cur)
    check("valid values accepted", not errs and new["n"] == 250 and new["tz_min"] == 330 and new["mtg"] == 2
          and new["owner"] == "My Name", str(errs))
    new, errs = dm_app.clean_settings({"tg_on": True}, cur)
    check("telegram on without token is rejected", "tg_on" in errs and new["tg_on"] is False)
    new, errs = dm_app.clean_settings({"tg_token": "nope"}, cur)
    check("bad telegram token rejected", "tg_token" in errs)
    new, errs = dm_app.clean_settings({"theme": "purple"}, cur)
    check("bad theme rejected", "theme" in errs)

    tg = ThreadingHTTPServer(("127.0.0.1", 0), TgHandler)
    threading.Thread(target=tg.serve_forever, daemon=True).start()
    os.environ["DM_TG_BASE"] = f"http://127.0.0.1:{tg.server_address[1]}"

    tmp = tempfile.mkdtemp()
    page = FakePage()
    info = json.loads(dm_app.start(tmp, page))
    base, token = f"http://127.0.0.1:{info['port']}", info["token"]
    say("backend on", base)

    check("static index is served", b"DARKMATTER" in urllib.request.urlopen(base + "/").read())
    check("renderer page is served", b"renderer.js" in urllib.request.urlopen(base + "/renderer?t=x").read())
    check("app.js is served", b"use strict" in urllib.request.urlopen(base + "/app.js").read())
    try:
        urllib.request.urlopen(base + "/api/state", timeout=5)
        check("api without token is refused", False)
    except urllib.error.HTTPError as e:
        check("api without token is refused", e.code == 401)
    code, st = call(base, token, "/api/state")
    check("state works", code == 200 and st["link"] == "disconnected" and st["engine"] == "idle")

    stop = threading.Event()
    threading.Thread(target=fake_renderer, args=(base, token, stop), daemon=True).start()
    check("renderer becomes alive", wait(lambda: call(base, token, "/api/state")[1]["renderer"], 10))

    # settings round trip
    code, r = call(base, token, "/api/settings", {"n": 120, "tz_min": "+6", "mtg": 0, "owner": "@TestOwner",
                                                  "tg_token": "123456:ABCDEFGHIJKLMNOPQRSTUVWXYZ012345",
                                                  "tg_chat": "-1001234567890", "tg_on": True,
                                                  "chart_sig": True, "chart_res": True, "theme": "dark"})
    check("settings saved", code == 200 and r["settings"]["tg_on"] and r["settings"]["n"] == 120, str(r))
    code, r = call(base, token, "/api/settings", {"n": 5})
    check("bad setting returns 400 and keeps old value", code == 400 and r["settings"]["n"] == 120)
    saved = json.load(open(os.path.join(tmp, "settings.json"), encoding="utf-8"))
    check("settings file written", saved["owner"] == "@TestOwner" and saved["theme"] == "dark")

    # chart + telegram tests
    code, r = call(base, token, "/api/test/chart", {})
    check("chart test returns a PNG", r.get("ok") and base64.b64decode(r["png"])[:4] == b"\x89PNG", str(r)[:200])
    code, r = call(base, token, "/api/test/telegram", {"chart": True}, 90)
    check("telegram test ok with chart", r.get("ok") and r.get("chart") and TG["photos"], str(r))

    # connect
    code, _ = call(base, token, "/api/connect", {})
    check("connect reaches online", wait(lambda: call(base, token, "/api/state")[1]["link"] == "online", 40))
    code, st = call(base, token, "/api/state")
    check("balance parsed", st.get("balance", {}).get("demo") == 1000.5, str(st.get("balance")))
    code, r = call(base, token, "/api/markets", timeout=40)
    mk = r.get("markets", [])
    check("markets listed", len(mk) == len(SYMS) and mk[0]["payout"] is not None, str(r)[:200])
    code, r = call(base, token, "/api/start", {"symbols": []})
    check("start without markets is refused", code == 400)

    # speed the scan up for the test only (constants of the engine are read at call time)
    sec = int(time.time()) % 60
    if sec > 30:
        say("waiting for the next minute so the scan window fits ...")
        time.sleep(61 - sec + 1)
        sec = int(time.time()) % 60
    bot.MIN_CONF = 0.0
    bot.SIGNAL_SECOND = sec + 4
    bot.SIGNAL_LATEST_START = min(sec + 9, 44)

    pick = [m["symbol"] for m in mk if m["open"] is not False][:3]
    code, r = call(base, token, "/api/start", {"symbols": pick}, 60)
    check("session starts", code == 200, str(r))
    check("session reaches running", wait(lambda: call(base, token, "/api/state")[1]["engine"] == "running", 60))
    code, r = call(base, token, "/api/partial", {})
    check("partial result works while running", code == 200, str(r))

    say("waiting for a signal and its result (about 2-3 minutes) ...")
    got_sig = wait(lambda: any(e["kind"] == "signal" for e in call(base, token, "/api/events")[1]["events"]), 100)
    check("a signal was produced by the engine", got_sig)
    got_res = wait(lambda: any(e["kind"] == "result" for e in call(base, token, "/api/events")[1]["events"]), 150)
    check("the signal's result was produced", got_res)

    code, st = call(base, token, "/api/state")
    sess = st.get("session") or {}
    check("state shows session numbers", sess.get("markets") == len(pick) and sess.get("signals", 0) >= 1, str(sess))

    code, _ = call(base, token, "/api/stop", {})
    check("session goes back to idle after stop", wait(lambda: call(base, token, "/api/state")[1]["engine"] == "idle", 60))
    code, st = call(base, token, "/api/state")
    check("last result kept", st.get("last_result") and st["last_result"]["signals"] >= 1, str(st.get("last_result")))
    check("no session error", not st.get("engine_error"), str(st.get("engine_error")))

    time.sleep(3)
    say(f"telegram received: {len(TG['messages'])} text message(s), {len(TG['photos'])} photo(s)")
    check("telegram got the text messages", len(TG["messages"]) >= 1, str(len(TG["messages"])))
    check("telegram got chart photos (start/signal/result)", len(TG["photos"]) >= 3, str(TG["photos"]))
    check("every photo was a real PNG", not TG["bad"], str(TG["bad"]))
    code, r = call(base, token, "/api/log?since=0")
    text = "\n".join(x["text"] for x in r["lines"])
    check("log has the engine output", "LIVE SIGNAL" in text.upper(), text[-400:])
    check("log has no ANSI escape codes", "\x1b[" not in text)
    check("log has no Python traceback", "Traceback" not in text, text[-600:])

    code, _ = call(base, token, "/api/disconnect", {})
    check("disconnect works", wait(lambda: call(base, token, "/api/state")[1]["link"] == "disconnected", 20))
    stop.set()
    page.stop = True
    say("\n%d check(s) failed" % len(FAILS) if FAILS else "\nALL CHECKS PASSED")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
