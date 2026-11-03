# -*- coding: utf-8 -*-
"""
DARKMATTER app backend.

Runs the UNMODIFIED darkmatter_bot.py engine inside the Android app. Nothing in the signal engine is
changed: this file only replaces the three pieces that cannot exist on a phone, from the outside:

  * the Chromium "bridge"   -> the Quotex page runs in an Android WebView (WebViewTransport below)
  * the chart picture maker -> Chromium screenshot replaced by a WebView canvas (AndroidChartEngine)
  * the keyboard watcher    -> buttons in the app (NullKeyWatcher)

The UI (dm_ui/) talks to this file through a small token-protected HTTP API on 127.0.0.1.
"""
import asyncio
import base64
import hmac
import json
import os
import pkgutil
import queue
import re
import secrets
import sys
import threading
import time
import traceback
import uuid
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import darkmatter_bot as bot

APP_VERSION = "1.0.0"
PREFERRED_PORT = 18765

# ---------------------------------------------------------------------------------------------
#  Settings (validated with the bot's own parsers, saved as JSON in the app's private folder)
# ---------------------------------------------------------------------------------------------
DEFAULTS = {
    "theme": "system",                              # system | light | dark
    "n": bot.SIGNAL_CANDLES_DEFAULT,
    "owner": bot.OWNER_DEFAULT,
    "tz_min": bot.TZ_DEFAULT_MIN,
    "mtg": bot.MTG_DEFAULT,
    "tg_on": False,
    "tg_token": "",
    "tg_chat": "",
    "chart_sig": True,
    "chart_res": True,
    "markets": [],
}
_TOKEN_RE = re.compile(r"^\d{5,}:[A-Za-z0-9_-]{20,}$")
_CHAT_RE = re.compile(r"^(@[A-Za-z0-9_]{4,}|-?\d{5,})$")


def _c_theme(v, cur):
    if v not in ("system", "light", "dark"):
        raise ValueError("Choose system, light or dark.")
    return v


def _c_int(lo, hi):
    def f(v, cur):
        if isinstance(v, bool):
            raise ValueError("Enter a whole number.")
        try:
            n = int(str(v).strip())
        except ValueError:
            raise ValueError("Enter a whole number.") from None
        if not lo <= n <= hi:
            raise ValueError(f"Enter a number from {lo} to {hi}.")
        return n
    return f


def _c_owner(v, cur):
    out = bot.parse_owner(str(v), cur)
    if not out:
        raise ValueError("Enter a name.")
    return out


def _c_tz(v, cur):
    if isinstance(v, bool):
        raise ValueError("Enter a timezone like +6 or UTC+05:30.")
    if isinstance(v, (int, float)):
        total = int(v)
        if not -12 * 60 <= total <= 14 * 60:
            raise ValueError("Timezone must be between UTC-12:00 and UTC+14:00.")
        return total
    got = bot.parse_tz(str(v), None)
    if got is None:
        raise ValueError("Enter a timezone like +6, UTC+05:30 or -3.")
    return got


def _c_bool(v, cur):
    if not isinstance(v, bool):
        raise ValueError("Must be on or off.")
    return v


def _c_token(v, cur):
    t = str(v or "").strip()
    if t and not _TOKEN_RE.match(t):
        raise ValueError("That does not look like a Telegram bot token (123456:ABC...).")
    return t


def _c_chat(v, cur):
    t = str(v or "").strip()
    if t and not _CHAT_RE.match(t):
        raise ValueError("Use a numeric chat ID (-100...) or @channelname.")
    return t


def _c_markets(v, cur):
    if not isinstance(v, list) or len(v) > 80:
        raise ValueError("Invalid market list.")
    out = []
    for s in v:
        s = str(s).strip()
        if s and len(s) <= 40 and s not in out:
            out.append(s)
    return out


_CHECK = {
    "theme": _c_theme,
    "n": _c_int(bot.SIGNAL_CANDLES_MIN, bot.SIGNAL_CANDLES_MAX),
    "owner": _c_owner,
    "tz_min": _c_tz,
    "mtg": _c_int(0, bot.MTG_MAX),
    "tg_on": _c_bool, "chart_sig": _c_bool, "chart_res": _c_bool,
    "tg_token": _c_token, "tg_chat": _c_chat,
    "markets": _c_markets,
}


def clean_settings(patch, current):
    """-> (new settings, {key: error text}). Unknown keys are ignored; a bad value keeps the old one."""
    out, errs = dict(current), {}
    for k, v in (patch or {}).items():
        if k not in DEFAULTS:
            continue
        try:
            out[k] = _CHECK[k](v, current.get(k, DEFAULTS[k]))
        except ValueError as e:
            errs[k] = str(e)
    if out.get("tg_on") and not (out.get("tg_token") and out.get("tg_chat")):
        out["tg_on"] = False
        errs["tg_on"] = "Enter the bot token and the chat ID first."
    return out, errs


class Store:
    def __init__(self, path):
        self.path, self.lock = path, threading.Lock()
        self.data = dict(DEFAULTS)
        try:
            with open(path, encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                for k in DEFAULTS:
                    if k in raw:
                        try:
                            self.data[k] = _CHECK[k](raw[k], DEFAULTS[k])
                        except ValueError:
                            pass
        except (OSError, ValueError):
            pass

    def get(self):
        with self.lock:
            return dict(self.data)

    def update(self, patch):
        with self.lock:
            new, errs = clean_settings(patch, self.data)
            if new != self.data:
                self.data = new
                self._save()
            return dict(self.data), errs

    def _save(self):
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=1)
            os.replace(tmp, self.path)
        except OSError:
            pass


# ---------------------------------------------------------------------------------------------
#  Log + event buffers
# ---------------------------------------------------------------------------------------------
class LogBuffer:
    """File-like object that replaces sys.stdout: keeps the engine's text output as clean lines."""
    encoding = "utf-8"
    errors = "replace"

    def __init__(self, cap=2000):
        self.lines = deque(maxlen=cap)
        self.seq = 0
        self.part = ""
        self.lock = threading.Lock()

    def isatty(self):
        return False

    def reconfigure(self, *a, **k):
        pass

    def flush(self):
        pass

    def fileno(self):
        raise OSError("no file descriptor")

    def write(self, s):
        if not s:
            return 0
        text = bot.strip_ansi(str(s)).replace("\r\n", "\n").replace("\r", "")
        with self.lock:
            buf = self.part + text
            parts = buf.split("\n")
            self.part = parts.pop()
            for ln in parts:
                self._add(ln.rstrip())
        return len(s)

    def _add(self, ln):
        if not ln and (not self.lines or not self.lines[-1][1]):
            return                                           # never two blank lines in a row
        self.seq += 1
        self.lines.append((self.seq, ln, time.time()))

    def note(self, text):
        with self.lock:
            self._add(text)

    def since(self, seq):
        with self.lock:
            return [{"i": i, "text": t, "ts": ts} for i, t, ts in self.lines if i > seq], self.seq


class Events:
    def __init__(self, cap=300):
        self.items, self.seq, self.lock = deque(maxlen=cap), 0, threading.Lock()

    def add(self, kind, **data):
        with self.lock:
            self.seq += 1
            self.items.append({"i": self.seq, "kind": kind, "ts": time.time(), **data})

    def all(self):
        with self.lock:
            return list(self.items)

    def clear(self):
        with self.lock:
            self.items.clear()


# ---------------------------------------------------------------------------------------------
#  Chart pictures: SVG -> PNG in the Android WebView (replaces the Chromium screenshot)
# ---------------------------------------------------------------------------------------------
class AndroidChartEngine:
    """Same public methods as bot.ChartEngine (render / ensure / bind / aclose), so every chart function of
    the engine works as before. The SVG goes to a hidden WebView (renderer.html), which draws it on a canvas
    and sends the PNG back."""
    ALIVE_SEC = 45.0

    def __init__(self):
        self.jobs = queue.Queue()
        self.waiting = {}
        self.lock = threading.Lock()
        self.last_poll = 0.0
        self.ok = 0
        self.failed = 0
        self.last_error = None

    def bind(self, loop):
        pass

    async def aclose(self):
        pass

    def alive(self):
        return time.time() - self.last_poll < self.ALIVE_SEC

    def ensure(self, timeout=20.0):
        end = time.time() + min(float(timeout), 20.0)
        while True:
            if self.alive():
                return True, None
            if time.time() >= end:
                return False, "the chart renderer (hidden WebView) is not running"
            time.sleep(0.25)

    def render(self, svg, w, h, timeout=60.0):
        if not self.alive():
            self.failed += 1
            self.last_error = "the chart renderer (hidden WebView) is not running"
            raise RuntimeError(self.last_error)
        jid = uuid.uuid4().hex
        ev, box = threading.Event(), {}
        with self.lock:
            self.waiting[jid] = (ev, box)
        self.jobs.put({"id": jid, "svg": svg, "w": int(w), "h": int(h)})
        try:
            if not ev.wait(timeout):
                raise RuntimeError(f"the chart renderer did not answer within {int(timeout)}s")
            if box.get("error"):
                raise RuntimeError(box["error"])
            png = box.get("png")
            if not png or png[:8] != b"\x89PNG\r\n\x1a\n":
                raise RuntimeError("the chart renderer returned no PNG")
            self.ok += 1
            return png
        except RuntimeError as e:
            self.failed += 1
            self.last_error = str(e)
            raise
        finally:
            with self.lock:
                self.waiting.pop(jid, None)

    # -- called by the HTTP server on behalf of the renderer page ---------------------------------
    def next_job(self, wait=20.0):
        self.last_poll = time.time()
        end = time.time() + wait
        while True:
            try:
                job = self.jobs.get(timeout=max(0.05, min(1.0, end - time.time())))
            except queue.Empty:
                self.last_poll = time.time()
                if time.time() >= end:
                    return None
                continue
            with self.lock:
                if job["id"] not in self.waiting:            # the caller gave up long ago
                    continue
            self.last_poll = time.time()
            return job

    def finish_job(self, jid, png_b64, error):
        with self.lock:
            hit = self.waiting.get(jid)
        if not hit:
            return False
        ev, box = hit
        if error:
            box["error"] = str(error)[:300]
        else:
            try:
                box["png"] = base64.b64decode(png_b64 or "", validate=False)
            except Exception:
                box["error"] = "bad PNG data"
        ev.set()
        return True


class NullKeyWatcher:
    """Replaces the terminal keyboard watcher (there is no keyboard / stdin in the app)."""
    def __init__(self, loop=None, on_stop=None, on_partial=None, *a, **k):
        self.pressed = threading.Event()
        self.dead = False

    def start(self):
        return True

    def stop(self):
        pass

    def __getattr__(self, name):
        raise AttributeError(name)


# ---------------------------------------------------------------------------------------------
#  Quotex page bridge: frames come from the WebView, sends go through the page
# ---------------------------------------------------------------------------------------------
class WebViewTransport:
    last_error = None

    def __init__(self, bridge):
        self.bridge = bridge

    async def connect(self, *a):
        pass

    async def send_str(self, text):
        if not self.bridge.socket_open:
            raise bot.QuotexError("The Quotex page has no open trading socket (still loading, or logged out).")
        self.bridge.native_call("sendToPage", text)

    async def recv(self):
        return await self.bridge.q.get()

    async def close(self):
        pass


class AppBridge:
    def __init__(self, rt):
        self.rt = rt
        self.q = asyncio.Queue()
        self.loop = asyncio.get_running_loop()
        self.socket_open = False
        self.href = ""
        self.has_token = False
        self.frames = 0
        self.closed = False

    def native_call(self, name, *args):
        nat = self.rt.native
        if nat is None:
            raise bot.QuotexError("The Android side is not ready.")
        getattr(nat, name)(*args)

    def on_frames(self, payload):
        """Called from the Android side (any thread) with a JSON list of [kind, data]."""
        try:
            items = json.loads(payload)
        except (TypeError, ValueError):
            return
        out = []
        for it in items:
            try:
                kind, data = it[0], it[1]
            except Exception:
                continue
            if kind == "t":
                out.append(("text", str(data)))
            elif kind == "b":
                try:
                    out.append(("binary", base64.b64decode(data)))
                except Exception:
                    pass
            elif kind == "o":
                self.socket_open = True
            elif kind == "c":
                self.socket_open = False
            elif kind == "p":
                try:
                    st = json.loads(data)
                    self.href = str(st.get("href") or "")
                    self.has_token = bool(st.get("token"))
                except Exception:
                    pass
        if out and not self.closed:
            self.frames += len(out)
            try:
                self.loop.call_soon_threadsafe(self._put, out)
            except RuntimeError:
                pass

    def _put(self, items):
        for it in items:
            self.q.put_nowait(it)


# ---------------------------------------------------------------------------------------------
#  Runtime: connection + live signal session, all on one asyncio loop in its own thread
# ---------------------------------------------------------------------------------------------
class Runtime:
    def __init__(self, store, log, events, charts):
        self.store, self.log, self.events, self.charts = store, log, events, charts
        self.native = None
        self.loop = None
        self.thread = None
        self.link = "disconnected"       # disconnected | connecting | online | reconnecting | error
        self.link_error = None
        self.engine = "idle"             # idle | starting | running | stopping
        self.engine_error = None
        self.ws = None
        self.bridge = None
        self.session = None
        self.run_task = None
        self.stop_evt = None
        self.supervisor = None
        self.started_at = None
        self.lock = threading.RLock()
        self.items = []                  # last market list (dicts)
        self.last_result = None

    # -- loop plumbing -----------------------------------------------------------------------------
    def start_loop(self):
        if self.thread:
            return
        ready = threading.Event()

        def run():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            ready.set()
            self.loop.run_forever()

        self.thread = threading.Thread(target=run, name="dm-loop", daemon=True)
        self.thread.start()
        ready.wait(5)

    def submit(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def call(self, coro, timeout=30):
        return self.submit(coro).result(timeout)

    # -- connection --------------------------------------------------------------------------------
    async def connect(self):
        with self.lock:
            if self.link in ("connecting", "online", "reconnecting"):
                return
            self.link, self.link_error = "connecting", None
        self.log.note("Connecting to the Quotex page ...")
        try:
            if self.native is None:
                raise bot.QuotexError("The Android service is not ready yet. Try again in a moment.")
            bridge = AppBridge(self)
            ws = bot.QuotexWS(None, {}, debug=False, passive=True)
            ws.attach(WebViewTransport(bridge))
            self.bridge, self.ws = bridge, ws
            url = f"{bot.BASE}/{bot.LANG}/trade"
            self.native.openBridge(url)
            try:
                await asyncio.wait_for(ws._auth.wait(), 60)
            except asyncio.TimeoutError:
                href = (bridge.href or "").lower()
                if any(k in href for k in ("sign-in", "signin", "login")) or not bridge.frames:
                    raise bot.QuotexError("Not logged in to Quotex yet. Open Settings > Quotex account > Log in, "
                                          "then press Connect again.") from None
                raise bot.QuotexError("The Quotex page opened but sent no data. Check the internet connection.") from None
            ws.start_background_refresh()
            try:
                await ws.get_balance(timeout=10)
            except Exception:
                pass
            with self.lock:
                self.link = "online"
            self.supervisor = asyncio.ensure_future(self._supervise())
            self.log.note("Connected.")
        except Exception as e:
            await self._teardown_link()
            with self.lock:
                self.link, self.link_error = "error", _why(e)
            self.log.note("Connection failed: " + _why(e))

    async def _teardown_link(self):
        if self.supervisor:
            self.supervisor.cancel()
            self.supervisor = None
        ws, self.ws = self.ws, None
        br, self.bridge = self.bridge, None
        if br:
            br.closed = True
        if ws:
            try:
                await asyncio.wait_for(ws.close(), 5)
            except Exception:
                pass
        try:
            if self.native is not None:
                self.native.closeBridge()
        except Exception:
            pass

    async def disconnect(self):
        await self.stop_engine(wait=True)
        await self._teardown_link()
        with self.lock:
            self.link, self.link_error = "disconnected", None
        self.log.note("Disconnected.")

    async def _supervise(self):
        """The page reconnects its own socket; when no frame arrives for 30 s the page is reloaded."""
        last_reload = 0.0
        try:
            while True:
                await asyncio.sleep(5)
                ws = self.ws
                if ws is None:
                    return
                age = time.monotonic() - ws.last_rx if ws.last_rx else 1e9
                if age > 30:
                    if self.link == "online":
                        with self.lock:
                            self.link = "reconnecting"
                        self.log.note("No data from Quotex for 30 s - reconnecting ...")
                    if time.monotonic() - last_reload > 45:
                        last_reload = time.monotonic()
                        try:
                            self.native.reloadBridge()
                        except Exception:
                            pass
                elif self.link == "reconnecting":
                    with self.lock:
                        self.link = "online"
                    self.log.note("Connection restored.")
        except asyncio.CancelledError:
            pass

    # -- markets -----------------------------------------------------------------------------------
    async def load_markets(self):
        ws = self.ws
        if ws is None or self.link not in ("online", "reconnecting"):
            raise bot.QuotexError("Not connected.")
        await ws.fresh_instruments(15.0, 8.0)
        live, otc = bot.market_items(ws.instruments)
        items = []
        for x in live + otc:
            items.append({"symbol": x["symbol"], "name": str(x["name"]).replace(" (OTC)", ""),
                          "open": x["open"], "payout": x["payout"], "p5": x["p5"], "otc": bool(x["otc"])})
        self.items = items
        return items

    # -- live signal session (same steps as the engine's own live_signal_flow, without the prompts) --
    async def start_engine(self, symbols):
        with self.lock:
            if self.engine != "idle":
                raise bot.QuotexError("A session is already running.")
            if self.link != "online":
                raise bot.QuotexError("Connect to Quotex first.")
            self.engine, self.engine_error = "starting", None
        try:
            cfgs = self.store.get()
            if not symbols:
                raise bot.QuotexError("Pick at least one market.")
            if cfgs["tg_on"] and not (cfgs["tg_token"] and cfgs["tg_chat"]):
                raise bot.QuotexError("Telegram is on, but the bot token or chat ID is missing.")
            live, otc = bot.market_items(self.ws.instruments)
            by_sym = {x["symbol"]: x for x in live + otc}
            picked = [by_sym[s] for s in symbols if s in by_sym]
            if not picked:
                raise bot.QuotexError("The selected markets are not in the broker's list right now.")
            picked.sort(key=lambda x: x["no"])
        except Exception as e:
            with self.lock:
                self.engine, self.engine_error = "idle", _why(e)
            raise
        self.stop_evt = asyncio.Event()
        self.run_task = asyncio.ensure_future(self._run_session(picked, cfgs))
        return len(picked)

    async def _run_session(self, picked, st):
        ws = self.ws
        notifier = None
        session = None
        started = False
        try:
            cfg = {"n": st["n"], "owner": st["owner"], "tz_min": st["tz_min"], "mtg": st["mtg"], "tg": None}
            if st["tg_on"]:
                cfg["tg"] = {"token": st["tg_token"], "chat": st["tg_chat"], "bot": None, "title": None,
                             "chart_sig": st["chart_sig"], "chart_res": st["chart_res"]}
                notifier = bot.TelegramNotifier(st["tg_token"], st["tg_chat"], base=os.environ.get("DM_TG_BASE") or bot.TG_API)
            session = bot.LiveSignalSession(ws, cfg, notifier=notifier)
            self._observe(session)
            self.session = session
            if notifier is not None:
                notifier.on_error = lambda why: session.note(f"Telegram: {why}")
                notifier.start()
                mods, err = await asyncio.get_running_loop().run_in_executor(None, bot.charts_backend)
                session.charts_ok, session.charts_err = mods is not None, err
                if not session.charts_ok:
                    self.log.note(f"Chart images are off for this session: {err}")
                await session.send_start(picked)
            try:
                ok = await session.load(picked)
            except BaseException:
                ws.tick_cb = None
                raise
            if not ok:
                ws.tick_cb = None
                rows = [f"{it['symbol']}: {why}" for it, why in session.skipped]
                raise bot.QuotexError("No market could be loaded for the live signal engine.\n" + "\n".join(rows))
            bot.put()
            session.print_header()
            started = True
            self.started_at = time.time()
            with self.lock:
                self.engine = "running"
            await session.run(self.stop_evt)
            bot.put()
            session.finish()
            if notifier is not None:
                await notifier.close()
        except asyncio.CancelledError:
            if started:
                try:
                    session.finish(send=False)
                except Exception:
                    pass
            raise
        except Exception as e:
            self.engine_error = _why(e)
            self.log.note("Session stopped: " + self.engine_error)
        finally:
            if notifier is not None and notifier.queue is not None:
                try:
                    await notifier.close(timeout=5.0)
                except Exception:
                    pass
            try:
                if session is not None and started:
                    w, l = session.stats.wins, session.stats.losses
                    self.last_result = {"wins": w, "losses": l, "signals": session.n_signals,
                                        "pct": round(100.0 * w / (w + l), 1) if (w + l) else None,
                                        "ended": time.time()}
            except Exception:
                pass
            with self.lock:
                self.engine = "idle"
            self.stop_evt = None

    def _observe(self, session):
        """Record signals/results for the UI. The original methods always run first and unchanged."""
        ev = self.events
        orig_issue = session._issue

        def issue(*a, **k):
            r = orig_issue(*a, **k)
            try:
                t = session.trade
                if t:
                    ev.add("signal", symbol=t.get("symbol"), dir=t.get("dir"), conf=t.get("conf"),
                           entry_ts=t.get("entry_ts"), payout=t.get("payout"))
            except Exception:
                pass
            return r

        session._issue = issue
        orig_add = session.stats.add

        def add(symbol, direction, entry_ts, win, step, payout=None):
            r = orig_add(symbol, direction, entry_ts, win, step, payout)
            try:
                ev.add("result", symbol=symbol, dir=direction, win=bool(win), step=step, entry_ts=entry_ts)
            except Exception:
                pass
            return r

        session.stats.add = add

    async def stop_engine(self, wait=False):
        task, evt = self.run_task, self.stop_evt
        if self.engine == "idle" or task is None:
            return
        with self.lock:
            if self.engine == "running":
                self.engine = "stopping"
        if evt is not None:
            evt.set()
        if wait:
            try:
                await asyncio.wait_for(asyncio.shield(task), 60)
            except Exception:
                task.cancel()

    def partial(self):
        s = self.session
        if self.engine != "running" or s is None:
            raise bot.QuotexError("No session is running.")
        self.loop.call_soon_threadsafe(s.show_partial)

    # -- state for the UI --------------------------------------------------------------------------
    def snapshot(self):
        st = {"link": self.link, "link_error": self.link_error, "engine": self.engine,
              "engine_error": self.engine_error, "balance": None, "session": None,
              "last_result": self.last_result}
        ws = self.ws
        try:
            bal = ws.balance if ws is not None else None
            if isinstance(bal, dict):
                st["balance"] = {k: float(v) for k, v in bal.items()
                                 if isinstance(v, (int, float)) and not isinstance(v, bool)}
            if ws is not None and ws.last_rx:
                st["age"] = round(time.monotonic() - ws.last_rx, 1)
        except Exception:
            pass
        s = self.session
        if s is not None and self.engine in ("running", "stopping", "starting"):
            try:
                w, l = s.stats.wins, s.stats.losses
                t = s.trade
                st["session"] = {
                    "markets": len(getattr(s, "series", {}) or {}),
                    "wins": w, "losses": l,
                    "pct": round(100.0 * w / (w + l), 1) if (w + l) else None,
                    "signals": s.n_signals,
                    "started": self.started_at,
                    "tz_min": s.cfg.get("tz_min"),
                    "trade": ({"symbol": t.get("symbol"), "dir": t.get("dir"), "entry_ts": t.get("entry_ts"),
                               "step": t.get("step", 0)} if t else None),
                }
            except Exception:
                st["session"] = {"markets": 0, "wins": 0, "losses": 0}
        return st


def _why(e):
    s = str(e).strip()
    return (s or type(e).__name__)[:600]


# ---------------------------------------------------------------------------------------------
#  HTTP API (127.0.0.1 only, every /api call needs the random token)
# ---------------------------------------------------------------------------------------------
G = {}          # store, log, events, charts, rt, token, port, server


def _sample_svg(w=1200, h=600):
    """A small test picture using the same SVG features as the engine's charts."""
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}">'
            '<defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#0b1220"/>'
            '<stop offset="1" stop-color="#1b2a4a"/></linearGradient>'
            f'<clipPath id="c"><rect x="40" y="120" width="{w - 80}" height="{h - 200}" rx="14"/></clipPath></defs>'
            f'<rect width="{w}" height="{h}" fill="url(#g)"/>'
            f'<text x="40" y="80" font-size="44" font-weight="700" fill="#e8eefc" textLength="520" '
            f'lengthAdjust="spacingAndGlyphs" font-family="sans-serif">DARKMATTER chart test</text>'
            f'<g clip-path="url(#c)"><rect x="40" y="120" width="{w - 80}" height="{h - 200}" fill="#0f1a30"/>'
            '<path d="M40 460 L220 380 L400 420 L580 300 L760 340 L940 220 L1160 260" stroke="#4f8cff" '
            'stroke-width="5" fill="none"/>'
            '<rect x="300" y="320" width="26" height="90" fill="#22c55e"/>'
            '<rect x="500" y="260" width="26" height="110" fill="#ef4444"/>'
            '<circle cx="940" cy="220" r="12" fill="#f5c451"/></g>'
            f'<text x="40" y="{h - 40}" font-size="26" fill="#8ea0c4" font-family="sans-serif">'
            'If you can read this, charts will be sent to Telegram.</text></svg>')


def _telegram_test(png_first):
    st = G["store"].get()
    token, chat = st["tg_token"], st["tg_chat"]
    if not token or not chat:
        return {"ok": False, "error": "Enter the bot token and the chat ID first."}
    base = os.environ.get("DM_TG_BASE") or bot.TG_API
    try:
        me = bot._tg_call(token, "getMe", {}, base)
    except Exception as e:
        return {"ok": False, "error": f"Telegram: {_why(e)}"}
    name = (me or {}).get("username") if isinstance(me, dict) else None
    png, chart_err = None, None
    if png_first:
        try:
            png = G["charts"].render(_sample_svg(), 1200, 600, timeout=40)
        except Exception as e:
            chart_err = _why(e)
    try:
        text = "DARKMATTER app: Telegram test OK"
        if png:
            bot._tg_photo(token, chat, png, text, None, base)
        else:
            bot._tg_call(token, "sendMessage", {"chat_id": chat, "text": text}, base)
    except Exception as e:
        return {"ok": False, "error": f"Telegram: {_why(e)}", "bot": name}
    return {"ok": True, "bot": name, "chart": bool(png), "chart_error": chart_err}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "DarkMatterApp"

    def log_message(self, *a):
        pass

    # helpers
    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False, default=str).encode("utf-8")
        elif isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _auth(self):
        tok = self.headers.get("X-Token", "")
        return bool(tok) and hmac.compare_digest(tok, G["token"])

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n <= 0:
            return {}
        if n > 8 * 1024 * 1024:
            raise ValueError("body too large")
        raw = self.rfile.read(n)
        try:
            return json.loads(raw.decode("utf-8"))
        except ValueError:
            return {}

    def _static(self, name, ctype):
        try:
            data = pkgutil.get_data("dm_ui", name)
        except Exception:
            data = None
        if data is None:                                     # fallback: plain file next to the package
            try:
                import dm_ui
                with open(os.path.join(os.path.dirname(dm_ui.__file__), name), "rb") as f:
                    data = f.read()
            except Exception:
                data = None
        if data is None:
            return self._send(404, {"error": "not found"})
        self._send(200, data, ctype)

    # routing
    def do_GET(self):
        self._route("GET")

    def do_POST(self):
        self._route("POST")

    def _route(self, method):
        try:
            u = urlsplit(self.path)
            path, q = u.path, parse_qs(u.query)
            if method == "GET" and not path.startswith("/api/"):
                pages = {"/": ("index.html", "text/html; charset=utf-8"),
                         "/renderer": ("renderer.html", "text/html; charset=utf-8"),
                         "/app.css": ("app.css", "text/css; charset=utf-8"),
                         "/app.js": ("app.js", "application/javascript; charset=utf-8"),
                         "/renderer.js": ("renderer.js", "application/javascript; charset=utf-8")}
                if path in pages:
                    return self._static(*pages[path])
                return self._send(404, {"error": "not found"})
            if not self._auth():
                return self._send(401, {"error": "unauthorized"})
            self._api(method, path, q)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            try:
                self._send(500, {"error": _why(e)})
            except Exception:
                pass

    def _api(self, method, path, q):
        rt, store, log, events, charts = G["rt"], G["store"], G["log"], G["events"], G["charts"]
        if method == "GET":
            if path == "/api/state":
                st = rt.snapshot()
                st.update({"version": APP_VERSION, "engine_version": bot.VERSION,
                           "renderer": charts.alive(), "charts": {"ok": charts.ok, "failed": charts.failed,
                                                                  "error": charts.last_error},
                           "now": time.time(), "settings": store.get()})
                return self._send(200, st)
            if path == "/api/log":
                since = int((q.get("since") or ["0"])[0] or 0)
                lines, last = log.since(since)
                return self._send(200, {"lines": lines, "last": last})
            if path == "/api/events":
                return self._send(200, {"events": events.all()})
            if path == "/api/markets":
                try:
                    items = rt.call(rt.load_markets(), 20)
                except Exception as e:
                    return self._send(400, {"error": _why(e)})
                return self._send(200, {"markets": items})
            if path == "/api/render/next":
                job = charts.next_job(20.0)
                if job is None:
                    return self._send(204, b"", "text/plain")
                return self._send(200, job)
            return self._send(404, {"error": "not found"})

        body = self._body()
        if path == "/api/settings":
            data, errs = store.update(body)
            return self._send(400 if errs else 200, {"settings": data, "errors": errs})
        if path == "/api/connect":
            rt.submit(rt.connect())
            return self._send(200, {"ok": True})
        if path == "/api/disconnect":
            rt.submit(rt.disconnect())
            return self._send(200, {"ok": True})
        if path == "/api/start":
            symbols = body.get("symbols")
            if not isinstance(symbols, list):
                symbols = store.get()["markets"]
            else:
                store.update({"markets": symbols})
            try:
                n = rt.call(rt.start_engine([str(s) for s in symbols]), 20)
            except Exception as e:
                return self._send(400, {"error": _why(e)})
            events.clear()
            return self._send(200, {"ok": True, "markets": n})
        if path == "/api/stop":
            rt.submit(rt.stop_engine())
            return self._send(200, {"ok": True})
        if path == "/api/partial":
            try:
                rt.partial()
            except Exception as e:
                return self._send(400, {"error": _why(e)})
            return self._send(200, {"ok": True})
        if path == "/api/render/done":
            ok = charts.finish_job(str(body.get("id") or ""), body.get("png"), body.get("error"))
            return self._send(200, {"ok": ok})
        if path == "/api/test/telegram":
            res = _telegram_test(bool(body.get("chart", True)))
            return self._send(200, res)
        if path == "/api/test/chart":
            try:
                png = charts.render(_sample_svg(), 1200, 600, timeout=40)
            except Exception as e:
                return self._send(200, {"ok": False, "error": _why(e)})
            return self._send(200, {"ok": True, "png": base64.b64encode(png).decode("ascii"), "bytes": len(png)})
        if path == "/api/clearlog":
            log.lines.clear()
            return self._send(200, {"ok": True})
        return self._send(404, {"error": "not found"})


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 32


# ---------------------------------------------------------------------------------------------
#  Public entry points (called from Kotlin through Chaquopy)
# ---------------------------------------------------------------------------------------------
def start(files_dir, native=None):
    """Start everything once. Returns JSON {"port": n, "token": "..."}."""
    if G.get("server"):
        if native is not None:
            G["rt"].native = native
        return json.dumps({"port": G["port"], "token": G["token"]})
    os.makedirs(files_dir, exist_ok=True)
    bot.SESSION_FILE = os.path.join(files_dir, "session.json")
    bot.DATA_DIR = files_dir
    log = LogBuffer()
    sys.stdout = log
    sys.stderr = log
    bot.set_color(False)
    bot.set_clear(False)
    charts = AndroidChartEngine()
    bot.CHARTS = charts                       # every chart function of the engine uses this global
    bot.KeyWatcher = NullKeyWatcher           # no keyboard in the app
    store = Store(os.path.join(files_dir, "settings.json"))
    events = Events()
    rt = Runtime(store, log, events, charts)
    rt.native = native
    rt.start_loop()
    G.update(store=store, log=log, events=events, charts=charts, rt=rt, token=secrets.token_urlsafe(24))
    server = None
    for port in (PREFERRED_PORT, 0):
        try:
            server = _Server(("127.0.0.1", port), Handler)
            break
        except OSError:
            continue
    if server is None:
        raise RuntimeError("could not open a local port")
    G["server"], G["port"] = server, server.server_address[1]
    threading.Thread(target=server.serve_forever, name="dm-http", daemon=True).start()
    log.note(f"DARKMATTER app {APP_VERSION} (engine {bot.VERSION}) ready.")
    return json.dumps({"port": G["port"], "token": G["token"]})


def set_native(native):
    G["rt"].native = native


def on_frames(payload):
    """Frames from the Quotex WebView (JSON list of [kind, data])."""
    rt = G.get("rt")
    br = rt.bridge if rt is not None else None
    if br is not None:
        br.on_frames(payload)


def prepare_login():
    """The login screen is about to open: close the hidden Quotex page first (one open page per account)."""
    rt = G.get("rt")
    if rt and rt.loop:
        rt.submit(rt.disconnect())


def login_url():
    return bot.START_URL


def status_text():
    """Short text for the Android notification."""
    try:
        s = G["rt"].snapshot()
    except Exception:
        return "DARKMATTER"
    if s["engine"] == "running" and s["session"]:
        x = s["session"]
        return f"Running - {x['markets']} markets - W {x['wins']} / L {x['losses']}"
    return {"online": "Connected", "connecting": "Connecting ...", "reconnecting": "Reconnecting ...",
            "error": "Connection failed", "disconnected": "Not connected"}.get(s["link"], "DARKMATTER")


def shutdown():
    try:
        rt = G.get("rt")
        if rt and rt.loop:
            rt.call(rt.disconnect(), 30)
    except Exception:
        pass
