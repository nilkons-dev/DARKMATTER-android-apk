#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DARKMATTER  (darkmatter_bot.py)  -  Quotex data / signal / auto-trading bot in ONE file
=======================================================================================
Made to run on a phone in Termux (also works on Windows / Linux / macOS).

What it does
    * account balance, historical candles, live prices / live candles, market list
    * LIVE SIGNAL     multi-market next-1-minute-candle signal engine with MTG tracking + Telegram delivery
    * AUTO TRADING    places the signals as Quotex trades (TIME mode), MTG money management, stop loss / take profit
    * chart pictures for Telegram, drawn by Chromium (no matplotlib / numpy / pandas needed)

How it connects to Quotex
    1) BRIDGE mode (default)  - opens the Quotex sign-in page (https://market-qx.info/en/sign-in) in a real
       Chromium, you are logged in there and the bot reads the data from the trading WebSocket that the Quotex
       page itself has open.
       * headless (no window, the default in Termux): the bot prints a local address (http://127.0.0.1:8765/?k=..)
         - open it in your phone's Chrome, you see the Quotex page, tap the fields and type with the phone
         keyboard. An existing session.json is used automatically (no login needed)
       * with a window (Termux:X11, Windows, Linux desktop): you log in by hand in the Chromium window
    2) DIRECT mode (--direct) - connects to the WebSocket with a saved session (session.json, written by
       bridge mode) or an SSID given with --ssid. No browser needed.

First start in Termux (everything is installed for you when you answer "y"):
    python darkmatter_bot.py

Options
    python darkmatter_bot.py                       bridge mode (Chromium)
    python darkmatter_bot.py --headed              show the Chromium window (needs Termux:X11 / a display)
    python darkmatter_bot.py --headless            never show a window
    python darkmatter_bot.py --url URL             open another Quotex address first
    python darkmatter_bot.py --direct              direct mode with the saved session.json
    python darkmatter_bot.py --direct --ssid "YOUR_SSID" [--live]
    python darkmatter_bot.py --setup               (re)install Chromium + fonts in Termux and exit
    python darkmatter_bot.py --debug               print every WebSocket frame
    python darkmatter_bot.py --big-menu            screen titles in the big colour font
    python darkmatter_bot.py --no-color            plain output (also honours the NO_COLOR variable)

Environment variables (all optional)
    CHROME_PATH   path of the Chromium / Chrome binary      QX_URL       start page (default market-qx.info sign-in)
    QX_SESSION    path of the session.json to use

TIP: the full logo box needs a terminal that is at least 117 columns wide (landscape + zoom out on a phone).
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import concurrent.futures
import csv
import dataclasses
import hashlib
import html
import html as _html
import json
import math
import os
import platform
import re
import shutil
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid
import warnings
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from datetime import timedelta, timezone
from functools import lru_cache
from typing import Callable, Dict, List, Optional


# =====================================================================
#  Where things live
# =====================================================================
APP_DIR = os.path.dirname(os.path.abspath(__file__))


def _pick_data_dir():
    """Chromium profile + logs. Not on the phone's shared storage (/sdcard): Chromium cannot lock files there."""
    shared = ("/sdcard", "/storage/", "/mnt/sdcard", "/mnt/media_rw")
    ok = os.access(APP_DIR, os.W_OK) and not APP_DIR.startswith(shared) and "/storage/emulated" not in APP_DIR
    if ok:
        return APP_DIR
    d = os.path.join(os.path.expanduser("~"), ".darkmatter")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        return APP_DIR
    return d


DATA_DIR = _pick_data_dir()


def find_session_file():
    """The session.json to use: QX_SESSION, then the current folder, the folder of this file, the phone's
    download folders; a new one is written next to this file when none exists."""
    home = os.path.expanduser("~")
    for p in (os.environ.get("QX_SESSION"),
              os.path.join(os.getcwd(), "session.json"),
              os.path.join(APP_DIR, "session.json"),
              os.path.join(home, "downloads", "session.json"),
              os.path.join(home, "Downloads", "session.json"),
              os.path.join(home, "storage", "downloads", "session.json"),
              os.path.join(home, "storage", "shared", "Download", "session.json")):
        if p and os.path.isfile(p):
            return p
    return os.path.join(APP_DIR, "session.json")



# =====================================================================
#  module: darkmatter/config.py  -  Constants, app info, the shared EXTRA dict and the QuotexError exception.
# =====================================================================
APP_NAME = "DARKMATTER"
VERSION = "2.9.0"
BUILD = "2026-11-01"

START_URL = os.environ.get("QX_URL") or "https://market-qx.info/en/sign-in"   # what Chromium opens first (--url / QX_URL)
BASE = START_URL.split("/", 3)[0] + "//" + START_URL.split("/", 3)[2]
LANG = "en"
WS_URL_DEFAULT = "wss://ws2.qxbroker.com/socket.io/?EIO=3&transport=websocket"
SESSION_FILE = find_session_file()
PROFILE_DIR = os.path.join(DATA_DIR, "qx_chrome_profile")
DEBUG_PORT = 9222
HISTORY_CHUNK = 200   # candles per history/load request (same as pyquotex)
HISTORY_RETRIES = 1   # extra attempts when the first history request gets no reply

# Real ws url / user-agent / origin captured from the browser (used by direct mode)
EXTRA = {}


class QuotexError(Exception):
    pass


# =====================================================================
#  module: cdp.py
# =====================================================================
# =====================================================================
#  CHROMIUM LAYER  -  WebSocket client, Chrome DevTools Protocol, Chromium launcher
#  (standard library only: no playwright, no aiohttp, no curl_cffi needed)
# =====================================================================


BRIDGE_REF = {}           # the running Bridge (the chart engine reuses its browser)
LIVEVIEW = {}             # {'url': ...} of the local live view of the broker page (shown in the banner)


def is_termux():
    return bool(os.environ.get("TERMUX_VERSION")) or "com.termux" in os.environ.get("PREFIX", "")


class MiniWebSocket:
    """Minimal RFC 6455 client on asyncio streams: text + binary, fragments, ping / pong, close.

    recv() -> ("text", str) | ("binary", bytes) | ("close", b"")."""

    MAX_MESSAGE = 128 * 1024 * 1024

    def __init__(self):
        self.reader = None
        self.writer = None
        self.closed = False
        self.close_code = None
        self.close_reason = ""
        self._send_lock = None
        self.server = False               # True for a connection we accepted: our frames are not masked

    @classmethod
    async def accept(cls, reader, writer, key):
        """Server side of the handshake (used by the local login viewer)."""
        ws = cls()
        ws.reader, ws.writer, ws.server = reader, writer, True
        ws._send_lock = asyncio.Lock()
        acc = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest())
        writer.write(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                     b"Sec-WebSocket-Accept: " + acc + b"\r\n\r\n")
        await writer.drain()
        return ws

    async def connect(self, url, headers=None, timeout=15):
        u = urllib.parse.urlsplit(url)
        scheme = u.scheme.lower()
        if scheme in ("ws", "http"):
            secure = False
        elif scheme in ("wss", "https"):
            secure = True
        else:
            raise QuotexError(f"Unsupported WebSocket url: {url}")
        host = u.hostname
        if not host:
            raise QuotexError(f"Bad WebSocket url: {url}")
        port = u.port or (443 if secure else 80)
        path = (u.path or "/") + (("?" + u.query) if u.query else "")
        ctx = ssl.create_default_context() if secure else None
        self.reader, self.writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=ctx, server_hostname=host if secure else None,
                                    limit=2 ** 20),
            timeout,
        )
        self._send_lock = asyncio.Lock()
        key = base64.b64encode(os.urandom(16)).decode()
        default_port = 443 if secure else 80
        lines = [
            f"GET {path} HTTP/1.1",
            f"Host: {host}" + (f":{port}" if port != default_port else ""),
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key}",
            "Sec-WebSocket-Version: 13",
        ]
        have = {"host", "upgrade", "connection", "sec-websocket-key", "sec-websocket-version"}
        for k, v in (headers or {}).items():
            if k.lower() not in have:
                lines.append(f"{k}: {v}")
        try:
            self.writer.write(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1", "replace"))
            await self.writer.drain()
            head = await asyncio.wait_for(self.reader.readuntil(b"\r\n\r\n"), timeout)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, ConnectionError, OSError) as e:
            await self.close()
            raise QuotexError(f"WebSocket handshake failed: {type(e).__name__}: {e}") from None
        except asyncio.TimeoutError:
            await self.close()
            raise QuotexError("WebSocket handshake timed out") from None
        text = head.decode("latin-1", "replace")
        status = text.split("\r\n", 1)[0]
        hdrs = {}
        for ln in text.split("\r\n")[1:]:
            if ":" in ln:
                k, v = ln.split(":", 1)
                hdrs[k.strip().lower()] = v.strip()
        if " 101 " not in status + " ":
            await self.close()
            raise QuotexError(f"WebSocket handshake refused: {status.strip()}")
        want = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
        if hdrs.get("sec-websocket-accept") != want:
            await self.close()
            raise QuotexError("WebSocket handshake failed: bad Sec-WebSocket-Accept")

    @staticmethod
    def _mask(data, key):
        n = len(data)
        if not n:
            return b""
        k = (key * (n // 4 + 1))[:n]
        return (int.from_bytes(data, "big") ^ int.from_bytes(k, "big")).to_bytes(n, "big")

    async def _send_frame(self, opcode, payload):
        if self.closed or self.writer is None:
            raise ConnectionError("WebSocket is closed")
        n = len(payload)
        mbit = 0 if self.server else 0x80
        head = bytearray([0x80 | opcode])
        if n < 126:
            head.append(mbit | n)
        elif n < 65536:
            head.append(mbit | 126)
            head += struct.pack("!H", n)
        else:
            head.append(mbit | 127)
            head += struct.pack("!Q", n)
        if self.server:
            data = bytes(head) + payload
        else:
            key = os.urandom(4)
            head += key
            data = bytes(head) + self._mask(payload, key)
        async with self._send_lock:
            self.writer.write(data)
            await self.writer.drain()

    async def send(self, data):
        if isinstance(data, str):
            await self._send_frame(0x1, data.encode("utf-8"))
        else:
            await self._send_frame(0x2, bytes(data))

    async def _read_frame(self):
        b1, b2 = await self.reader.readexactly(2)
        fin, opcode = bool(b1 & 0x80), b1 & 0x0F
        masked, n = bool(b2 & 0x80), b2 & 0x7F
        if n == 126:
            n = struct.unpack("!H", await self.reader.readexactly(2))[0]
        elif n == 127:
            n = struct.unpack("!Q", await self.reader.readexactly(8))[0]
        if n > self.MAX_MESSAGE:
            raise ConnectionError("WebSocket frame too large")
        key = await self.reader.readexactly(4) if masked else None
        payload = await self.reader.readexactly(n) if n else b""
        if key:
            payload = self._mask(payload, key)
        return fin, opcode, payload

    async def recv(self):
        parts, first_op = [], None
        try:
            while True:
                fin, op, payload = await self._read_frame()
                if op == 0x8:  # close
                    if len(payload) >= 2:
                        self.close_code = struct.unpack("!H", payload[:2])[0]
                        self.close_reason = payload[2:].decode("utf-8", "replace")
                    try:
                        await self._send_frame(0x8, payload[:2])
                    except Exception:
                        pass
                    await self.close()
                    return "close", b""
                if op == 0x9:  # ping -> pong
                    try:
                        await self._send_frame(0xA, payload)
                    except Exception:
                        pass
                    continue
                if op == 0xA:  # pong
                    continue
                if op in (0x1, 0x2):
                    first_op, parts = op, [payload]
                elif op == 0x0 and first_op is not None:
                    parts.append(payload)
                else:
                    continue
                if fin:
                    data = b"".join(parts)
                    if first_op == 0x1:
                        return "text", data.decode("utf-8", "replace")
                    return "binary", data
        except (asyncio.IncompleteReadError, ConnectionError, OSError, ssl.SSLError) as e:
            self.close_reason = self.close_reason or f"{type(e).__name__}: {e}"
            await self.close()
            return "close", b""

    async def close(self):
        if self.closed and self.writer is None:
            return
        self.closed = True
        w, self.writer = self.writer, None
        if w is not None:
            try:
                w.close()
                await asyncio.wait_for(w.wait_closed(), 3)
            except Exception:
                pass


async def http_get(host, port, path, timeout=5.0):
    """Tiny HTTP/1.1 GET (asyncio). Returns (status, body bytes). Used for Chromium's /json endpoints."""
    reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
    try:
        writer.write(f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n".encode())
        await writer.drain()
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout)
        lines = head.decode("latin-1", "replace").split("\r\n")
        status = int(lines[0].split(" ", 2)[1]) if lines[0].startswith("HTTP/") else 0
        hdrs = {}
        for ln in lines[1:]:
            if ":" in ln:
                k, v = ln.split(":", 1)
                hdrs[k.strip().lower()] = v.strip()
        if "content-length" in hdrs:
            body = await asyncio.wait_for(reader.readexactly(int(hdrs["content-length"])), timeout)
        elif "chunked" in hdrs.get("transfer-encoding", "").lower():
            body = b""
            while True:
                size = int((await asyncio.wait_for(reader.readline(), timeout)).split(b";")[0].strip() or b"0", 16)
                if size == 0:
                    break
                body += await asyncio.wait_for(reader.readexactly(size), timeout)
                await asyncio.wait_for(reader.readline(), timeout)
        else:
            body = await asyncio.wait_for(reader.read(-1), timeout)
        return status, body
    finally:
        try:
            writer.close()
        except Exception:
            pass


class CdpError(Exception):
    pass


class Cdp:
    """Chrome DevTools Protocol connection (browser endpoint, flattened sessions)."""

    def __init__(self, ws_url):
        self.ws_url = ws_url
        self.ws = MiniWebSocket()
        self._id = 0
        self._pending = {}
        self._handlers = {}
        self._task = None
        self.closed = False
        self.close_hooks = []

    async def connect(self, timeout=15):
        await self.ws.connect(self.ws_url, timeout=timeout)
        self._task = asyncio.create_task(self._reader())

    def on(self, method, cb):
        self._handlers.setdefault(method, []).append(cb)

    async def call(self, method, params=None, session=None, timeout=30):
        if self.closed:
            raise CdpError("Chromium connection is closed")
        self._id += 1
        mid = self._id
        msg = {"id": mid, "method": method, "params": params or {}}
        if session:
            msg["sessionId"] = session
        fut = asyncio.get_running_loop().create_future()
        self._pending[mid] = fut
        try:
            await self.ws.send(json.dumps(msg))
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            raise CdpError(f"{method} timed out after {timeout}s") from None
        except (ConnectionError, OSError) as e:
            raise CdpError(f"{method}: connection lost ({e})") from None
        finally:
            self._pending.pop(mid, None)

    async def _reader(self):
        try:
            while True:
                kind, data = await self.ws.recv()
                if kind == "close":
                    break
                if kind != "text":
                    continue
                try:
                    msg = json.loads(data)
                except ValueError:
                    continue
                if "id" in msg:
                    fut = self._pending.get(msg["id"])
                    if fut is not None and not fut.done():
                        if "error" in msg:
                            e = msg["error"]
                            fut.set_exception(CdpError(f"{e.get('message', 'error')} ({e.get('code')})"))
                        else:
                            fut.set_result(msg.get("result") or {})
                    continue
                for cb in list(self._handlers.get(msg.get("method"), ())):
                    try:
                        cb(msg.get("params") or {}, msg.get("sessionId"))
                    except Exception:
                        pass
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        finally:
            self.closed = True
            for fut in list(self._pending.values()):
                if not fut.done():
                    fut.set_exception(CdpError("Chromium connection closed"))
            for hook in list(self.close_hooks):
                try:
                    hook()
                except Exception:
                    pass

    async def close(self):
        self.closed = True
        await self.ws.close()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except BaseException:
                pass


def find_chromium():
    """Path of a Chromium / Chrome / Edge binary, or None. CHROME_PATH (or CHROMIUM_PATH) wins."""
    for var in ("CHROME_PATH", "CHROMIUM_PATH", "QX_CHROMIUM"):
        p = os.environ.get(var)
        if p and os.path.exists(p):
            return p
    prefix = os.environ.get("PREFIX") or "/data/data/com.termux/files/usr"
    for name in ("chromium", "chromium-browser", "chromium-browser-stable", "google-chrome-stable",
                 "google-chrome", "chrome", "microsoft-edge", "microsoft-edge-stable", "msedge"):
        p = shutil.which(name)
        if p:
            return p
    candidates = [
        os.path.join(prefix, "bin", "chromium"),
        os.path.join(prefix, "bin", "chromium-browser"),
        os.path.join(prefix, "lib", "chromium", "chrome"),
        os.path.join(prefix, "lib", "chromium", "chromium"),
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium",
        "/snap/bin/chromium",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ]
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return None


def free_port(preferred=None):
    for p in ([preferred] if preferred else []) + [0]:
        s = socket.socket()
        try:
            s.bind(("127.0.0.1", p))
            return s.getsockname()[1]
        except OSError:
            continue
        finally:
            s.close()
    return 0


def has_display():
    if sys.platform in ("win32", "darwin"):
        return True
    return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))


class ChromiumProcess:
    """Starts a Chromium with remote debugging and waits until its DevTools endpoint answers."""

    def __init__(self, exe, port, profile, headless, url="about:blank", log_path=None):
        self.exe, self.port, self.profile = exe, port, profile
        self.headless, self.url = headless, url
        self.log_path = log_path or os.path.join(DATA_DIR, "qx_chromium.log")
        self.proc = None
        self.version = ""

    def argv(self, headless_flag="--headless=new"):
        a = [
            self.exe,
            f"--remote-debugging-port={self.port}",
            "--remote-debugging-address=127.0.0.1",
            "--remote-allow-origins=*",
            f"--user-data-dir={self.profile}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-timer-throttling",
            "--disable-renderer-backgrounding",
            "--disable-backgrounding-occluded-windows",
            "--disable-features=Translate,MediaRouter,OptimizationHints,InterestFeedContentSuggestions",
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--password-store=basic",
            "--mute-audio",
            "--lang=en-US",
        ]
        if sys.platform.startswith("linux") or is_termux():
            a.append("--disable-dev-shm-usage")
            if is_termux() or (hasattr(os, "geteuid") and os.geteuid() == 0):
                a.append("--no-sandbox")
        if is_termux():
            a += ["--disable-gpu", "--disable-software-rasterizer"]
        if self.headless:
            a += [headless_flag, "--hide-scrollbars", "--window-size=1366,768", "--disable-gpu"]
        a.append(self.url)
        return a

    def _log_tail(self, n=12):
        try:
            with open(self.log_path, encoding="utf-8", errors="replace") as f:
                lines = [ln.rstrip() for ln in f.readlines() if ln.strip()]
            return "\n".join(lines[-n:])
        except OSError:
            return ""

    async def start(self, timeout=45):
        os.makedirs(self.profile, exist_ok=True)
        last = None
        flags = ["--headless=new", "--headless"] if self.headless else [None]
        for flag in flags:
            try:
                log = open(self.log_path, "ab")
            except OSError:
                log = subprocess.DEVNULL
            try:
                kw = {}
                if os.name == "posix":
                    kw["start_new_session"] = True
                self.proc = subprocess.Popen(self.argv(flag or ""), stdin=subprocess.DEVNULL, stdout=log,
                                             stderr=log, **kw)
            except OSError as e:
                raise QuotexError(f"Could not start Chromium ({self.exe}): {e}") from None
            end = time.time() + timeout
            while time.time() < end:
                if self.proc.poll() is not None:
                    last = f"Chromium exited with code {self.proc.returncode}."
                    break
                try:
                    st, body = await http_get("127.0.0.1", self.port, "/json/version", 2.0)
                    if st == 200:
                        self.version = json.loads(body.decode("utf-8", "replace")).get("Browser", "")
                        return
                except Exception:
                    pass
                await asyncio.sleep(0.4)
            else:
                last = f"Chromium did not open its DevTools port {self.port} in {timeout}s."
            self.stop()
        tail = self._log_tail()
        raise QuotexError((last or "Chromium could not be started.") + (f"\nChromium log:\n{tail}" if tail else ""))

    def stop(self):
        p, self.proc = self.proc, None
        if p is None:
            return
        try:
            p.terminate()
            try:
                p.wait(timeout=4)
            except Exception:
                p.kill()
        except Exception:
            pass


async def browser_ws_url(port, timeout=3.0):
    """DevTools websocket url of a Chromium that listens on `port`, or None."""
    try:
        st, body = await http_get("127.0.0.1", port, "/json/version", timeout)
        if st == 200:
            return json.loads(body.decode("utf-8", "replace")).get("webSocketDebuggerUrl")
    except Exception:
        pass
    return None


class CdpPage:
    """A page target (flattened session). evaluate() works like the usual page.evaluate(fn, arg)."""

    def __init__(self, cdp, target_id, session):
        self.cdp, self.target_id, self.session = cdp, target_id, session

    async def evaluate(self, fn, arg=None, timeout=15):
        expr = f"({fn})({json.dumps(arg) if arg is not None else ''})"
        r = await self.cdp.call(
            "Runtime.evaluate",
            {"expression": expr, "returnByValue": True, "awaitPromise": True, "userGesture": True},
            self.session, timeout,
        )
        if "exceptionDetails" in r:
            d = r["exceptionDetails"]
            raise CdpError((d.get("exception") or {}).get("description") or d.get("text") or "script error")
        return (r.get("result") or {}).get("value")

    async def navigate(self, url):
        return await self.cdp.call("Page.navigate", {"url": url}, self.session)

    async def reload(self):
        return await self.cdp.call("Page.reload", {"ignoreCache": False}, self.session)


# ---------------------------------------------------------------------
#  Secret input (password prompt that does not echo)
# ---------------------------------------------------------------------
async def ainput_secret(prompt=""):
    import getpass

    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    def deliver(setter, value):
        if not fut.done():
            setter(value)

    def worker():
        try:
            text = getpass.getpass(strip_ansi(prompt))
        except BaseException as e:
            loop.call_soon_threadsafe(deliver, fut.set_exception, e)
        else:
            loop.call_soon_threadsafe(deliver, fut.set_result, text)

    threading.Thread(target=worker, daemon=True, name="ainput-secret").start()
    return (await fut).strip()


# =====================================================================
#  module: darkmatter/connection/bridge.py
# =====================================================================
# =====================================================================
#  Bridge mode: drives a real Chromium (Termux / Linux / Windows / macOS) over the DevTools protocol
# =====================================================================
_LOGIN_FILL_JS = """
(a) => {
  const el = document.querySelector(a.sel);
  if (!el) return false;
  const proto = el.tagName === 'TEXTAREA' ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
  const setter = Object.getOwnPropertyDescriptor(proto, 'value').set;
  el.focus();
  setter.call(el, a.val);
  el.dispatchEvent(new Event('input', {bubbles: true}));
  el.dispatchEvent(new Event('change', {bubbles: true}));
  return true;
}
"""

_LOGIN_SUBMIT_JS = """
() => {
  const forms = Array.from(document.querySelectorAll('form'));
  const visible = e => !!(e.offsetWidth || e.offsetHeight || e.getClientRects().length);
  const btns = Array.from(document.querySelectorAll('button[type="submit"], input[type="submit"]')).filter(visible);
  if (btns.length) { btns[0].click(); return true; }
  const f = forms.find(visible) || forms[0];
  if (f) { if (f.requestSubmit) f.requestSubmit(); else f.submit(); return true; }
  return false;
}
"""

_LOGIN_PROBE_JS = """
() => {
  const q = s => document.querySelector(s);
  const vis = e => !!e && !!(e.offsetWidth || e.offsetHeight || e.getClientRects().length);
  const err = q('.input-control-cabinet__hint, .hint--danger, [class*="error"], [class*="Error"]');
  return {
    email: vis(q('input[name="email"], input[type="email"]')),
    password: vis(q('input[name="password"], input[type="password"]')),
    pin: vis(q('input[name="code"], input[name="pin"], input[autocomplete="one-time-code"]')),
    err: err && err.textContent ? err.textContent.trim().slice(0, 160) : ''
  };
}
"""

_STATE_JS = """
() => ({
  href: location.href,
  origin: location.origin,
  path: location.pathname,
  ready: document.readyState,
  token: !!(window.settings && window.settings.token)
})
"""


_VIEWER_HTML = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>DARKMATTER login</title><style>
html,body{margin:0;height:100%;background:#0b0b17;color:#eef1f8;font-family:sans-serif}
#top{position:fixed;top:0;left:0;right:0;height:30px;line-height:30px;padding:0 8px;font-size:12px;background:#04050a;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;box-sizing:border-box}
#wrap{position:absolute;top:30px;bottom:104px;left:0;right:0;overflow:auto}
#scr{width:100%;display:block;touch-action:manipulation;background:#000;min-height:120px}
#bar{position:fixed;bottom:0;left:0;right:0;height:104px;background:#04050a;padding:6px;box-sizing:border-box}
#txt{width:100%;box-sizing:border-box;font-size:16px;padding:8px;border-radius:6px;border:1px solid #394058;background:#0a0c14;color:#eef1f8}
.row{display:flex;gap:5px;margin-top:6px}
button{flex:1;padding:9px 0;font-size:13px;border-radius:6px;border:1px solid #394058;background:#141826;color:#eef1f8}
</style></head><body>
<div id="top">connecting...</div>
<div id="wrap"><img id="scr" alt=""></div>
<div id="bar"><input id="txt" type="text" autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false"
 placeholder="tap a field above, type here, press Send">
<div class="row"><button id="bs">Send</button><button id="bt">Tab</button><button id="be">Enter</button>
<button id="bb">Del</button><button id="br">Reload</button><button id="bk">Back</button></div></div>
<script>
const K=new URLSearchParams(location.search).get('k'),img=document.getElementById('scr'),top_=document.getElementById('top'),txt=document.getElementById('txt');
let ws=null,t0=null;
function conn(){
  ws=new WebSocket('ws://'+location.host+'/ws?k='+K);ws.binaryType='blob';
  ws.onmessage=e=>{
    if(typeof e.data==='string'){const m=JSON.parse(e.data);
      if(m.t==='status')top_.textContent=(m.ok?'\u25cf live  ':'')+m.url;
    }else{const old=img.src,u=URL.createObjectURL(e.data);img.onload=()=>{if(old&&old.startsWith('blob:'))URL.revokeObjectURL(old)};img.src=u;}
  };
  ws.onclose=()=>{top_.textContent='disconnected...';setTimeout(conn,1000)};
}
function send(o){if(ws&&ws.readyState===1)ws.send(JSON.stringify(o));}
function tap(cx,cy){const r=img.getBoundingClientRect();send({t:'tap',x:(cx-r.left)/r.width,y:(cy-r.top)/r.height});}
img.addEventListener('touchstart',e=>{const t=e.touches[0];t0={x:t.clientX,y:t.clientY}},{passive:true});
img.addEventListener('touchend',e=>{
  if(!t0)return;const t=e.changedTouches[0],dx=t.clientX-t0.x,dy=t.clientY-t0.y,r=img.getBoundingClientRect();
  if(Math.abs(dy)>14||Math.abs(dx)>14)send({t:'scroll',dy:-dy/r.height});else tap(t.clientX,t.clientY);
  t0=null;e.preventDefault();
},{passive:false});
img.addEventListener('click',e=>{if(!('ontouchstart' in window))tap(e.clientX,e.clientY);});
function sendText(){const v=txt.value;if(v){send({t:'text',s:v});txt.value='';}}
document.getElementById('bs').onclick=sendText;
txt.addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();sendText();}});
document.getElementById('bt').onclick=()=>{sendText();send({t:'key',k:'Tab'})};
document.getElementById('be').onclick=()=>{sendText();send({t:'key',k:'Enter'})};
document.getElementById('bb').onclick=()=>send({t:'key',k:'Backspace'});
document.getElementById('br').onclick=()=>send({t:'nav',a:'reload'});
document.getElementById('bk').onclick=()=>send({t:'nav',a:'back'});
conn();
</script></body></html>"""

_KEYS = {"Enter": (13, "\r"), "Backspace": (8, ""), "Tab": (9, "")}


class LoginViewer:
    """Shows the headless Chromium page in the phone's own browser (http://127.0.0.1:PORT) so you can tap and
    type there with the phone keyboard. Local only, protected by a random key in the address."""

    VW, VH, SCALE = 460, 860, 1.5

    def __init__(self, bridge, port=8765):
        self.b = bridge
        self.cdp = bridge.cdp
        self.page = bridge.page
        self.port = port
        self.key = base64.urlsafe_b64encode(os.urandom(6)).decode().rstrip("=")
        self.server = None
        self.clients = set()
        self.last = None
        self._tasks = []
        self._casting = False

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}/?k={self.key}"

    async def start(self):
        try:
            self.server = await asyncio.start_server(self._conn, "127.0.0.1", self.port)
        except OSError:
            self.server = await asyncio.start_server(self._conn, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        s = self.page.session
        self.cdp.on("Page.screencastFrame", self._on_frame)
        await self.cdp.call("Emulation.setDeviceMetricsOverride",
                            {"width": self.VW, "height": self.VH, "deviceScaleFactor": self.SCALE, "mobile": False}, s)
        self._tasks.append(asyncio.create_task(self._status_loop()))

    async def _cast(self, on):
        """The page is only streamed while somebody is looking (saves battery)."""
        if on == self._casting:
            return
        self._casting = on
        try:
            if on:
                await self.cdp.call("Page.startScreencast",
                                    {"format": "jpeg", "quality": 55, "maxWidth": int(self.VW * self.SCALE),
                                     "maxHeight": int(self.VH * self.SCALE), "everyNthFrame": 1}, self.page.session)
            else:
                await self.cdp.call("Page.stopScreencast", None, self.page.session, 5)
        except CdpError:
            self._casting = not on

    def _on_frame(self, p, sid):
        if sid != self.page.session:
            return
        self._tasks.append(asyncio.ensure_future(self._ack(p.get("sessionId"))))
        try:
            self.last = base64.b64decode(p.get("data") or "")
        except Exception:
            return
        for ws in list(self.clients):
            if ws.writer is not None and ws.writer.transport.get_write_buffer_size() < 400_000:
                self._tasks.append(asyncio.ensure_future(self._safe_send(ws, self.last)))
        self._tasks = [t for t in self._tasks if not t.done()]

    async def _ack(self, sid):
        try:
            await self.cdp.call("Page.screencastFrameAck", {"sessionId": sid}, self.page.session, 5)
        except Exception:
            pass

    @staticmethod
    async def _safe_send(ws, data):
        try:
            await ws.send(data)
        except Exception:
            pass

    async def _status_loop(self):
        while True:
            if self.clients:
                st = await self.b._state()
                if st:
                    msg = json.dumps({"t": "status", "url": st.get("href", "")[:90],
                                      "ok": bool(st.get("token"))})
                    for ws in list(self.clients):
                        await self._safe_send(ws, msg)
            await asyncio.sleep(1.0)

    async def _conn(self, reader, writer):
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
            lines = head.decode("latin-1", "replace").split("\r\n")
            parts = lines[0].split(" ")
            hdrs = {k.strip().lower(): v.strip() for k, v in (ln.split(":", 1) for ln in lines[1:] if ":" in ln)}
            u = urllib.parse.urlsplit(parts[1] if len(parts) > 1 else "/")
            q = urllib.parse.parse_qs(u.query)
            if q.get("k", [""])[0] != self.key:
                writer.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            elif u.path == "/ws" and hdrs.get("upgrade", "").lower() == "websocket":
                ws = await MiniWebSocket.accept(reader, writer, hdrs.get("sec-websocket-key", ""))
                self.clients.add(ws)
                try:
                    await self._cast(True)
                    if self.last:
                        await ws.send(self.last)
                    await self._serve(ws)
                finally:
                    self.clients.discard(ws)
                    await ws.close()
                    if not self.clients:
                        await self._cast(False)
                return
            elif u.path == "/":
                body = _VIEWER_HTML.encode("utf-8")
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-store\r\n"
                             b"Connection: close\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
            else:
                writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            await writer.drain()
        except Exception:
            pass
        try:
            writer.close()
        except Exception:
            pass

    async def _serve(self, ws):
        while True:
            kind, data = await ws.recv()
            if kind == "close":
                return
            if kind != "text":
                continue
            try:
                await self._input(json.loads(data))
            except (CdpError, ValueError, TypeError, KeyError):
                pass

    async def _input(self, m):
        c, s, t = self.cdp, self.page.session, m.get("t")
        if t == "tap":
            x = max(0.0, min(1.0, float(m["x"]))) * self.VW
            y = max(0.0, min(1.0, float(m["y"]))) * self.VH
            base = {"x": x, "y": y, "button": "left", "pointerType": "mouse"}
            await c.call("Input.dispatchMouseEvent", dict(base, type="mouseMoved"), s)
            await c.call("Input.dispatchMouseEvent", dict(base, type="mousePressed", buttons=1, clickCount=1), s)
            await c.call("Input.dispatchMouseEvent", dict(base, type="mouseReleased", buttons=0, clickCount=1), s)
        elif t == "scroll":
            await c.call("Input.dispatchMouseEvent", {"type": "mouseWheel", "x": self.VW / 2, "y": self.VH / 2,
                                                      "deltaX": 0, "deltaY": float(m["dy"]) * self.VH * 1.2}, s)
        elif t == "text":
            await c.call("Input.insertText", {"text": str(m["s"])[:200]}, s)
        elif t == "key" and m.get("k") in _KEYS:
            vk, txt = _KEYS[m["k"]]
            ev = {"key": m["k"], "code": m["k"], "windowsVirtualKeyCode": vk, "nativeVirtualKeyCode": vk}
            await c.call("Input.dispatchKeyEvent", dict(ev, type="keyDown", **({"text": txt} if txt else {})), s)
            await c.call("Input.dispatchKeyEvent", dict(ev, type="keyUp"), s)
        elif t == "nav":
            if m.get("a") == "reload":
                await self.page.reload()
            elif m.get("a") == "back":
                await self.page.evaluate("() => history.back()")

    async def stop(self):
        for t in self._tasks:
            t.cancel()
        if self._casting:
            await self._cast(False)
        for ws in list(self.clients):
            await ws.close()
        if self.server is not None:
            self.server.close()
        self.clients.clear()


def open_in_phone_browser(url):
    """Termux: ask Android to open the address in the default browser (Chrome)."""
    if not is_termux():
        return False
    for cmd in (["termux-open-url", url], ["am", "start", "-a", "android.intent.action.VIEW", "-d", url]):
        if shutil.which(cmd[0]):
            try:
                subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                return True
            except OSError:
                continue
    return False



def _cookie_domain_candidates(ws_url, origin):
    """Hosts the saved cookies are meant for: the websocket host's parent domain and the page origin host."""
    out = []
    for u in (ws_url, origin, BASE):
        try:
            host = urllib.parse.urlsplit(u or "").hostname
        except ValueError:
            host = None
        if not host:
            continue
        parts = host.split(".")
        is_ip = all(p.isdigit() for p in parts) or ":" in host
        dom = "." + ".".join(parts[-2:]) if (len(parts) >= 2 and not is_ip) else host
        if dom not in out:
            out.append(dom)
    return out


class Bridge:
    """Quotex through a real Chromium: it logs in, the bot reads the page's own trading socket."""

    def __init__(self, debug=False, headless=None):
        self.debug = debug
        self.headless = headless          # None = automatic (headless when there is no display)
        self.cdp = None
        self.proc = None
        self.page = None
        self.ws = None
        self.found = {}
        self.owns_browser = False
        self._queue = None
        self._cur_ws = None               # request id of the newest trading socket
        self._ws_urls = {}
        self._closed = False
        self.viewer = None

    # ---------------- helpers ----------------
    async def _state(self):
        try:
            return await self.page.evaluate(_STATE_JS, None, timeout=8)
        except Exception:
            return None

    def _on_ws_created(self, p, sid):
        if sid != self.page.session:
            return
        url = p.get("url") or ""
        self._ws_urls[p.get("requestId")] = url
        if "socket.io" in url:
            self._cur_ws = p.get("requestId")
            self.found["ws_url"] = url

    def _on_ws_sent(self, p, sid):
        if sid != self.page.session or "socket.io" not in self._ws_urls.get(p.get("requestId"), ""):
            return
        r = p.get("response") or {}
        data = r.get("payloadData") or ""
        if r.get("opcode") == 2:
            try:
                data = base64.b64decode(data).decode("utf-8", "ignore")
            except Exception:
                return
        m = re.search(r'"session"\s*:\s*"([^"]{16,})"', data)
        if m:
            self.found["ssid"] = m.group(1)
        if self.ws:
            self.ws.note_sent(data)

    def _on_ws_received(self, p, sid):
        if sid != self.page.session or p.get("requestId") != self._cur_ws:
            return
        r = p.get("response") or {}
        data = r.get("payloadData")
        if data is None:
            return
        if r.get("opcode") == 2:
            try:
                self._queue.put_nowait(("binary", base64.b64decode(data)))
            except Exception:
                pass
        else:
            self._queue.put_nowait(("text", data))

    def _on_ws_closed(self, p, sid):
        if sid == self.page.session and p.get("requestId") == self._cur_ws:
            self._cur_ws = None

    def _on_cdp_closed(self):
        if self._queue is not None and not self._closed:
            self._queue.put_nowait(("close", b""))

    # ---------------- start ----------------
    async def _open_browser(self):
        port = DEBUG_PORT
        ws_url = await browser_ws_url(port)
        if ws_url:                                   # reuse a Chromium that is already running with remote debugging
            self.owns_browser = False
            if self.headless is None:
                self.headless = False
            return ws_url
        exe = find_chromium()
        if not exe:
            raise QuotexError(
                "Chromium was not found.\n"
                + ("In Termux run:  pkg install -y x11-repo tur-repo && pkg install -y chromium\n"
                   "(or start this file again and answer yes to the setup question)."
                   if is_termux() else
                   "Install Chromium / Chrome, or set the CHROME_PATH environment variable.")
            )
        if self.headless is None:
            self.headless = not has_display()
        self.proc = ChromiumProcess(exe, port, os.path.abspath(PROFILE_DIR), self.headless, START_URL)
        async with Progress("Starting Chromium" + (" (headless)" if self.headless else "")):
            await self.proc.start()
        self.owns_browser = True
        ws_url = await browser_ws_url(port, 5.0)
        if not ws_url:
            raise QuotexError("Chromium started, but its DevTools endpoint did not answer.")
        return ws_url

    async def _pick_page(self):
        info_ = await self.cdp.call("Target.getTargets")
        pages = [t for t in info_.get("targetInfos", [])
                 if t.get("type") == "page" and not str(t.get("url", "")).startswith(("devtools://", "chrome-extension://"))]
        pages.sort(key=lambda t: 0 if "quotex" in t.get("url", "") or "qxbroker" in t.get("url", "") else 1)
        if pages:
            tid = pages[0]["targetId"]
        else:
            tid = (await self.cdp.call("Target.createTarget", {"url": START_URL}))["targetId"]
        sid = (await self.cdp.call("Target.attachToTarget", {"targetId": tid, "flatten": True}))["sessionId"]
        return CdpPage(self.cdp, tid, sid)

    async def _prepare_page(self):
        c, s = self.cdp, self.page.session
        await c.call("Page.enable", None, s)
        await c.call("Network.enable", None, s)
        await c.call("Page.addScriptToEvaluateOnNewDocument", {"source": HOOK_JS}, s)
        if self.headless:
            try:
                ua = await self.page.evaluate("() => navigator.userAgent")
                await c.call("Emulation.setUserAgentOverride",
                             {"userAgent": str(ua).replace("HeadlessChrome", "Chrome"),
                              "acceptLanguage": "en-US,en;q=0.9"}, s)
                await c.call("Emulation.setDeviceMetricsOverride",
                             {"width": 1366, "height": 768, "deviceScaleFactor": 1, "mobile": False}, s)
            except CdpError:
                pass
        c.on("Network.webSocketCreated", self._on_ws_created)
        c.on("Network.webSocketFrameSent", self._on_ws_sent)
        c.on("Network.webSocketFrameReceived", self._on_ws_received)
        c.on("Network.webSocketClosed", self._on_ws_closed)
        c.close_hooks.append(self._on_cdp_closed)

    async def start(self, login_timeout=300):
        self._queue = asyncio.Queue()
        ws_url = await self._open_browser()
        self.cdp = Cdp(ws_url)
        try:
            await self.cdp.connect()
        except QuotexError as e:
            raise QuotexError(f"Could not connect to Chromium: {e}") from None
        BRIDGE_REF["bridge"] = self
        self.page = await self._pick_page()
        await self._prepare_page()
        self.ws = QuotexWS(None, {}, debug=self.debug, passive=True)

        put()
        if self.headless:
            try:
                self.viewer = LoginViewer(self)
                await self.viewer.start()
            except Exception as e:
                self.viewer = None
                warn(f"Could not start the login viewer ({e}); the login will be typed in this terminal.")
        if self.viewer:
            info("Chromium runs headless. To log in, open this address in your phone's Chrome:")
            put(f"      {self.viewer.url}")
            if open_in_phone_browser(self.viewer.url):
                info("(Android was asked to open it - if Chrome did not appear, open the address by hand.)")
            info("Tap the e-mail / password field, type in the box at the bottom, press Send, then log in.")
            info("The same address shows the live broker page any time - closing Chrome never stops the bot.")
            LIVEVIEW["url"] = self.viewer.url
        elif self.headless:
            info("Chromium runs headless (no window). If a login is needed it happens here in the terminal.")
        else:
            info("In the Chromium window: log in if needed (captcha / PIN are yours to solve).")
        await self._wait_for_trade_page(login_timeout)

        self.ws.attach(BridgeTransport(self.page, self._queue))
        good("Logged in. Reloading the page once to hook its trading socket.")
        async with Progress("Hooking the trading socket"):
            await self.page.reload()
            try:
                await asyncio.wait_for(self.ws._auth.wait(), 45)
            except asyncio.TimeoutError:
                raise QuotexError(
                    "No data arrived from the page's trading socket. Frames seen: "
                    + (" | ".join(f"{k}:{v}" for k, v in self.ws.frames[:6]) or "none")
                ) from None
        await self._save_session()
        return self.ws

    async def _wait_for_trade_page(self, login_timeout):
        end = time.time() + login_timeout
        c = {"went": False, "t_nav": 0.0, "seeded": False, "login_done": self.viewer is not None, "fallback": False}
        while True:
            async with Progress("Waiting for your login in Chrome" if self.viewer else "Waiting for the trading page + login") as bar:
                verdict = await self._poll_trade_page(end, c, bar)
            if verdict == "trade":
                return
            await self._terminal_login(end)         # headless only: the prompts own the screen, no progress bar

    async def _poll_trade_page(self, end, c, bar=None):
        """Poll until the logged-in trading page is open ('trade'), or - headless only - until the
        sign-in page needs the terminal login ('login')."""
        while time.time() < end:
            st = await self._state()
            if st:
                href = st.get("href") or ""
                path = (st.get("path") or "").lower()
                ready = st.get("ready")
                on_trade = "/trade" in path
                on_login = any(k in path for k in ("sign-in", "signin", "sign_in", "login"))
                origin = st.get("origin") or ""
                if not origin.startswith("http"):
                    origin = BASE
                if bar is not None:
                    where = urllib.parse.urlsplit(href)
                    if not self.viewer:
                        bar.label = ("Waiting " + (where.netloc + where.path)[:34]) if where.netloc else "Waiting for the page"
                if on_trade and st.get("token"):
                    return "trade"
                if not on_trade and not on_login and self.headless and ready in ("interactive", "complete"):
                    pr = await self._probe()          # a login form on any other address counts as the login page
                    if pr and pr.get("email") and pr.get("password"):
                        on_login = True
                if on_login:
                    if ready in ("interactive", "complete"):
                        if not c["seeded"]:
                            c["seeded"] = True
                            if await self._seed_cookies(st):
                                c["went"], c["t_nav"] = True, time.time()
                                await self.page.navigate(f"{origin}/{LANG}/trade")
                                await asyncio.sleep(1.0)
                                continue
                        if self.headless and not c["login_done"]:
                            c["login_done"] = True
                            return "login"
                elif not on_trade:
                    if ready == "complete" or href.startswith(("chrome-error:", "about:")):
                        if not c["went"]:
                            c["went"], c["t_nav"] = True, time.time()
                            await self.page.navigate(f"{origin}/{LANG}/trade")
                        elif time.time() - c["t_nav"] > 25 and not c["fallback"]:
                            c["fallback"], c["t_nav"] = True, time.time()   # that domain has no trade page
                            await self.page.navigate(f"{BASE}/{LANG}/trade")
            await asyncio.sleep(0.5)
        raise QuotexError("The trading page was not reached in time.")

    async def _seed_cookies(self, st):
        """Put the cookies of a saved session.json into Chromium (only used when the page asks for a login)."""
        ssid, cookies = load_session()
        if not cookies:
            return False
        origin = st.get("origin") or BASE
        host = urllib.parse.urlsplit(origin).hostname or ""
        secure = origin.startswith("https")
        params = []
        for dom in _cookie_domain_candidates(EXTRA.get("ws_url"), origin):
            for k, v in cookies.items():
                params.append({"name": k, "value": str(v), "domain": dom, "path": "/", "secure": secure})
        if host:
            for k, v in cookies.items():
                params.append({"name": k, "value": str(v), "domain": host, "path": "/", "secure": secure})
        try:
            await self.cdp.call("Network.setCookies", {"cookies": params}, self.page.session)
            return True
        except CdpError:
            return False

    async def _terminal_login(self, end):
        """Headless login: e-mail / password / PIN are typed here, the bot fills the Quotex form."""
        for _ in range(40):                                   # wait for the form
            pr = await self._probe()
            if pr and pr.get("email") and pr.get("password"):
                break
            await asyncio.sleep(0.5)
        else:
            raise QuotexError(
                "The Quotex login form did not appear (Cloudflare check?). Use a visible browser instead: "
                "install termux-x11 and run again with --headed, or copy a session.json next to this file."
            )
        put()
        say("Quotex login (typed here, the password is not shown on screen).")
        email = await ainput(ask_prompt("E-mail"))
        password = await ainput_secret(ask_prompt("Password"))
        if not email or not password:
            raise QuotexError("Login cancelled (e-mail or password was empty).")
        ok1 = await self.page.evaluate(_LOGIN_FILL_JS, {"sel": 'input[name="email"], input[type="email"]', "val": email})
        ok2 = await self.page.evaluate(_LOGIN_FILL_JS, {"sel": 'input[name="password"], input[type="password"]', "val": password})
        if not (ok1 and ok2):
            raise QuotexError("Could not fill the Quotex login form (the page layout changed).")
        await asyncio.sleep(0.3)
        await self.page.evaluate(_LOGIN_SUBMIT_JS)
        asked_pin = False
        t0 = time.time()
        while time.time() < end:
            await asyncio.sleep(0.7)
            st2 = await self._state()
            if st2 and st2.get("token") and "/trade" in (st2.get("path") or "").lower():
                return
            pr = await self._probe()
            if not pr:
                continue
            if pr.get("pin") and not asked_pin:
                asked_pin = True
                put()
                say("Quotex sent a PIN code (check your e-mail).")
                pin = await ainput(ask_prompt("PIN"))
                if pin:
                    await self.page.evaluate(_LOGIN_FILL_JS, {"sel": 'input[name="code"], input[name="pin"], input[autocomplete="one-time-code"]', "val": pin})
                    await asyncio.sleep(0.3)
                    await self.page.evaluate(_LOGIN_SUBMIT_JS)
                    t0 = time.time()
            elif pr.get("err") and time.time() - t0 > 4:
                raise QuotexError(f"Quotex rejected the login: {pr['err']}")
            elif time.time() - t0 > 90 and not asked_pin:
                raise QuotexError("The login did not finish. Check e-mail / password (or a captcha is asked: use --headed).")
        raise QuotexError("The login was not completed in time.")

    async def _probe(self):
        try:
            return await self.page.evaluate(_LOGIN_PROBE_JS, None, timeout=8)
        except Exception:
            return None

    async def _save_session(self):
        """Keep SSID / cookies / real ws url so --direct mode can try later."""
        ssid = self.found.get("ssid")
        if not ssid:
            try:
                ssid = await self.page.evaluate("() => window.settings && window.settings.token")
            except Exception:
                ssid = None
        if not ssid:
            return
        try:
            ws_url = self.found.get("ws_url")
            if ws_url:
                EXTRA["ws_url"] = ws_url
            EXTRA["origin"] = await self.page.evaluate("() => location.origin")
            EXTRA["ua"] = await self.page.evaluate("() => navigator.userAgent")
            if self.headless:
                EXTRA["ua"] = str(EXTRA["ua"]).replace("HeadlessChrome", "Chrome")
            url = (ws_url or WS_URL_DEFAULT).replace("wss://", "https://", 1)
            r = await self.cdp.call("Network.getCookies", {"urls": [url]}, self.page.session)
            cookies = {c["name"]: c["value"] for c in r.get("cookies", [])}
            save_session(ssid, cookies)
        except Exception:
            pass

    async def close(self):
        self._closed = True
        if self.viewer is not None:
            try:
                await self.viewer.stop()
            except Exception:
                pass
        BRIDGE_REF.pop("bridge", None)
        try:
            if self.ws:
                await self.ws.close()
        except Exception:
            pass
        try:
            await CHARTS.aclose()
        except Exception:
            pass
        if self.cdp is not None:
            if self.owns_browser:
                try:
                    await asyncio.wait_for(self.cdp.call("Browser.close"), 3)
                except Exception:
                    pass
            try:
                await self.cdp.close()
            except Exception:
                pass
        if self.proc:
            self.proc.stop()


# =====================================================================
#  module: darkmatter/connection/candle_data.py  -  Parsing helpers: balance, ticks, candle builder, raw candle rows, time formatting.
# =====================================================================
def find_balance(obj, depth=0):
    """Search nested JSON for demoBalance / liveBalance (bounded, so big tick frames stay cheap)."""
    if depth > 5:
        return None
    if isinstance(obj, dict):
        if "demoBalance" in obj or "liveBalance" in obj:
            return {"demo": obj.get("demoBalance"), "live": obj.get("liveBalance")}
        for v in obj.values():
            if isinstance(v, (dict, list)):
                r = find_balance(v, depth + 1)
                if r:
                    return r
    elif isinstance(obj, list):
        if obj and isinstance(obj[0], str):  # [asset, ts, price] tick row
            return None
        for v in obj[:50]:
            if isinstance(v, (dict, list)):
                r = find_balance(v, depth + 1)
                if r:
                    return r
    return None


def iter_ticks(data):
    """Find [asset, timestamp, price] ticks (also inside nested lists)."""
    if isinstance(data, list):
        if (
            len(data) >= 3
            and isinstance(data[0], str)
            and isinstance(data[1], (int, float))
            and isinstance(data[2], (int, float))
        ):
            yield data[0], float(data[1]), float(data[2])
        else:
            for item in data:
                yield from iter_ticks(item)


class CandleBuilder:
    """Builds OHLC candles from ticks."""

    def __init__(self, period=60):
        self.period = period
        self.cur = {}

    def add(self, asset, ts, price, on_close=None):
        bucket = int(ts // self.period * self.period)
        c = self.cur.get(asset)
        if c is None or c["t"] != bucket:
            if c and on_close:
                on_close(asset, c)
            self.cur[asset] = {"t": bucket, "o": price, "h": price, "l": price, "c": price}
        else:
            c["h"] = max(c["h"], price)
            c["l"] = min(c["l"], price)
            c["c"] = price
        return self.cur[asset]


def candles_from_raw(raw):
    """
    Parse raw candle rows. Column layout is not guaranteed, so:
      * column 0 = time, column 1 = open
      * high/low = max/min of the four price columns (always right)
      * close = whichever of column 2 / column 4 lines up with the NEXT candle's open
    """
    rows = []
    for c in raw or []:
        try:
            if isinstance(c, (list, tuple)) and len(c) >= 5:
                rows.append((int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4])))
            elif isinstance(c, dict):
                rows.append((int(c["time"]), float(c["open"]), float(c["close"]),
                             float(c["high"]), float(c["low"])))
        except (TypeError, ValueError, KeyError):
            continue
    rows.sort()
    close_idx = 2
    if len(rows) >= 3:
        err_a = sum(abs(rows[i][2] - rows[i + 1][1]) for i in range(len(rows) - 1))
        err_b = sum(abs(rows[i][4] - rows[i + 1][1]) for i in range(len(rows) - 1))
        close_idx = 2 if err_a <= err_b else 4
    out = []
    for r in rows:
        prices = r[1:5]
        out.append({"t": r[0], "o": r[1], "c": r[close_idx], "h": max(prices), "l": min(prices)})
    return out


def parse_candle_rows(raw):
    """
    Quotex history/load returns rows as dicts:
        {"time", "open", "close", "high", "low", "ticks", ...}
    Older/other layouts (plain lists) fall back to candles_from_raw().
    """
    dict_rows, list_rows = [], []
    for c in raw or []:
        if isinstance(c, dict) and "time" in c:
            try:
                dict_rows.append({
                    "t": int(c["time"]), "o": float(c["open"]), "h": float(c["high"]),
                    "l": float(c["low"]), "c": float(c["close"]), "ticks": c.get("ticks"),
                })
            except (KeyError, TypeError, ValueError):
                pass
        elif isinstance(c, (list, tuple)):
            list_rows.append(c)
    return dict_rows + (candles_from_raw(list_rows) if list_rows else [])


def mask_secrets(text):
    return re.sub(r'("session"\s*:\s*")[^"]+', r"\1***", text)


def fmt_time(ts):
    return time.strftime("%H:%M:%S", time.localtime(ts))


def fmt_dt(ts):
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))


# ---------------------------------------------------------------------


# =====================================================================
#  module: darkmatter/connection/transports.py  -  WebSocket transports: Curl, Aio and Bridge (browser) transports.
# =====================================================================
try:
    import aiohttp
except ImportError:  # only needed for the direct-mode fallback transport
    aiohttp = None



# =====================================================================
#  Transports
# =====================================================================
class CurlTransport:
    """Direct mode: WebSocket with Chrome's TLS fingerprint (curl_cffi)."""

    last_error = None

    async def connect(self, url, headers):
        from curl_cffi.requests import AsyncSession

        self.s = AsyncSession(impersonate="chrome")
        self.ws = await self.s.ws_connect(url, headers=headers)

    async def send_str(self, text):
        await self.ws.send_str(text)

    async def recv(self):
        from curl_cffi.const import CurlWsFlag

        try:
            data, flags = await self.ws.recv()
        except Exception as e:
            self.last_error = f"{type(e).__name__}: {e}"
            return "close", b""
        if flags & CurlWsFlag.CLOSE:
            self.last_error = (
                f"server closed the connection (code={getattr(self.ws, 'close_code', None)}, "
                f"reason={getattr(self.ws, 'close_reason', None)})"
            )
            return "close", b""
        if flags & (CurlWsFlag.PING | CurlWsFlag.PONG):
            return "skip", b""
        if flags & CurlWsFlag.BINARY:
            return "binary", data
        return "text", data.decode("utf-8", "ignore")

    async def close(self):
        for obj in (getattr(self, "ws", None), getattr(self, "s", None)):
            try:
                if obj:
                    await obj.close()
            except Exception:
                pass


class AioTransport:
    """Direct mode fallback: plain aiohttp WebSocket."""

    last_error = None

    async def connect(self, url, headers):
        if aiohttp is None:
            raise RuntimeError("aiohttp is not installed")
        self.http = aiohttp.ClientSession()
        try:
            self.ws = await self.http.ws_connect(url, headers=headers)
        except Exception:
            await self.http.close()
            raise

    async def send_str(self, text):
        await self.ws.send_str(text)

    async def recv(self):
        msg = await self.ws.receive()
        if msg.type == aiohttp.WSMsgType.TEXT:
            return "text", msg.data
        if msg.type == aiohttp.WSMsgType.BINARY:
            return "binary", msg.data
        self.last_error = f"server closed the connection (type={msg.type}, data={msg.data})"
        return "close", b""

    async def close(self):
        try:
            await self.ws.close()
            await self.http.close()
        except Exception:
            pass


class RawTransport:
    """Direct mode without any extra library: a plain TLS WebSocket written with the standard library."""

    last_error = None

    async def connect(self, url, headers):
        self.ws = MiniWebSocket()
        await self.ws.connect(url, headers)

    async def send_str(self, text):
        await self.ws.send(text)

    async def recv(self):
        kind, data = await self.ws.recv()
        if kind == "close":
            self.last_error = (f"server closed the connection (code={self.ws.close_code}, "
                               f"reason={self.ws.close_reason})")
            return "close", b""
        return kind, data

    async def close(self):
        await self.ws.close()


# JavaScript injected into the Quotex page: remembers every WebSocket the page opens,
# so the bot can send messages through the page's own (already authorized) socket.
HOOK_JS = """
(() => {
  if (window.__qxHooked) return;
  window.__qxHooked = true;
  const Orig = window.WebSocket;
  window.__qxSockets = [];
  function Hooked(url, protocols) {
    const ws = protocols === undefined ? new Orig(url) : new Orig(url, protocols);
    window.__qxSockets.push(ws);
    return ws;
  }
  Hooked.prototype = Orig.prototype;
  Hooked.CONNECTING = Orig.CONNECTING; Hooked.OPEN = Orig.OPEN;
  Hooked.CLOSING = Orig.CLOSING; Hooked.CLOSED = Orig.CLOSED;
  window.WebSocket = Hooked;
})();
"""

SEND_JS = """
(msg) => {
  const list = (window.__qxSockets || []).filter(
    s => s.readyState === 1 && String(s.url).includes('socket.io'));
  const s = list[list.length - 1];
  if (!s) return false;
  s.send(msg);
  return true;
}
"""


class BridgeTransport:
    """Bridge mode: frames come from the browser's socket, sends go through the page."""

    last_error = None

    def __init__(self, page, queue):
        self.page = page
        self.q = queue

    async def connect(self, *a):
        pass

    async def send_str(self, text):
        ok = await self.page.evaluate(SEND_JS, text)
        if not ok:
            raise QuotexError("No open Quotex WebSocket found in the browser page.")

    async def recv(self):
        return await self.q.get()

    async def close(self):
        pass


# =====================================================================
#  module: darkmatter/ui/theme.py  -  DARKMATTER theme: logo banner lines and the big colour font renderer.
# =====================================================================
# =====================================================================
#  DARKMATTER theme: banner + big colour font (same style as the logo)
# =====================================================================
RESET = "\033[0m"
_SGR = re.compile(r"\x1b\[[0-9;]*m")
C_EDGE_L, C_EDGE, C_SHADOW = "\033[0;37m", "\033[0;97m", "\033[0;90m"
# fill style per pixel row (colour, shade): exactly the shading used by the logo
FILL_STYLES = [
    ("\033[0;94;47m", "\u2593"), ("\033[0;94;47m", "\u2591"), ("\033[0;90;47m", "\u2593"),
    ("\033[0;37m", "\u2591"), ("\033[0;90m", "\u2592"),
]

# The logo, exactly as supplied (7 rows, 112 columns wide).
BANNER_LINES = [
    '\x1b[0;37m┌\x1b[0;97m─────┐\x1b[0;90m┐\x1b[0;37m     \x1b[0;97m┌───┐\x1b[0;90m┐\x1b[0;37m   ┌\x1b[0;97m─────┐\x1b[0;90m┐\x1b[0;37m   ┌\x1b[0;97m──┐\x1b[0;90m┐\x1b[0;97m┌──┐\x1b[0;90m┐\x1b[0;37m ┌\x1b[0;97m─────┐\x1b[0;90m┐\x1b[0;97m──┐\x1b[0;90m┐\x1b[0;37m     \x1b[0;97m┌───┐\x1b[0;90m┐\x1b[0;37m   \x1b[0;97m┌───────┐\x1b[0;90m┐\x1b[0;37m \x1b[0;97m┌───────┐\x1b[0;90m┐\x1b[0;37m   \x1b[0;97m┌────┐\x1b[0;90m┐\x1b[0;37m ┌\x1b[0;97m─────┐\x1b[0;90m┐\x1b[0;37m  \x1b[0m',
    '\x1b[0;37m│\x1b[0;94;47m▓▓▓▓▓\x1b[0;97m└─┐\x1b[0;90m┐\x1b[0;37m ┌\x1b[0;97m─┘\x1b[0;94;47m▓▓▓\x1b[0;97m└─┐\x1b[0;90m┐\x1b[0;37m │\x1b[0;94;47m▓▓▓▓▓\x1b[0;97m└─┐\x1b[0;90m┐\x1b[0;37m │\x1b[0;94;47m▓▓\x1b[0;97m│\x1b[0;90m│\x1b[0;37m│\x1b[0;94;47m▓▓\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │\x1b[0;94;47m▓▓▓▓▓\x1b[0;97m└─\x1b[0;94;47m▓▓\x1b[0;97m└─┐\x1b[0;90m┐\x1b[0;37m ┌\x1b[0;97m─┘\x1b[0;94;47m▓▓▓\x1b[0;97m└─┐\x1b[0;90m┐\x1b[0;37m │\x1b[0;94;47m▓▓▓▓▓▓▓\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │\x1b[0;94;47m▓▓▓▓▓▓▓\x1b[0;97m│\x1b[0;90m│\x1b[0;37m \x1b[0;97m┌─┘\x1b[0;94;47m▓▓▓▓\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │\x1b[0;94;47m▓▓▓▓▓\x1b[0;97m└─┐\x1b[0;90m┐\x1b[0m',
    '\x1b[0;37m│\x1b[0;94;47m░\x1b[0;97m┌\x1b[0;37m───┐\x1b[0;94;47m░\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │\x1b[0;94;47m░░\x1b[0;97m┐-\x1b[0;37m┌\x1b[0;94;47m░░\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │\x1b[0;94;47m░\x1b[0;37m┌─┐\x1b[0;94;47m░\x1b[0;97m┌\x1b[0;37m─┘  │\x1b[0;94;47m░░\x1b[0;97m└─\x1b[0;37m┘\x1b[0;94;47m░\x1b[0;37m┌\x1b[0;97m┘\x1b[0;90m└\x1b[0;37m │\x1b[0;94;47m░░\x1b[0;97m┌\x1b[0;37m─┐\x1b[0;94;47m░░\x1b[0;37m─┐\x1b[0;94;47m░░\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │\x1b[0;94;47m░░\x1b[0;97m┐-\x1b[0;37m┌\x1b[0;94;47m░░\x1b[0;97m│\x1b[0;90m│\x1b[0;37m └─┐\x1b[0;94;47m░░\x1b[0;37m───\x1b[0;97m┘\x1b[0;90m└\x1b[0;37m └─┐\x1b[0;94;47m░░\x1b[0;37m───\x1b[0;97m┘\x1b[0;90m└\x1b[0;37m │\x1b[0;94;47m░░\x1b[0;37m──┬─\x1b[0;97m┘\x1b[0;90m└\x1b[0;37m │\x1b[0;94;47m░\x1b[0;37m┌─┐\x1b[0;94;47m░\x1b[0;97m┌\x1b[0;37m─┘ \x1b[0m',
    '\x1b[0;37m│\x1b[0;90;47m▓\x1b[0;97m│\x1b[0;90m│\x1b[0;37m ┌\x1b[0;97m┘\x1b[0;90;47m▓\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │\x1b[0;90;47m▓▓\x1b[0;97m└\x1b[0;37m─┘\x1b[0;90;47m▓▓\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │\x1b[0;90;47m▓\x1b[0;37m└─┘\x1b[0;90;47m▓\x1b[0;97m└─┐\x1b[0;90m┐\x1b[0;37m │\x1b[0;90;47m▓▓▓▓▓\x1b[0;37m┌\x1b[0;97m┘\x1b[0;90m┐\x1b[0;37m  │\x1b[0;90;47m▓▓\x1b[0;97m│\x1b[0;90m│\x1b[0;37m│\x1b[0;90;47m▓▓\x1b[0;90m│\x1b[0;37m│\x1b[0;90;47m▓▓\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │\x1b[0;90;47m▓▓\x1b[0;97m└\x1b[0;37m─┘\x1b[0;90;47m▓▓\x1b[0;97m│\x1b[0;90m│\x1b[0;37m   │\x1b[0;90;47m▓▓\x1b[0;97m│\x1b[0;90m│\x1b[0;37m      │\x1b[0;90;47m▓▓\x1b[0;97m│\x1b[0;90m│\x1b[0;37m    │\x1b[0;90;47m▓▓▓▓\x1b[0;97m│\x1b[0;90m│\x1b[0;37m   │\x1b[0;90;47m▓\x1b[0;37m└─┘\x1b[0;90;47m▓\x1b[0;97m└─┐\x1b[0;90m┐\x1b[0m',
    '\x1b[0;37m│░\x1b[0;97m│\x1b[0;37m┌\x1b[0;97m─┘\x1b[0;37m░░\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │░░░░░░░\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │░░░░░░░\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │░░\x1b[0;97m┌\x1b[0;37m─┐░\x1b[0;97m└┐\x1b[0;90m┐\x1b[0;37m │░░\x1b[0;97m│\x1b[0;90m│\x1b[0;37m│░░\x1b[0;90m│\x1b[0;37m│░░\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │░░░░░░░\x1b[0;97m│\x1b[0;90m│\x1b[0;37m   │░░\x1b[0;97m│\x1b[0;90m│\x1b[0;37m      │░░\x1b[0;97m│\x1b[0;90m│\x1b[0;37m    │░░\x1b[0;97m└───┐\x1b[0;90m┐\x1b[0;37m │░░░░░░░\x1b[0;97m│\x1b[0;90m│\x1b[0m',
    '\x1b[0;37m│\x1b[0;90m▒\x1b[0;97m└┘\x1b[0;90m▒▒\x1b[0;97m┌─┘\x1b[0;90m└\x1b[0;37m │\x1b[0;90m▒▒\x1b[0;97m┌\x1b[0;37m─┐\x1b[0;90m▒▒\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │\x1b[0;90m▒\x1b[0;97m┌\x1b[0;37m─┐\x1b[0;90m▒▒▒\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │\x1b[0;90m▒▒\x1b[0;97m│\x1b[0;90m│\x1b[0;37m│\x1b[0;90m▒▒\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │\x1b[0;90m▒▒\x1b[0;97m│\x1b[0;90m│\x1b[0;37m│\x1b[0;90m▒▒│\x1b[0;37m│\x1b[0;90m▒▒\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │\x1b[0;90m▒▒\x1b[0;97m┌\x1b[0;37m─┐\x1b[0;90m▒▒\x1b[0;97m│\x1b[0;90m│\x1b[0;37m   │\x1b[0;90m▒▒\x1b[0;97m│\x1b[0;90m│\x1b[0;37m      │\x1b[0;90m▒▒\x1b[0;97m│\x1b[0;90m│\x1b[0;37m    └─┐\x1b[0;90m▒▒▒▒\x1b[0;97m│\x1b[0;90m│\x1b[0;37m │\x1b[0;90m▒\x1b[0;97m┌\x1b[0;37m─┐\x1b[0;90m▒▒▒\x1b[0;97m│\x1b[0;90m│\x1b[0m',
    '\x1b[0;37m└─────\x1b[0;97m┘\x1b[0;90m└\x1b[0;37m   └──\x1b[0;97m┘\x1b[0;90m└\x1b[0;37m└─\x1b[0;97m─┘\x1b[0;90m└\x1b[0;37m └─\x1b[0;97m┘\x1b[0;37m └───\x1b[0;97m┘\x1b[0;90m└\x1b[0;37m └──\x1b[0;97m┘\x1b[0;90m└\x1b[0;37m└──\x1b[0;97m┘\x1b[0;90m└\x1b[0;37m └──\x1b[0;97m┘\x1b[0;37m └── └──\x1b[0;97m┘\x1b[0;90m└\x1b[0;37m └──\x1b[0;97m┘\x1b[0;90m└\x1b[0;37m└─\x1b[0;97m─┘\x1b[0;90m└\x1b[0;37m   └──\x1b[0;97m┘\x1b[0;90m└\x1b[0;37m      └──\x1b[0;97m┘\x1b[0;90m└\x1b[0;37m      └────\x1b[0;97m┘\x1b[0;90m└\x1b[0;37m └─\x1b[0;97m┘\x1b[0;37m └───\x1b[0;97m┘\x1b[0;90m└\x1b[0m',
]


def strip_ansi(s):
    return _SGR.sub("", s)


def _parse_cells(line):
    """Split an ANSI string into [(sgr, char), ...]."""
    cells, cur, pos = [], None, 0
    for m in _SGR.finditer(line):
        cells += [(cur, ch) for ch in line[pos:m.start()]]
        cur = None if m.group(0) == RESET else m.group(0)
        pos = m.end()
    cells += [(cur, ch) for ch in line[pos:]]
    return cells


def _logo_glyphs():
    """Cut the logo into its letters so they can be reused in titles (D A R K M T E)."""
    rows = [_parse_cells(l) for l in BANNER_LINES]
    width = max(len(r) for r in rows)
    rows = [r + [(None, " ")] * (width - len(r)) for r in rows]
    blank = [all(r[c][1] == " " for r in rows) for c in range(width)]
    segs, start = [], None
    for c in range(width + 1):
        filled = c < width and not blank[c]
        if filled and start is None:
            start = c
        if not filled and start is not None:
            segs.append((start, c))
            start = None
    if len(segs) != len("DARKMATTER"):
        return {}
    out = {}
    for letter, (a, b) in zip("DARKMATTER", segs, strict=True):
        out.setdefault(letter, [r[a:b] for r in rows])
    return out


# 5-row x 7-column bitmaps for the letters that are not in the logo (X = filled)
_BITMAPS = {
    "B": ["XXXXXX.", "XX...XX", "XXXXXX.", "XX...XX", "XXXXXX."],
    "C": [".XXXXXX", "XX.....", "XX.....", "XX.....", ".XXXXXX"],
    "F": ["XXXXXXX", "XX.....", "XXXXX..", "XX.....", "XX....."],
    "G": [".XXXXXX", "XX.....", "XX.XXXX", "XX...XX", ".XXXXXX"],
    "H": ["XX...XX", "XX...XX", "XXXXXXX", "XX...XX", "XX...XX"],
    "I": ["XXXXXXX", "..XX...", "..XX...", "..XX...", "XXXXXXX"],
    "J": ["..XXXXX", "...XX..", "...XX..", "XX.XX..", ".XXX..."],
    "L": ["XX.....", "XX.....", "XX.....", "XX.....", "XXXXXXX"],
    "N": ["XX...XX", "XXX..XX", "XX.X.XX", "XX..XXX", "XX...XX"],
    "O": [".XXXXX.", "XX...XX", "XX...XX", "XX...XX", ".XXXXX."],
    "P": ["XXXXXX.", "XX...XX", "XXXXXX.", "XX.....", "XX....."],
    "Q": [".XXXXX.", "XX...XX", "XX.X.XX", "XX..XX.", ".XXXX.X"],
    "S": [".XXXXXX", "XX.....", ".XXXXX.", ".....XX", "XXXXXX."],
    "U": ["XX...XX", "XX...XX", "XX...XX", "XX...XX", ".XXXXX."],
    "V": ["XX...XX", "XX...XX", "XX...XX", ".XX.XX.", "..XXX.."],
    "W": ["XX...XX", "XX...XX", "XX.X.XX", "XXX.XXX", "XX...XX"],
    "X": ["XX...XX", ".XX.XX.", "..XXX..", ".XX.XX.", "XX...XX"],
    "Y": ["XX...XX", ".XX.XX.", "..XXX..", "..XX...", "..XX..."],
    "Z": ["XXXXXXX", "....XX.", "...XX..", "..XX...", "XXXXXXX"],
    "0": [".XXXXX.", "XX..XXX", "XX.X.XX", "XXX..XX", ".XXXXX."],
    "1": ["..XX...", ".XXX...", "..XX...", "..XX...", "XXXXXX."],
    "2": [".XXXXX.", "XX...XX", "...XXX.", ".XXX...", "XXXXXXX"],
    "3": ["XXXXXX.", ".....XX", "..XXXX.", ".....XX", "XXXXXX."],
    "4": ["XX..XX.", "XX..XX.", "XXXXXXX", "....XX.", "....XX."],
    "5": ["XXXXXXX", "XX.....", "XXXXXX.", ".....XX", "XXXXXX."],
    "6": [".XXXXX.", "XX.....", "XXXXXX.", "XX...XX", ".XXXXX."],
    "7": ["XXXXXXX", "....XX.", "...XX..", "..XX...", "..XX..."],
    "8": [".XXXXX.", "XX...XX", ".XXXXX.", "XX...XX", ".XXXXX."],
    "9": [".XXXXX.", "XX...XX", ".XXXXXX", ".....XX", ".XXXXX."],
    "-": [".......", ".......", ".XXXXX.", ".......", "......."],
    # letters that also exist in the logo, used only if the logo could not be parsed
    "A": ["..XXX..", "XX...XX", "XX...XX", "XXXXXXX", "XX...XX"],
    "D": ["XXXXX..", "XX..XX.", "XX...XX", "XX..XX.", "XXXXX.."],
    "E": ["XXXXXXX", "XX.....", "XXXXX..", "XX.....", "XXXXXXX"],
    "K": ["XX..XX.", "XX.XX..", "XXXX...", "XX.XX..", "XX..XX."],
    "M": ["XX...XX", "XXX.XXX", "XX.X.XX", "XX...XX", "XX...XX"],
    "R": ["XXXXXX.", "XX...XX", "XXXXXX.", "XX.XX..", "XX..XXX"],
    "T": ["XXXXXXX", "..XX...", "..XX...", "..XX...", "..XX..."],
}
_BOX = {  # keys follow the order U, D, L, R
    "DR": "\u250c", "DL": "\u2510", "UR": "\u2514", "UL": "\u2518", "LR": "\u2500", "UD": "\u2502",
    "DLR": "\u252c", "ULR": "\u2534", "UDR": "\u251c", "UDL": "\u2524", "UDLR": "\u253c",
    "L": "\u2500", "R": "\u2500", "U": "\u2502", "D": "\u2502",
}
_SHADOW = {"\u2510": "\u2510", "\u2502": "\u2502", "\u2518": "\u2514", "\u2524": "\u2502"}


def _make_glyph(bm):
    """Draw a bitmap with a box-drawing outline, shaded fill and drop shadow (logo style)."""
    h, w = len(bm), len(bm[0])

    def on(r, c):
        return 1 <= r <= h and 1 <= c <= w and bm[r - 1][c - 1] == "X"

    def n8(r, c):
        return {(r + i, c + j) for i in (-1, 0, 1) for j in (-1, 0, 1)}

    ring = {(r, c) for r in range(h + 2) for c in range(w + 2)
            if not on(r, c) and any(on(a, b) for a, b in n8(r, c))}

    def linked(a, b):  # two touching outline cells belong to the same contour
        return any(on(r, c) for r, c in n8(*a) & n8(*b))

    rows = [[(None, " ")] * (w + 3) for _ in range(h + 2)]
    for r in range(h + 2):
        for c in range(w + 2):
            if on(r, c):
                rows[r][c] = FILL_STYLES[min(r - 1, len(FILL_STYLES) - 1)]
            elif (r, c) in ring:
                d = "".join(k for k, (dr, dc) in (("U", (-1, 0)), ("D", (1, 0)), ("L", (0, -1)), ("R", (0, 1)))
                            if (r + dr, c + dc) in ring and linked((r, c), (r + dr, c + dc)))
                key = "".join(sorted(d, key="UDLR".index))
                rows[r][c] = (C_EDGE_L if c == 0 else C_EDGE, _BOX.get(key, "\u00b7"))
    for r in range(h + 2):
        for c in range(w + 2):
            if (r, c) in ring and (r, c + 1) not in ring and not on(r, c + 1):
                sh = _SHADOW.get(rows[r][c][1])
                if sh:
                    rows[r][c + 1] = (C_SHADOW, sh)
    return rows


_GLYPHS = _logo_glyphs()


def _glyph(ch):
    ch = ch.upper()
    if ch not in _GLYPHS and ch in _BITMAPS:
        _GLYPHS[ch] = _make_glyph(_BITMAPS[ch])
    return _GLYPHS.get(ch)


def render_big(text):
    """Render text in the big colour font. Returns 7 ANSI strings (or [] if nothing printable)."""
    rows = [[] for _ in range(7)]
    first = True
    for ch in text:
        if ch == " ":
            for r in rows:
                r += [(None, " ")] * 3
            continue
        g = _glyph(ch)
        if not g:
            continue
        for i in range(7):
            rows[i] += ([] if first else [(None, " ")]) + list(g[i])
        first = False
    if first:
        return []
    out = []
    for row in rows:
        buf, cur = [], None
        for sgr, ch in row:
            if sgr != cur:
                buf.append(sgr or RESET)
                cur = sgr
            buf.append(ch)
        out.append("".join(buf) + RESET)
    return out


# =====================================================================
#  module: darkmatter/ui/ui_kit.py  -  UI kit: palette, width helpers, boxes, tables and message helpers.
# =====================================================================
# =====================================================================
#  DARKMATTER UI kit: palette, width helpers, boxes, tables, messages
# =====================================================================
USE_COLOR = True
CLEAR_ON = True
R = "\033[0m"
BORDER = "\033[38;5;67m"            # steel blue frame
ACCENT = "\033[38;5;75m"            # sky blue (keys, highlights)
TEXT = "\033[38;5;252m"             # primary text
DIM = "\033[38;5;245m"              # secondary text / labels
TITLE = "\033[1;38;5;255m"          # headings
OK = "\033[38;5;78m"                # green
BAD = "\033[38;5;203m"              # red
WARN = "\033[38;5;179m"             # amber
KEYBG = "\033[1;38;5;16;48;5;75m"   # dark text on sky-blue chip

_SGR_SPLIT = re.compile(r"(\x1b\[[0-9;]*m)")


def set_color(on):
    global USE_COLOR
    USE_COLOR = bool(on)


def set_clear(on):
    global CLEAR_ON
    CLEAR_ON = bool(on)


def clearing():
    return CLEAR_ON and sys.stdout.isatty()


CLEAR_SEQ = "\033[H\033[2J\033[3J"   # home, erase screen, THEN erase scrollback (this order matters)


def clear_screen():
    """Wipe the screen and the scrollback so every menu / screen opens clean (no growing output).

    Order matters: Windows Terminal / conhost push the visible screen into the scrollback on
    "erase screen", so the scrollback must be erased AFTER it. On Windows `cls` is run first because
    it empties the whole console buffer even where the escape sequences are only partly supported."""
    if not clearing():
        return
    sys.stdout.flush()
    if sys.platform == "win32":
        try:
            os.system("cls")
        except Exception:
            pass
    sys.stdout.write(CLEAR_SEQ)
    sys.stdout.flush()


def put(text="", end="\n"):
    """Single output gate: strips colour when it is switched off."""
    if not USE_COLOR:
        text = strip_ansi(text)
    sys.stdout.write(text + end)


def flush():
    try:
        sys.stdout.flush()
    except Exception:
        pass


def paint(color, text):
    return f"{color}{text}{R}"


@lru_cache(maxsize=8192)
def _cw(ch):
    if unicodedata.combining(ch) or ch in "\u200d\ufe0e\ufe0f":
        return 0
    return 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1


def dw(s):
    """Display width in terminal columns (emoji / wide characters count as 2)."""
    return sum(_cw(ch) for ch in s)


def vlen(s):
    return dw(strip_ansi(s))


def pad(s, width, align="<"):
    gap = width - vlen(s)
    if gap <= 0:
        return s
    if align == ">":
        return " " * gap + s
    if align == "^":
        left = gap // 2
        return " " * left + s + " " * (gap - left)
    return s + " " * gap


def clip(s, width):
    """Shorten a plain string to `width` columns (adds an ellipsis)."""
    if dw(s) <= width:
        return s
    out, w = [], 0
    for ch in s:
        cw = _cw(ch)
        if w + cw > width - 1:
            break
        out.append(ch)
        w += cw
    return "".join(out) + "\u2026"


def clip_ansi(s, width):
    """Same as clip() but keeps colour codes intact."""
    if vlen(s) <= width:
        return s
    out, w = [], 0
    for part in _SGR_SPLIT.split(s):
        if _SGR_SPLIT.fullmatch(part):
            out.append(part)
            continue
        for ch in part:
            cw = _cw(ch)
            if w + cw > width - 1:
                out.append("\u2026" + R)
                return "".join(out)
            out.append(ch)
            w += cw
    return "".join(out)


def wrap_plain(items, width, sep="  "):
    """Greedy-wrap a list of short strings into lines no wider than `width`."""
    lines, cur = [], ""
    for it in items:
        cand = it if not cur else cur + sep + it
        if cur and vlen(cand) > width:
            lines.append(cur)
            cur = it
        else:
            cur = cand
    if cur:
        lines.append(cur)
    return lines


def wrap_text(text, width):
    """Word-wrap plain text (keeps existing line breaks)."""
    out = []
    for part in str(text).splitlines() or [""]:
        out += wrap_plain(part.split(" "), width, " ") or [""]
    return out


def term_width():
    return shutil.get_terminal_size((120, 30)).columns


def term_height():
    return shutil.get_terminal_size((120, 30)).lines


LOGO_W = 112
FULL_W = LOGO_W + 4          # logo + 1 space padding + frame on both sides


def ui_width():
    """One width for every box on screen: the logo box when it fits, else what the terminal allows."""
    tw = term_width()
    return FULL_W if tw > FULL_W else max(60, tw - 1)


def hline(w, left, right, title=None, split=None, mid="\u252c", title2=None, color=None):
    """Horizontal frame line, optionally with title(s) and a column divider at `split` (inner index)."""
    color = color or BORDER
    inner = w - 2

    def head(t):
        return (f"\u2500 {TITLE}{t}{R}{color} ", vlen(t) + 3) if t else ("", 0)

    h1, u1 = head(title)
    if split is None:
        body = h1 + "\u2500" * max(0, inner - u1)
    else:
        h2, u2 = head(title2)
        body = (h1 + "\u2500" * max(0, split - u1) + mid + h2 +
                "\u2500" * max(0, inner - split - 1 - u2))
    return f"{color}{left}{body}{right}{R}"


class Box:
    """Collects frame lines; every row is padded by display width so the borders always line up."""

    def __init__(self, title=None, w=None, color=None):
        self.w = w or ui_width()
        self.color = color or BORDER
        self.lines = [hline(self.w, "\u250c", "\u2510", title, color=self.color)]

    @property
    def inner(self):
        return self.w - 4

    def row(self, content="", align="<"):
        c = self.color
        self.lines.append(f"{c}\u2502{R} {pad(clip_ansi(content, self.inner), self.inner, align)} {c}\u2502{R}")

    def rows(self, items, align="<"):
        for it in items:
            self.row(it, align)

    def rule(self, title=None):
        self.lines.append(hline(self.w, "\u251c", "\u2524", title, color=self.color))

    # ----- two-panel helpers -----
    def panel_widths(self):
        total = self.w - 7
        wl = total // 2
        return wl, total - wl

    def top_split(self, title_l, title_r):
        wl, _ = self.panel_widths()
        self.lines[-1:] = [hline(self.w, "\u250c", "\u2510", title_l, split=wl + 2, mid="\u252c",
                                 title2=title_r, color=self.color)]

    def rule_split(self, kind="mid", title_l=None, title_r=None):
        wl, _ = self.panel_widths()
        mid = {"open": "\u252c", "mid": "\u253c", "close": "\u2534"}[kind]
        self.lines.append(hline(self.w, "\u251c", "\u2524", title_l, split=wl + 2, mid=mid,
                                title2=title_r, color=self.color))

    def cols(self, left, right):
        wl, wr = self.panel_widths()
        c = self.color
        self.lines.append(
            f"{c}\u2502{R} {pad(clip_ansi(left, wl), wl)} {c}\u2502{R} "
            f"{pad(clip_ansi(right, wr), wr)} {c}\u2502{R}")

    def end(self, split=False):
        if split:
            wl, _ = self.panel_widths()
            self.lines.append(hline(self.w, "\u2514", "\u2518", split=wl + 2, mid="\u2534", color=self.color))
        else:
            self.lines.append(hline(self.w, "\u2514", "\u2518", color=self.color))
        return self.lines

    def show(self, split=False):
        for ln in self.end(split):
            put(ln)
        flush()


# ---------------------------------------------------------------------
#  Small building blocks
# ---------------------------------------------------------------------
def chip(text):
    """Key / label tile: dark text on a sky-blue background."""
    return f"{KEYBG} {text} {R}"


def tag(label, bg):
    return f"\033[1;38;5;16;48;5;{bg}m {label:^5} {R}"


def kv(label, value, lw=9):
    return f"{DIM}{label:<{lw}}{R} {TEXT}{value}{R}"


def dot(color):
    return f"{color}\u25cf{R}"


def say(msg):
    put(f"  {DIM}{msg}{R}")


def info(msg):
    put(f"  {tag('INFO', 75)} {TEXT}{msg}{R}")


def good(msg):
    put(f"  {tag('OK', 78)} {TEXT}{msg}{R}")


def warn(msg):
    put(f"  {tag('WARN', 179)} {TEXT}{msg}{R}")


def fail(msg):
    """Error message in a red frame (long text is wrapped)."""
    put()
    box = Box("ERROR", color=BAD)
    for ln in wrap_text(msg, box.inner):
        box.row(f"{TEXT}{ln}{R}")
    box.show()


def ask_prompt(label):
    return f"  {ACCENT}{label}{R} {chip('>')} "


def fmt_uptime(secs):
    secs = int(secs)
    return f"{secs // 3600:02d}:{secs % 3600 // 60:02d}:{secs % 60:02d}"


# ---------------------------------------------------------------------


# =====================================================================
#  module: darkmatter/ui/widgets.py  -  Table helper and the ASCII loading bar (Progress).
# =====================================================================
#  Table helper: columns = [(header, width, align)], rows = lists of cell strings
# ---------------------------------------------------------------------
def table_lines(columns, rows, gap=2):
    sp = " " * gap
    head = sp.join(pad(f"{DIM}{h}{R}", w, a) for h, w, a in columns)
    body = [sp.join(pad(clip_ansi(str(c), w), w, a) for c, (_, w, a) in zip(row, columns)) for row in rows]
    return head, body


# ---------------------------------------------------------------------
#  ASCII loading bar (spinner + bar + elapsed time)
# ---------------------------------------------------------------------
BLK_FULL, BLK_LIGHT = "\u2588", "\u2591"


class Progress:
    """`async with Progress("label", total=N) as bar: ... bar.set(done)`.
    Without a total it shows a bouncing bar."""

    def __init__(self, label, total=None, width=30):
        self.label, self.total, self.width = label, total, width
        self.value, self._tick, self._task, self._maxlen = 0, 0, None, 0
        self._t0 = time.monotonic()
        self._tty = sys.stdout.isatty()

    def set(self, value, total=None):
        self.value = value
        if total is not None:
            self.total = total

    def _line(self, final=False):
        spin = "|/-\\"[self._tick % 4]
        el = time.monotonic() - self._t0
        if self.total:
            frac = 1.0 if final else min(max(self.value / self.total, 0.0), 1.0)
            filled = int(self.width * frac)
            bar = f"{ACCENT}{BLK_FULL * filled}{R}{DIM}{BLK_LIGHT * (self.width - filled)}{R}"
            tail = f"{TEXT}{int(frac * 100):3d}%{R} {DIM}{el:4.1f}s{R} {ACCENT}{' ' if final else spin}{R}"
        else:
            span = 6
            n = self.width - span
            p = self._tick % (2 * n)
            pos = p if p <= n else 2 * n - p
            bar = f"{DIM}{BLK_LIGHT * pos}{R}{ACCENT}{BLK_FULL * span}{R}{DIM}{BLK_LIGHT * (n - pos)}{R}"
            tail = f"{DIM}{el:4.1f}s{R} {ACCENT}{spin}{R}"
        return f"  {TEXT}{self.label}{R} {BORDER}[{R}{bar}{BORDER}]{R} {tail}"

    def _draw(self, final=False):
        if not self._tty:
            return
        line = self._line(final)
        self._maxlen = max(self._maxlen, vlen(line) + 2)
        text = "\r" + pad(line, self._maxlen)
        sys.stdout.write(text if USE_COLOR else strip_ansi(text))
        sys.stdout.flush()

    async def _loop(self):
        while True:
            self._draw()
            self._tick += 1
            await asyncio.sleep(0.1)

    async def __aenter__(self):
        self._t0 = time.monotonic()
        self._task = asyncio.create_task(self._loop())
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        if exc_type is None:
            if self._tty:
                self._draw(final=True)
                sys.stdout.write("\n")
            else:
                put(self._line(final=True))
        elif self._tty:
            sys.stdout.write("\r" + " " * (self._maxlen + 2) + "\r")
        flush()
        return False


# ---------------------------------------------------------------------


# =====================================================================
#  module: darkmatter/ui/market_view.py  -  Candle colours, market list printing and market selection parsing.
# =====================================================================
#  Candle colours
# ---------------------------------------------------------------------
DOJI_BODY_RATIO = 0.10  # body smaller than 10% of the high-low range counts as a doji
ICON_UP, ICON_DOWN, ICON_DOJI = "\U0001F7E9", "\U0001F7E5", "\u2B1C"   # green, red, white squares


def candle_icon(c):
    rng = c["h"] - c["l"]
    body = abs(c["c"] - c["o"])
    if body == 0 or rng <= 0 or body / rng < DOJI_BODY_RATIO:
        return ICON_DOJI
    return ICON_UP if c["c"] > c["o"] else ICON_DOWN


def candle_color(c):
    return {ICON_UP: "up", ICON_DOWN: "down", ICON_DOJI: "doji"}[candle_icon(c)]



# ---------------------------------------------------------------------
#  Market list (columns follow pyquotex: id, symbol, name, category, ..., [5]=payout,
#  [14]=open flag, [-8]=5-minute profit)
# ---------------------------------------------------------------------
def parse_instrument(row):
    if not isinstance(row, list) or len(row) < 3 or not isinstance(row[1], str):
        return None

    def num(v):
        return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None

    symbol = row[1]
    return {
        "id": row[0],
        "symbol": symbol,
        "name": str(row[2]).replace("\n", " "),
        "category": str(row[3]).lower() if len(row) > 3 and row[3] else "other",
        "payout": num(row[5]) if len(row) > 5 else None,
        "open": bool(row[14]) if len(row) > 14 else None,
        "p5": num(row[-8]) if len(row) >= 20 else None,
        "otc": symbol.lower().endswith("_otc"),
    }


def fmt_payout(v):
    return "-" if v is None else f"{v:g}%"


def market_items(rows):
    """
    Parse instruments and return (live, otc). Each list is sorted: open markets first, highest
    payout on top, closed markets last. Every item gets a unique number "no" (live first, then
    OTC) that is used for selection.
    """
    items = [x for x in (parse_instrument(r) for r in rows or []) if x]

    def key(x):
        return (0 if x["open"] else 1, -(x["payout"] if x["payout"] is not None else -1), x["symbol"])

    live = sorted((x for x in items if not x["otc"]), key=key)
    otc = sorted((x for x in items if x["otc"]), key=key)
    for n, x in enumerate(live + otc, 1):
        x["no"] = n
    return live, otc


def pay_color(x):
    p = x["payout"]
    if not x["open"] or p is None:
        return DIM
    return OK if p >= 85 else TEXT if p >= 70 else WARN


def market_columns(panel_w):
    """Column spec for one market panel; the name column only appears when there is room."""
    fixed = [("#", 3, ">"), ("", 1, "<"), ("PAIR", 16, "<")]
    tail = [("PAY", 6, ">"), ("5M", 6, ">")]
    used = sum(w for _, w, _ in fixed + tail) + (len(fixed) + len(tail) - 1)
    name_w = panel_w - used - 1
    cols = fixed + ([("NAME", min(name_w, 22), "<")] if name_w >= 8 else []) + tail
    return cols


def market_row(x, has_name):
    op = x["open"]
    dc = OK if op else (WARN if op is None else BAD)
    base = TEXT if op else DIM
    cells = [f"{ACCENT if op else DIM}{x['no']}{R}", dot(dc), f"{base}{x['symbol']}{R}"]
    if has_name:
        cells.append(f"{DIM}{x['name'].replace(' (OTC)', '')}{R}")
    cells += [f"{pay_color(x)}{fmt_payout(x['payout'])}{R}", f"{DIM}{fmt_payout(x['p5'])}{R}"]
    return cells


def group_title(name, group):
    return f"{name} {DIM}{sum(1 for x in group if x['open'])}/{len(group)} open{R}"


def market_legend(updated_age):
    return (f"{dot(OK)} {DIM}open{R}   {dot(BAD)} {DIM}closed{R}   {DIM}\u2502   sorted by payout "
            f"(highest first), closed last   \u2502   updated {updated_age}s ago{R}")


def print_market(ws, live, otc):
    """LIVE and OTC side by side in one frame (stacked when the terminal is too narrow)."""
    age = max(0, int(time.time() - ws.instruments_ts))
    box = Box()
    split = box.w >= 75
    put()
    if split:
        wl, wr = box.panel_widths()
        cl = cr = market_columns(wl)  # identical columns in both panels
        hl, bl = table_lines(cl, [market_row(x, len(cl) == 6) for x in live], gap=1)
        hr, br = table_lines(cr, [market_row(x, len(cr) == 6) for x in otc], gap=1)
        box.top_split(group_title("LIVE MARKETS", live), group_title("OTC MARKETS", otc))
        box.cols(hl, hr)
        box.rule_split("mid")
        for i in range(max(len(bl), len(br))):
            box.cols(bl[i] if i < len(bl) else "", br[i] if i < len(br) else "")
        box.rule_split("close")
        box.row(market_legend(age))
        box.show()
        return
    first = True
    for name, group in (("LIVE MARKETS", live), ("OTC MARKETS", otc)):
        cols = market_columns(box.inner)
        head, body = table_lines(cols, [market_row(x, len(cols) == 6) for x in group], gap=1)
        if first:
            box.lines[0] = hline(box.w, "\u250c", "\u2510", group_title(name, group))
            first = False
        else:
            box.rule(group_title(name, group))
        box.row(head)
        box.rule()
        box.rows(body)
    box.rule()
    box.row(market_legend(age))
    box.show()


def parse_selection(text, by_no, by_sym):
    """
    Parse a selection line: numbers (3 5), ranges (41-45) and symbols (USDINR_otc).
    Returns (picked_items, errors).
    """
    picked, errors = [], []
    for tok in re.split(r"[,\s]+", text.strip()):
        if not tok:
            continue
        m = re.fullmatch(r"([0-9]+)-([0-9]+)", tok)
        if m:
            a, b = sorted((int(m.group(1)), int(m.group(2))))
            found = [by_no[i] for i in range(a, min(b, a + 1000) + 1) if i in by_no]
            picked += found
            if not found:
                errors.append(tok)
        else:
            item = by_no.get(int(tok)) if (tok.isascii() and tok.isdecimal()) else by_sym.get(tok.lower())
            if item:
                picked.append(item)
            else:
                errors.append(tok)
    return picked, errors


def print_selection_help():
    box = Box("SELECT MARKETS")
    box.row(f"{chip('3 5')} {DIM}numbers{R}    {chip('41-45')} {DIM}range{R}    "
            f"{chip('USDINR_otc')} {DIM}symbol{R}")
    box.row(f"{chip('Enter')} {DIM}done (on an empty line){R}    {chip('clear')} {DIM}reset{R}    "
            f"{chip('q')} {DIM}cancel{R}")
    put()
    box.show()


def print_selection(selected):
    box = Box(f"SELECTED  {len(selected)}")
    tags = []
    for s in selected:
        t = f"{ACCENT}{s['symbol']}{R} {DIM}{fmt_payout(s['payout'])}{R}"
        if not s["open"]:
            t += f" {BAD}closed{R}"
        tags.append(t)
    for ln in wrap_plain(tags, box.inner, "   "):
        box.row(ln)
    box.show()


# ---------------------------------------------------------------------


# =====================================================================
#  module: darkmatter/connection/quotex_ws.py  -  Quotex protocol client (Socket.IO EIO=3).
# =====================================================================
# =====================================================================
#  Quotex protocol client (Socket.IO EIO=3)
# =====================================================================
class QuotexWS:
    def __init__(self, ssid, cookies, demo=True, debug=False, passive=False):
        self.ssid = ssid or ""
        self.cookies = cookies or {}
        self.demo = demo
        self.debug = debug
        self.passive = passive  # bridge mode: the page already did handshake/auth/pings
        self.tr = None
        self.frames = []
        self.balance = None
        self.seen_events = []
        self._pending = {}          # request index -> (asset, future)
        self.instruments = []       # raw rows from instruments/list
        self.instruments_ts = 0.0
        self._instr_evt = asyncio.Event()
        self._req_seq = int(time.time() * 1000)
        self.samples = {}           # first payload seen per event (for debugging)
        self.tick_assets = {}       # asset -> tick count (live mode)
        self.tick_cb = None
        self.event_hook = None      # fn(event name, payload) - AUTO TRADING listens to order events here
        self._auth = asyncio.Event()
        self._bal = asyncio.Event()
        self._tasks = []
        self._bg_task = None
        self.last_rx = 0.0          # monotonic time of the last frame (connection health)
        self.balance_ts = 0.0
        self._seen_set = set()

    def _log(self, *a):
        if self.debug:
            print("[debug]", *a)

    # ---------------- connection ----------------
    def attach(self, transport):
        """Bridge mode: start reading from an already-connected transport."""
        self.tr = transport
        self._tasks = [asyncio.create_task(self._reader())]

    async def connect(self, timeout=15):
        """Direct mode."""
        url = EXTRA.get("ws_url") or WS_URL_DEFAULT
        headers = {"Origin": EXTRA.get("origin") or BASE}
        if EXTRA.get("ua"):
            headers["User-Agent"] = EXTRA["ua"]
        if self.cookies:
            headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in self.cookies.items())
        info("Connecting to " + url.split("?")[0])

        errors = []
        for cls in (CurlTransport, AioTransport, RawTransport):
            self.tr = cls()
            self.frames = []
            self._auth.clear()
            try:
                await self.tr.connect(url, headers)
            except Exception as e:
                errors.append(f"[{cls.__name__}] could not connect: {e}")
                self.tr = None
                continue
            self._tasks = [
                asyncio.create_task(self._reader()),
                asyncio.create_task(self._heartbeat()),
            ]
            try:
                await asyncio.wait_for(self._auth.wait(), timeout)
                return
            except asyncio.TimeoutError:
                sample = " | ".join(f"{k}:{v}" for k, v in self.frames[:6]) or "no frames received"
                msg = f"[{cls.__name__}] authorization failed. Frames: {sample}"
                if self.tr.last_error:
                    msg += f"\n    reason: {self.tr.last_error}"
                errors.append(msg)
                await self.close()
        raise QuotexError("\n".join(errors))

    async def close(self):
        tasks = list(self._tasks) + ([self._bg_task] if self._bg_task else [])
        self._tasks, self._bg_task = [], None
        for t in tasks:
            t.cancel()
        if tasks:  # wait until they are really gone, so nothing outlives the session
            await asyncio.gather(*tasks, return_exceptions=True)
        if self.tr:
            await self.tr.close()
            self.tr = None

    async def _heartbeat(self):
        try:
            while True:
                await asyncio.sleep(20)
                await self.tr.send_str("2")
        except Exception:
            pass

    async def _emit(self, name, payload=None):
        arr = [name] if payload is None else [name, payload]
        await self.tr.send_str("42" + json.dumps(arr))

    # ---------------- reader ----------------
    async def _reader(self):
        pending = None
        try:
            while True:
                kind, data = await self.tr.recv()
                if kind == "close":
                    break
                self.last_rx = time.monotonic()
                if kind == "skip":
                    continue

                if kind == "text":
                    t = data
                    if len(self.frames) < 12:
                        self.frames.append(("T", t[:140].replace(self.ssid or "\x00", "***")))
                    if self.debug:
                        self._log("TXT", t[:160])
                    if t == "2":
                        if not self.passive:
                            await self.tr.send_str("3")
                    elif t.startswith("0"):
                        if not self.passive:
                            await self.tr.send_str("40")
                    elif t.startswith("40"):
                        if not self.passive:
                            await self._emit(
                                "authorization",
                                {
                                    "session": self.ssid,
                                    "isDemo": 1 if self.demo else 0,
                                    "tournamentId": 0,
                                },
                            )
                    elif t.startswith("451-"):
                        try:
                            pending = json.loads(t[4:])[0]
                        except Exception:
                            pending = None
                    elif t.startswith("42"):
                        try:
                            arr = json.loads(t[2:])
                            self._handle(arr[0], arr[1] if len(arr) > 1 else None)
                        except Exception as e:
                            self._log("handler error:", repr(e))
                else:  # binary frame (payload of a preceding "451-" placeholder)
                    raw = data
                    if len(self.frames) < 12:
                        self.frames.append(("B", raw[:100]))
                    if self.debug:
                        self._log("BIN", raw[:160])
                    idx = [i for i in (raw.find(b"{"), raw.find(b"[")) if i != -1]
                    if idx:
                        try:
                            obj = json.loads(raw[min(idx):].decode("utf-8", "ignore"))
                            self._handle(pending or "binary", obj)
                        except Exception as e:
                            self._log("handler error:", repr(e))
                    pending = None
        except asyncio.CancelledError:
            pass
        except Exception as e:  # never die silently
            self._log("reader stopped:", repr(e))

    def _handle(self, name, data):
        if name not in self._seen_set:
            self._seen_set.add(name)
            self.seen_events.append(name)
        key = "recv:" + str(name)
        if key not in self.samples:
            if any(w in str(name) for w in ("orders", "settings", "balance", "pending")):
                self.samples[key] = "(omitted: may contain private data)"
            else:
                self.samples[key] = mask_secrets(json.dumps(data, default=str))[:500]

        if name == "s_authorization" or self.passive:
            self._auth.set()

        hook = self.event_hook
        if hook is not None:                # a broken listener must never stop the reader
            try:
                hook(name, data)
            except Exception as e:
                self._log("event hook error:", repr(e))

        bal = None if (name == "instruments/list" or str(name).startswith("history/")) else find_balance(data)
        if bal:
            self.balance = bal
            self.balance_ts = time.time()
            self._bal.set()
            self._auth.set()

        if name == "instruments/list":
            rows = data if isinstance(data, list) else (data.get("list") if isinstance(data, dict) else None)
            if rows:
                self.instruments = rows
                self.instruments_ts = time.time()
                self._instr_evt.set()

        # history reply: the server echoes our unique request "index"
        if isinstance(data, dict) and str(name).startswith("history/"):
            idx = data.get("index")
            entry = self._pending.get(idx) if isinstance(idx, int) else None
            if entry is not None:
                asset, fut = entry
                if data.get("asset") in (None, asset) and not fut.done():
                    fut.set_result(data)
                    return

        # live ticks
        if not isinstance(data, dict):
            for asset, ts, price in iter_ticks(data):
                self.tick_assets[asset] = self.tick_assets.get(asset, 0) + 1
                if self.tick_cb:
                    self.tick_cb(asset, ts, price)

    def note_sent(self, text):
        """Record what the page (or this bot) sends, so request formats can be learned."""
        if not text.startswith("42"):
            return
        try:
            name = json.loads(text[2:])[0]
        except Exception:
            return
        key = "sent:" + str(name)
        if key not in self.samples:
            self.samples[key] = mask_secrets(text)[:400]

    def save_samples(self, path="quotex_samples.json"):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.samples, f, indent=2, ensure_ascii=False)
        return path

    # ---------------- public API ----------------
    async def get_balance(self, timeout=15):
        try:
            await asyncio.wait_for(self._bal.wait(), timeout)
        except asyncio.TimeoutError:
            raise QuotexError(
                "No balance event received. Run with --debug and check the frames. "
                "Events seen so far: " + ", ".join(self.seen_events)
            ) from None
        return self.balance

    async def refresh_instruments(self, timeout=8):
        """Ask the server for a fresh instrument list (payouts change). True if fresh data arrived."""
        self._instr_evt.clear()
        try:
            await self._emit("instruments/get")
            await asyncio.wait_for(self._instr_evt.wait(), timeout)
            return True
        except (asyncio.TimeoutError, QuotexError):
            return False

    def start_background_refresh(self, interval=10.0):
        """Keep the instrument list (and its payouts) fresh so the market list opens instantly."""
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._bg_refresh(interval))

    async def _bg_refresh(self, interval):
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    await self._emit("instruments/get")
                except Exception:
                    pass  # socket briefly unavailable (e.g. page reloading); try again next round
        except asyncio.CancelledError:
            pass

    async def fresh_instruments(self, max_age=15.0, timeout=5.0):
        """Instant when the cached list is young; otherwise ask the server and wait briefly."""
        if self.instruments and time.time() - self.instruments_ts <= max_age:
            return True
        return await self.refresh_instruments(timeout)

    def _next_index(self):
        self._req_seq += 1
        return self._req_seq

    async def subscribe(self, asset, period=60):
        """The same three messages the Quotex page (and pyquotex) send when an asset is opened."""
        await self._emit("instruments/update", {"asset": asset, "period": period})
        await self._emit("chart_notification/get", {"asset": asset, "version": "1.0.0"})
        await self._emit("depth/follow", asset)


    async def change_account(self, demo, tournament_id=0):
        """Switch the active account (same message as the Quotex page and pyquotex.change_account)."""
        self.demo = bool(demo)
        await self._emit("account/change", {"demo": 1 if demo else 0, "tournamentId": int(tournament_id)})

    async def _history_batch(self, asset, period, end_time, seconds, timeout):
        """One history/load request. Returns parsed candles, or None on timeout."""
        index = self._next_index()
        fut = asyncio.get_running_loop().create_future()
        self._pending[index] = (asset, fut)
        try:
            await self._emit(
                "history/load",
                {"asset": asset, "index": index, "time": int(end_time),
                 "offset": int(seconds), "period": period},
            )
            data = await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            self._pending.pop(index, None)
        return parse_candle_rows(data.get("data") or data.get("candles") or [])

    async def get_candles(self, asset, period=60, count=100, timeout=10, progress=None):
        """
        Fetch the last `count` closed candles, paging backwards in chunks of
        HISTORY_CHUNK candles (the same approach as pyquotex.get_historical_candles).
        Returns candles sorted oldest -> newest as dicts {t, o, h, l, c, ticks}.
        """
        if progress:
            progress(0, count)
        await self.subscribe(asset, period)
        chunk = period * HISTORY_CHUNK
        got = {}
        end_t = int(time.time())
        misses = 0
        first = True
        while len(got) < count and misses < 4:
            rows = await self._history_batch(asset, period, end_t, chunk, timeout)
            if rows is None and first:
                # Quotex answers history for the asset that was subscribed most recently, so
                # subscribe again right before every retry
                for _ in range(HISTORY_RETRIES):
                    await self.subscribe(asset, period)
                    rows = await self._history_batch(asset, period, end_t, chunk, timeout)
                    if rows is not None:
                        break
            if rows is None:  # timeout
                if first:
                    raise QuotexError(
                        f"No history reply for {asset} ({HISTORY_RETRIES + 1} tries x {timeout}s). "
                        "The market may be closed or temporarily unavailable - check it in "
                        "TEST BOT > MARKETS, or run with --debug."
                    )
                misses += 1
                end_t -= chunk
                continue
            first = False
            new = [r for r in rows if r["t"] not in got]
            for r in rows:
                got[r["t"]] = r
            if progress:
                progress(min(len(got), count), count)
            if not new:
                misses += 1
                end_t -= chunk
            else:
                misses = 0
                oldest = min(r["t"] for r in rows)
                end_t = oldest if oldest < end_t else end_t - chunk
            await asyncio.sleep(0.05)  # be gentle with the server
        return sorted(got.values(), key=lambda r: r["t"])[-count:]

    async def watch_many(self, assets, period, stop, view):
        """
        Live dashboard for one or more markets. `assets` = market dicts (symbol, payout, ...),
        `stop` = asyncio.Event, `view` = LiveView that draws the table. Runs until stop is set.
        Returns the final state.
        """
        cb = CandleBuilder(period)
        state = {
            it["symbol"]: {"price": None, "dir": " ", "cur": None, "ts": 0.0, "mono": 0.0,
                           "ticks": 0, "last": " ", "closed": []}
            for it in assets
        }

        def on_close(a, c):
            state[a]["closed"].append(c)
            state[a]["last"] = candle_icon(c)

        def on_tick(a, ts, price):
            s = state.get(a)
            if s is None:
                return
            if s["price"] is not None and price != s["price"]:
                s["dir"] = "\u25B2" if price > s["price"] else "\u25BC"
            s.update(price=price, ts=ts, mono=time.monotonic())
            s["ticks"] += 1
            s["cur"] = cb.add(a, ts, price, on_close)

        self.tick_cb = on_tick
        try:
            for it in assets:
                await self.subscribe(it["symbol"], period)
                await asyncio.sleep(0.02)
            while not stop.is_set():
                view.draw(state)
                try:
                    await asyncio.wait_for(stop.wait(), view.interval)
                except asyncio.TimeoutError:
                    pass
        finally:
            self.tick_cb = None
        return state


# =====================================================================
#  module: darkmatter/connection/session_store.py  -  Saved session (session.json) and the Chrome finder.
# =====================================================================
def save_session(ssid, cookies):
    with open(SESSION_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {"ssid": ssid, "cookies": cookies, "extra": EXTRA, "saved": int(time.time())}, f
        )


def load_session():
    try:
        with open(SESSION_FILE, encoding="utf-8") as f:
            d = json.load(f)
        EXTRA.update(d.get("extra", {}))
        return d.get("ssid"), d.get("cookies", {})
    except Exception:
        return None, {}


def _unused_find_chrome():
    candidates = [
        os.environ.get("CHROME_PATH"),
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        "/usr/bin/google-chrome",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    ]
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return None


# =====================================================================
#  module: darkmatter/connection/direct_mode.py  -  Direct mode connection (saved session / SSID, no browser window).
# =====================================================================
# =====================================================================
#  Direct mode
# =====================================================================
async def direct_connect(args):
    saved_ssid, cookies = load_session()
    ssid = args.ssid.strip().strip('"') if args.ssid else saved_ssid
    if not ssid:
        raise QuotexError(
            "No SSID available. Run once WITHOUT --direct (bridge mode saves the session), "
            "or pass --ssid."
        )
    if args.ssid:
        cookies = cookies if ssid == saved_ssid else {}
    ws = QuotexWS(ssid, cookies, demo=not args.live, debug=args.debug)
    await ws.connect()
    return ws


# =====================================================================
#  module: darkmatter/flows/timeframes.py  -  Timeframe list and the other shared flow constants.
# =====================================================================
# =====================================================================
#  Menus and flows
# =====================================================================
TIMEFRAMES = [(5, "5s"), (10, "10s"), (15, "15s"), (30, "30s"), (60, "1m"), (120, "2m"), (180, "3m"),
              (300, "5m"), (600, "10m"), (900, "15m"), (1800, "30m"), (3600, "1h"), (14400, "4h")]
TF_SECONDS = {s for s, _ in TIMEFRAMES}
MAX_CANDLES = 5000
BACK_WORDS = ("q", "quit", "cancel", "back", "b")
NO_LIST = "No instrument list captured yet. Restart the bot (it reloads the page)."


def tf_label(secs):
    return next((label for s, label in TIMEFRAMES if s == secs), f"{secs}s")


# =====================================================================
#  module: darkmatter/ui/banner.py  -  Screen titles, the banner (logo box + system information) and the status line under it.
# =====================================================================
def screen_title(text, subtitle=None, big=False):
    """Header box for a screen: compact by default, big colour font with --big-menu."""
    begin_screen()
    put()
    box = Box()
    art = render_big(text) if big else []
    if art and max(len(strip_ansi(x)) for x in art) <= box.inner:
        box.rows(art)
    else:
        box.row(f"{ACCENT}\u2593\u2593\u2592\u2591{R} {TITLE}{text}{R} {DIM}\u2591\u2592\u2593\u2593{R}"
                + (f"   {DIM}{subtitle}{R}" if subtitle else ""))
    box.show()


#  Banner (logo box + system information)
# ---------------------------------------------------------------------
UI_CTX = {}   # session info shown in the banner; set once by main()


def set_ui_ctx(ctx):
    UI_CTX.clear()
    UI_CTX.update(ctx)


def _stacked_logo(width):
    """The real DARKMATTER logo letters, stacked in 2+ rows when the terminal is too narrow for one row."""
    for groups in (("DARK", "MATTER"), ("DARK", "MAT", "TER"), ("DA", "RK", "MA", "TT", "ER")):
        parts = [render_big(g) for g in groups]
        if all(p and max(vlen(ln) for ln in p) <= width for p in parts):
            return [ln for p in parts for ln in p]
    return []


def banner_lines(ctx):
    """Logo box + system information + connection state, as ready-to-print frame lines."""
    box = Box()
    if box.w >= FULL_W:
        for line in BANNER_LINES:
            box.row(line)
    else:
        logo = _stacked_logo(box.inner)
        if logo:
            for ln in logo:
                box.row(ln, "^")
        else:
            box.row(f"{TITLE}D A R K M A T T E R{R}", "^")
    box.rule()
    box.row(f"{ACCENT}QUOTEX DATA ANALYSIS AI{R}", "^")
    box.rule("SYSTEM")
    sysname = f"{platform.system()} {platform.release()}".strip()
    cells = [
        kv("VERSION", f"v{VERSION}"), kv("MODE", ctx["mode_label"]), kv("PYTHON", platform.python_version()),
        kv("BUILD", BUILD), kv("BROKER", BASE.split("//", 1)[1]), kv("SYSTEM", sysname),
        kv("STARTED", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ctx["started"]))),
        kv("ACCOUNT", ctx["account"]), kv("DEBUG", "ON" if ctx["debug"] else "OFF"),
    ]
    cw = box.inner // 3
    for i in range(0, len(cells), 3):
        row = "".join(pad(clip_ansi(c, cw - 2), cw) for c in cells[i:i + 3])
        box.row(row)
    if LIVEVIEW.get("url"):
        box.row(kv("LIVE VIEW", LIVEVIEW["url"]))
    box.rule()
    state = ctx.get("state")
    if state == "connected":
        box.row(f"{dot(OK)} {OK}Successfully connected{R}", "^")
    elif state == "failed":
        box.row(f"{dot(BAD)} {BAD}Connection failed{R}", "^")
    else:
        box.row(f"{dot(WARN)} {WARN}Connecting to Quotex...{R}", "^")
    if box.w < FULL_W and not logo:
        box.rule()
        box.row(f"{WARN}Widen the terminal to {FULL_W + 1}+ columns to see the full logo.{R}")
    return box.end()


def banner_height():
    return len(banner_lines(UI_CTX)) if UI_CTX else 0


_BANNER_STATE = [None]


def begin_screen():
    """Start every screen the same way: wipe the old one, then draw the banner at the top.
    Without a clearing terminal (--debug, pipes) the banner is only repeated when its state changes."""
    clear_screen()
    if not UI_CTX:
        return
    state = UI_CTX.get("state")
    if clearing() or _BANNER_STATE[0] != state:
        _BANNER_STATE[0] = state
        for ln in banner_lines(UI_CTX):
            put(ln)
        flush()


def print_banner(ctx):
    for ln in banner_lines(ctx):
        put(ln)
    flush()


def status_line(ws, ctx):
    age = time.monotonic() - ws.last_rx if ws.last_rx else None
    if age is None:
        link = f"{dot(WARN)} {WARN}WAITING{R}"
    elif age < 30:
        link = f"{dot(OK)} {OK}ONLINE{R}"
    else:
        link = f"{dot(BAD)} {BAD}NO DATA {int(age)}s{R}"
    parts = [link, f"{TEXT}{ctx['mode_label']}{R}"]
    if ws.instruments:
        live, otc = market_items(ws.instruments)
        total = len(live) + len(otc)
        opened = sum(1 for x in live + otc if x["open"])
        parts.append(f"{TEXT}{total} markets{R} {DIM}({opened} open){R}")
    return f"  {DIM}\u00b7{R}  ".join(parts)


# ---------------------------------------------------------------------


# =====================================================================
#  module: darkmatter/ui/console_io.py  -  Non-blocking input, the pause prompt and Windows ANSI enabling.
# =====================================================================
#  Input without blocking exit (daemon thread: Ctrl+C never hangs on a pending input())
# ---------------------------------------------------------------------
async def ainput(prompt=""):
    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    def deliver(setter, value):
        if not fut.done():
            setter(value)

    def worker():
        try:
            if not USE_COLOR:
                text = input(strip_ansi(prompt))
            else:
                text = input(prompt)
        except BaseException as e:  # EOFError, KeyboardInterrupt in the thread, ...
            loop.call_soon_threadsafe(deliver, fut.set_exception, e)
        else:
            loop.call_soon_threadsafe(deliver, fut.set_result, text)

    threading.Thread(target=worker, daemon=True, name="ainput").start()
    return (await fut).strip()


async def pause(text="Press Enter to go back"):
    """Hold the current screen until Enter, so results stay visible before the next screen clears it."""
    put()
    await ainput(ask_prompt(text))


def enable_ansi():
    """Turn on ANSI / VT processing for the Windows console (no shell spawn)."""
    if sys.platform != "win32":
        return
    try:
        import ctypes

        k32 = ctypes.windll.kernel32
        for std in (-11, -12):  # stdout, stderr
            h = k32.GetStdHandle(std)
            mode = ctypes.c_uint32()
            if k32.GetConsoleMode(h, ctypes.byref(mode)):
                k32.SetConsoleMode(h, mode.value | 0x0004)
    except Exception:
        os.system("")


# =====================================================================
#  module: darkmatter/ui/history_view.py  -  Prices, balance, history table and the live dashboard rendering.
# =====================================================================
#  Prices, balance, history, live dashboard
# ---------------------------------------------------------------------
def fmt_price(p):
    if p is None:
        return "-"
    return ("%.6f" % p).rstrip("0").rstrip(".")


def fmt_money(v):
    return f"{v:,.2f}" if isinstance(v, (int, float)) and not isinstance(v, bool) else "-"


def print_balance(b, age):
    box = Box()
    put()
    box.top_split("DEMO ACCOUNT", "LIVE ACCOUNT")
    box.cols("", "")
    box.cols(f"{TITLE}{fmt_money(b['demo'])}{R}", f"{TITLE}{fmt_money(b['live'])}{R}")
    box.cols("", "")
    box.rule_split("close")
    box.row(f"{DIM}updated {age}s ago   \u2502   values as reported by Quotex for this session{R}")
    box.show()


def price_decimals(candles):
    d = 0
    for c in candles:
        for k in ("o", "h", "l", "c"):
            s = ("%.8f" % c[k]).rstrip("0")
            if "." in s:
                d = max(d, len(s.split(".")[1]))
    return d


def chg_cell(c):
    pct = (c["c"] - c["o"]) / c["o"] * 100 if c["o"] else 0.0
    col = OK if pct > 0 else BAD if pct < 0 else DIM
    return f"{col}{pct:+.3f}%{R}"


def print_history(item, candles, tf, rows, error=None):
    box = Box(item["symbol"])
    box.row(f"{TITLE}{item['name']}{R}   {kv('PAYOUT', fmt_payout(item['payout']), 6)}   "
            f"{kv('TIMEFRAME', tf_label(tf), 9)}   {kv('CANDLES', len(candles), 7)}")
    put()
    if error:
        box.rule()
        for ln in wrap_text(error, box.inner):
            box.row(f"{BAD}{ln}{R}")
        box.show()
        return
    if not candles:
        box.rule()
        box.row(f"{WARN}No candles returned (market closed, or no data).{R}")
        box.show()
        return
    dec = price_decimals(candles)
    shown = candles[-rows:]
    fm = lambda v: f"{v:.{dec}f}"
    w = {k: max(len(h), max(len(fm(c[k])) for c in shown)) for k, h in
         (("o", "OPEN"), ("h", "HIGH"), ("l", "LOW"), ("c", "CLOSE"))}
    cols = [("", 2, "<"), ("TIME", 16, "<"), ("OPEN", w["o"], ">"), ("HIGH", w["h"], ">"),
            ("LOW", w["l"], ">"), ("CLOSE", w["c"], ">"), ("CHG", 9, ">")]
    body = [[candle_icon(c), f"{TEXT}{fmt_dt(c['t'])}{R}", f"{TEXT}{fm(c['o'])}{R}",
             f"{OK}{fm(c['h'])}{R}", f"{BAD}{fm(c['l'])}{R}", f"{TEXT}{fm(c['c'])}{R}", chg_cell(c)]
            for c in shown]
    head, lines = table_lines(cols, body, gap=2)
    box.rule()
    box.row(head)
    box.rows(lines)
    if len(candles) > rows:
        box.row(f"{DIM}... showing the last {rows} of {len(candles)} candles{R}")
    icons = [candle_icon(c) for c in candles]
    n = len(icons)
    up, down, doji = icons.count(ICON_UP), icons.count(ICON_DOWN), icons.count(ICON_DOJI)
    pct = lambda k: f"{k / n * 100:.1f}%"
    box.rule("SUMMARY")
    span = fmt_dt(candles[0]["t"]) + "  \u2192  " + fmt_dt(candles[-1]["t"])
    box.row(kv("RANGE", span, 6))
    box.row(f"{ICON_UP} {OK}up {up}{R} {DIM}({pct(up)}){R}     {ICON_DOWN} {BAD}down {down}{R} "
            f"{DIM}({pct(down)}){R}     {ICON_DOJI} {TEXT}doji {doji}{R} {DIM}({pct(doji)}){R}     "
            f"{DIM}doji = body < {int(DOJI_BODY_RATIO * 100)}% of range{R}")
    strip = icons[-60:]
    per = max(5, min(30, box.inner // 2))
    box.rule(f"LAST {len(strip)} CANDLES  (old \u2192 new)")
    for i in range(0, len(strip), per):
        box.row("".join(strip[i:i + per]))
    box.show()


def payout_for_dash(it):
    return f"{pay_color(it)}{fmt_payout(it['payout'])}{R}"


def render_dashboard(assets, state, period, now_mono, w=None, max_rows=40):
    """Frame lines for the live multi-market table (constant line count, so it can redraw in place)."""
    box = Box("LIVE TICKS", w)
    n = len(assets)
    box.row(f"{dot(OK)} {OK}STREAMING{R}   {DIM}\u2502{R}   {TEXT}{n} market{'s' if n != 1 else ''}{R}   "
            f"{DIM}\u2502{R}   {TEXT}{tf_label(period)} candles{R}   {DIM}\u2502{R}   "
            f"press {chip('Enter')} to stop")
    full = box.inner >= 91
    cols = [("PAIR", 16, "<"), ("PAY", 6, ">"), ("PRICE", 12, ">"), ("", 1, "<")]
    if full:
        cols += [("OPEN", 11, ">"), ("HIGH", 11, ">"), ("LOW", 11, ">")]
    cols += [("LEFT", 5, ">"), ("LAST", 4, "<")]
    body = []
    for it in assets[:max_rows]:
        s = state[it["symbol"]]
        head = [f"{TEXT}{it['symbol']}{R}", payout_for_dash(it)]
        if s["price"] is None:
            body.append(head + [f"{DIM}waiting\u2026{R}", ""] + [""] * (3 if full else 0) + ["", ""])
            continue
        c = s["cur"]
        left = int(period - ((s["ts"] + (now_mono - s["mono"])) % period))
        arrow = f"{OK}\u25b2{R}" if s["dir"] == "\u25B2" else f"{BAD}\u25bc{R}" if s["dir"] == "\u25BC" else " "
        row = head + [f"{TITLE}{fmt_price(s['price'])}{R}", arrow]
        if full:
            row += [f"{TEXT}{fmt_price(c['o'])}{R}", f"{OK}{fmt_price(c['h'])}{R}", f"{BAD}{fmt_price(c['l'])}{R}"]
        row += [f"{ACCENT}{left}s{R}", s["last"] if s["last"].strip() else f"{DIM}\u00b7{R}"]
        body.append(row)
    head, lines = table_lines(cols, body, gap=2)
    box.rule()
    box.row(head)
    box.rows(lines)
    if n > max_rows:
        box.rule()
        box.row(f"{WARN}+{n - max_rows} more market(s) not shown (terminal too short) - they are still recorded{R}")
    return box.end()


class LiveView:
    """Redraws the dashboard in place (cursor-up), only when something changed."""

    def __init__(self, assets, period):
        self.assets, self.period = assets, period
        self.tty = sys.stdout.isatty()
        self.interval = 0.25 if self.tty else 2.0
        self.prev = 0
        self._last = None

    def draw(self, state, extra_up=0):
        lines = render_dashboard(self.assets, state, self.period, time.monotonic(),
                                 max_rows=max(3, term_height() - banner_height() - 10))  # the banner stays on screen
        if lines == self._last and not extra_up:
            return
        self._last = lines
        if self.tty:
            buf = []
            up = self.prev + extra_up
            if up:
                buf.append(f"\x1b[{up}A")
            buf += [f"\r\x1b[2K{ln}\n" for ln in lines]
            if up > len(lines):
                buf.append("\x1b[J")
            put("".join(buf), end="")
        else:
            put("\n".join(lines) + "\n")
        flush()
        self.prev = len(lines)


def print_live_summary(assets, state):
    box = Box("SESSION SUMMARY")
    cols = [("PAIR", 16, "<"), ("TICKS", 8, ">"), ("CANDLES", 8, ">"), ("LAST 30 CANDLES", 60, "<")]
    body = []
    for it in assets:
        s = state[it["symbol"]]
        body.append([f"{TEXT}{it['symbol']}{R}", f"{TEXT}{s['ticks']}{R}", f"{TEXT}{len(s['closed'])}{R}",
                     "".join(candle_icon(c) for c in s["closed"][-30:])])
    head, lines = table_lines(cols, body, gap=2)
    box.row(head)
    box.rows(lines)
    put()
    box.show()


# =====================================================================
#  module: darkmatter/flows/basic_flows.py  -  Menus and flows: timeframe / count prompts, market, balance, history, live and debug flows.
# =====================================================================
def parse_timeframe(text, default):
    """'' -> default, '5' -> 5th item of the list (1m), '10s' / '5m' / '1h' / '300' -> exact seconds."""
    t = text.strip().lower()
    if not t:
        return default
    m = re.fullmatch(r"([0-9]+)\s*([smh]?)", t)
    if not m:
        return None
    num, unit = int(m.group(1)), m.group(2)
    if unit:
        secs = num * {"s": 1, "m": 60, "h": 3600}[unit]
    elif 1 <= num <= len(TIMEFRAMES):
        return TIMEFRAMES[num - 1][0]
    else:
        secs = num
    return secs if secs in TF_SECONDS else None


def parse_count(text, default):
    t = text.strip()
    if not t:
        return default
    if not (t.isascii() and t.isdecimal()):
        return None
    n = int(t)
    return n if 1 <= n <= MAX_CANDLES else None


def print_timeframe_box(default):
    box = Box("HISTORY SETUP")
    cell_w = 13
    per_row = max(1, min(7, box.inner // cell_w))
    cells = []
    for i, (secs, label) in enumerate(TIMEFRAMES, 1):
        col = OK if secs == default else TEXT
        cells.append(pad(f"{chip(f'{i:>2}')} {col}{label}{R}", cell_w))
    for i in range(0, len(cells), per_row):
        box.row("".join(cells[i:i + per_row]))
    box.rule()
    box.row(f"{DIM}A number picks from the list; add a unit for exact values, e.g. {R}"
            f"{chip('10s')} {chip('5m')} {chip('1h')}    {OK}green{R}{DIM} = current default{R}")
    box.row(f"{DIM}Candles per market: 1 - {MAX_CANDLES}.   {R}{chip('q')} {DIM}cancels{R}")
    put()
    box.show()


async def ask_timeframe(default, picked):
    notice = None
    while True:
        begin_screen()
        put()
        print_selection(picked)
        print_timeframe_box(default)
        if notice:
            warn(notice)
        text = await ainput(ask_prompt(f"Timeframe [{tf_label(default)}]"))
        if text.lower() in BACK_WORDS:
            return None
        tf = parse_timeframe(text, default)
        if tf:
            return tf
        notice = "Not a supported timeframe. Supported: " + ", ".join(label for _, label in TIMEFRAMES)


async def ask_count(default, picked, tf):
    notice = None
    while True:
        begin_screen()
        put()
        print_selection(picked)
        box = Box("HISTORY SETUP")
        box.row(f"{kv('TIMEFRAME', tf_label(tf), 9)}    {DIM}Candles per market: 1 - {MAX_CANDLES}.{R}   "
                f"{chip('q')} {DIM}cancels{R}")
        put()
        box.show()
        if notice:
            warn(notice)
        text = await ainput(ask_prompt(f"Candles [{default}]"))
        if text.lower() in BACK_WORDS:
            return None
        n = parse_count(text, default)
        if n:
            return n
        notice = f"Please enter a whole number between 1 and {MAX_CANDLES}."


async def load_market(ws):
    """(live, otc) or None. A background poll keeps the list fresh, so this is normally instant."""
    if not ws.instruments:
        info("Loading market list...")
    await ws.fresh_instruments(max_age=15.0, timeout=5.0)
    if not ws.instruments:
        return None
    return market_items(ws.instruments)


async def show_market(ws, big):
    screen_title("MARKETS", "live + OTC market list with payouts", big)
    data = await load_market(ws)
    if data is None:
        warn(NO_LIST)
    else:
        print_market(ws, *data)
    await pause()


async def pick_markets(ws, title, subtitle, big):
    """Show the market list and let the user pick one or more markets. The screen is redrawn after
    every input, so nothing piles up. The list is frozen while picking, so the numbers never move."""
    screen_title(title, subtitle, big)
    data = await load_market(ws)
    if data is None:
        warn(NO_LIST)
        await pause()
        return None
    live, otc = data
    by_no = {x["no"]: x for x in live + otc}
    by_sym = {x["symbol"].lower(): x for x in live + otc}

    selected, notice = [], None
    while True:
        screen_title(title, subtitle, big)
        print_market(ws, live, otc)
        print_selection_help()
        if selected:
            print_selection(selected)
        if notice:
            warn(notice)
            notice = None
        line = (await ainput(ask_prompt(f"Select [{len(selected)}]"))).strip()
        low = line.lower()
        if not line:
            if selected:
                return selected
            notice = "Nothing selected yet. Type a number or symbol, or 'q' to cancel."
            continue
        if low in BACK_WORDS:
            return None
        if low == "clear":
            selected = []
            notice = "Selection cleared."
            continue
        picked, errors = parse_selection(line, by_no, by_sym)
        for it in picked:
            if all(it["symbol"] != s["symbol"] for s in selected):
                selected.append(it)
        if errors:
            notice = "Not understood: " + ", ".join(errors)


async def balance_flow(ws, big):
    screen_title("BALANCE", "demo / live account balance", big)
    b = await ws.get_balance()
    age = max(0, int(time.time() - ws.balance_ts)) if ws.balance_ts else 0
    print_balance(b, age)
    await pause()


async def history_flow(ws, picked, tf, n):
    """Markets are fetched ONE AT A TIME: Quotex answers history for the market that was subscribed
    most recently, so parallel requests make most markets time out."""
    results = []
    total = len(picked) * n
    begin_screen()
    put()
    async with Progress("Fetching candles", total=total) as bar:
        base = 0
        for i, it in enumerate(picked, 1):
            bar.label = f"Fetching {i}/{len(picked)}  {it['symbol']}"

            def prog(d, t, base=base):
                bar.set(base + d)

            try:
                candles = await ws.get_candles(it["symbol"], tf, n, progress=prog)
                results.append((it, candles, None))
            except QuotexError as e:
                results.append((it, [], str(e)))
            except (KeyboardInterrupt, asyncio.CancelledError):
                raise
            except Exception as e:  # one broken market must not stop the others
                results.append((it, [], f"{type(e).__name__}: {e}"))
            base += n
            bar.set(base)
    rows = 20 if len(picked) == 1 else 8
    for it, candles, err in results:
        put()
        print_history(it, candles, tf, rows, err)
    if any(c for _, c, _ in results):
        put()
        answer = await ainput(ask_prompt("Save to CSV? [y/N]"))
        if answer.lower() in ("y", "yes"):
            for it, candles, _ in results:
                if not candles:
                    continue
                fn = f"{re.sub(r'[^A-Za-z0-9._-]+', '_', it['symbol'])}_{tf}s.csv"
                try:
                    with open(fn, "w", encoding="utf-8", newline="") as f:
                        w = csv.writer(f)
                        w.writerow(["time", "open", "high", "low", "close", "ticks", "color"])
                        for c in candles:
                            w.writerow([fmt_dt(c["t"]), c["o"], c["h"], c["l"], c["c"],
                                        c.get("ticks") or "", candle_color(c)])
                    good(f"Saved {fn}")
                except OSError as e:
                    warn(f"Could not save {fn}: {e}")
    await pause()


async def live_flow(ws, picked, tf=60):
    stop = asyncio.Event()

    async def wait_enter():
        try:
            await ainput("")
        except (EOFError, KeyboardInterrupt):
            pass
        stop.set()

    begin_screen()
    waiter = asyncio.create_task(wait_enter())
    view = LiveView(picked, tf)
    put()
    finished = False
    try:
        state = await ws.watch_many(picked, tf, stop, view)
        view.draw(state, extra_up=1 if view.tty else 0)  # the Enter key moved the cursor one line down
        put()
        print_live_summary(picked, state)
        finished = True
    except BaseException as e:
        if isinstance(e, (KeyboardInterrupt, asyncio.CancelledError)):
            raise
        fail(f"{type(e).__name__}: {e}")
        say("Press Enter to continue...")
    finally:
        if not waiter.done():
            await waiter  # consumes the Enter press so it cannot leak into the menu
    if finished:
        await pause()


async def debug_flow(ws, big):
    screen_title("DEBUG", "event log and debug samples", big)
    box = Box("CONNECTION")
    age = f"{time.monotonic() - ws.last_rx:.1f}s ago" if ws.last_rx else "never"
    box.row(f"{kv('LAST DATA', age, 10)}    {kv('EVENTS', len(ws.seen_events), 7)}    "
            f"{kv('MARKETS', len(ws.instruments), 8)}")
    box.rule("EVENTS SEEN")
    for ln in wrap_plain([f"{TEXT}{e}{R}" for e in ws.seen_events] or [f"{DIM}none{R}"], box.inner, "  "):
        box.row(ln)
    box.rule("SAMPLES")
    box.row(kv("SAVED", ws.save_samples(), 6))
    put()
    box.show()
    await pause()


# =====================================================================
#  module: darkmatter/notify/charts_core.py
# =====================================================================
# =====================================================================
#  CHARTS  -  pictures that go with the Telegram messages (drawn as SVG, rendered by Chromium)
#  No matplotlib / mplfinance / pandas / numpy needed.
# =====================================================================

CH_GREEN, CH_RED, CH_AMBER, CH_CYAN, CH_PURPLE = "#00e5a8", "#ff3d5a", "#f5b942", "#22d3ee", "#8b5cf6"
CH_BG, CH_SIDE, CH_PANEL, CH_CARD, CH_BORDER = "#0b0b17", "#04050a", "#070a10", "#0a0c14", "#141826"
CH_TEXT, CH_GRAY, CH_GRAY2, CH_FOOT, CH_DIM = "#eef1f8", "#6f768d", "#a3a9bf", "#4a5068", "#394058"
CH_SHOW = 100               # candles drawn in a trade chart
CH_START_BAL = 1000.0       # the session summary replays the signals on a virtual account ...
CH_STAKE_PCT = 2.0          # ... that risks this % of its balance on every signal
CH_ENGINE_TAG = "v2.0"
CH_PHOTO_CAPTION = 1024     # Telegram's limit for a photo caption (UTF-16 units)

CH_FONT_STACK = ("'DejaVu Sans','Bitstream Vera Sans',Verdana,'Noto Sans',Roboto,'Droid Sans',"
                 "'Liberation Sans',Arial,Helvetica,sans-serif")
# advance widths (1/1000 em) of DejaVu Sans / DejaVu Sans Bold: the layout is computed with these and every
# text is forced to exactly that width (SVG textLength), so the picture looks the same with any installed font
_FW_CHARS = ' !"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~©·\u200a•→≥▲▼●✓✕—–…'
_FW_R = [318, 401, 460, 838, 636, 950, 780, 275, 390, 390, 500, 838, 318, 361, 318, 337, 636, 636, 636, 636, 636, 636, 636, 636, 636, 636, 337, 337, 838, 838, 838, 531, 1000, 684, 686, 698, 770, 632, 575, 775, 752, 295, 295, 656, 557, 863, 748, 787, 603, 787, 695, 635, 611, 732, 684, 989, 685, 611, 685, 390, 337, 390, 838, 500, 500, 613, 635, 550, 635, 615, 352, 635, 634, 278, 278, 579, 278, 974, 634, 612, 635, 635, 411, 521, 392, 634, 592, 818, 592, 592, 525, 636, 337, 636, 838, 1000, 318, 100, 590, 838, 838, 769, 769, 873, 838, 838, 1000, 500, 1000]
_FW_B = [348, 456, 521, 838, 696, 1002, 872, 306, 457, 457, 523, 838, 380, 415, 380, 365, 696, 696, 696, 696, 696, 696, 696, 696, 696, 696, 400, 400, 838, 838, 838, 580, 1000, 774, 762, 734, 830, 683, 683, 821, 837, 372, 372, 775, 637, 995, 837, 850, 733, 850, 770, 720, 682, 812, 774, 1103, 771, 724, 725, 457, 365, 457, 838, 500, 500, 675, 716, 593, 716, 678, 435, 716, 712, 343, 343, 665, 343, 1042, 712, 687, 716, 716, 493, 595, 478, 712, 652, 924, 645, 652, 582, 712, 365, 712, 838, 1000, 380, 100, 639, 838, 838, 769, 769, 873, 838, 838, 1000, 500, 1000]
_FW = {"normal": dict(zip(_FW_CHARS, _FW_R)), "bold": dict(zip(_FW_CHARS, _FW_B))}


def text_width(s, px, w="normal"):
    tbl = _FW["bold" if w == "bold" else "normal"]
    dflt = 700 if w == "bold" else 620
    return sum(tbl.get(ch, dflt) for ch in str(s)) * px / 1000.0


def _rgb(h):
    h = h.lstrip("#")
    return tuple(int(h[i:i + 2], 16) / 255.0 for i in (0, 2, 4))


def _rgba(h, a=1.0):
    return _rgb(h) + (a,)


def _mix(c1, c2, t):
    a, b = _rgb(c1), _rgb(c2)
    return tuple(a[i] + (b[i] - a[i]) * t for i in range(3))


def _css(c):
    """'#rrggbb' / 'none' / (r, g, b) / (r, g, b, a) with floats 0..1  ->  an SVG colour string."""
    if c is None:
        return "none"
    if isinstance(c, str):
        return c
    r, g, b = (max(0, min(255, int(round(float(v) * 255)))) for v in c[:3])
    if len(c) >= 4 and float(c[3]) < 0.9995:
        return f"rgba({r},{g},{b},{max(0.0, float(c[3])):.4f})"
    return f"#{r:02x}{g:02x}{b:02x}"


def _svgn(v):
    return f"{float(v):.2f}".rstrip("0").rstrip(".") if abs(float(v)) < 1e9 else "0"


def tz_short(minutes):
    """360 -> UTC+6, 330 -> UTC+5:30, -180 -> UTC-3"""
    sign = "-" if minutes < 0 else "+"
    m = abs(int(minutes))
    return f"UTC{sign}{m // 60}" + (f":{m % 60:02d}" if m % 60 else "")


def spaced(s, gap="\u200a"):
    """Letter-spaced text (the small caps headings of the dashboards)."""
    return " ".join(gap.join(w) for w in s.split(" "))


class ChartEngine:
    """SVG -> PNG with a headless Chromium (reuses the bridge's browser when it is headless, otherwise it
    starts a private headless one the first time a picture is needed). Methods without the leading
    underscore are called from worker threads; the work itself runs on the bot's event loop."""

    def __init__(self):
        self.loop = None
        self.cdp = None
        self.proc = None
        self.profile = None
        self.ctx_id = None
        self.shared = False
        self._lock = None
        self._render_lock = None

    def bind(self, loop):
        self.loop = loop

    def _run(self, coro, timeout):
        if self.loop is None or not self.loop.is_running():
            coro.close()
            raise RuntimeError("the chart engine has no running event loop")
        try:
            if asyncio.get_running_loop() is self.loop:
                coro.close()
                raise RuntimeError("chart engine called from the event loop thread")
        except RuntimeError as e:
            if "chart engine called" in str(e):
                raise
        fut = asyncio.run_coroutine_threadsafe(coro, self.loop)
        try:
            return fut.result(timeout)
        except concurrent.futures.TimeoutError:
            fut.cancel()
            raise RuntimeError(f"the chart engine did not answer within {timeout}s") from None

    def ensure(self, timeout=120):
        """-> (True, None) or (False, reason)"""
        try:
            self._run(self._ensure(), timeout)
            return True, None
        except BaseException as e:
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
            return False, f"{type(e).__name__}: {e}"

    def render(self, svg, w, h, timeout=90):
        return self._run(self._render(svg, w, h), timeout)

    async def _ensure(self):
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self.cdp is not None and not self.cdp.closed:
                return
            self.cdp, self.ctx_id = None, None
            br = BRIDGE_REF.get("bridge")
            if br is not None and br.cdp is not None and not br.cdp.closed and br.headless:
                self.cdp, self.shared = br.cdp, True
            else:
                exe = find_chromium()
                if not exe:
                    raise QuotexError("Chromium was not found (needed for the chart pictures).")
                if self.proc is not None:
                    self.proc.stop()
                self.profile = tempfile.mkdtemp(prefix="qx_charts_")
                port = free_port()
                self.proc = ChromiumProcess(exe, port, self.profile, True, "about:blank",
                                            log_path=os.path.join(DATA_DIR, "qx_chromium_charts.log"))
                await self.proc.start()
                url = await browser_ws_url(port, 5.0)
                if not url:
                    raise QuotexError("The chart Chromium started, but its DevTools endpoint did not answer.")
                cdp = Cdp(url)
                await cdp.connect()
                self.cdp, self.shared = cdp, False
            r = await self.cdp.call("Target.createBrowserContext", {})
            self.ctx_id = r["browserContextId"]

    async def _render(self, svg, w, h):
        await self._ensure()
        if self._render_lock is None:
            self._render_lock = asyncio.Lock()
        async with self._render_lock:
            cdp = self.cdp
            if cdp is None or cdp.closed:
                raise QuotexError("The chart browser is not running.")
            tid = (await cdp.call("Target.createTarget", {"url": "about:blank", "browserContextId": self.ctx_id,
                                                          "width": int(w), "height": int(h)}))["targetId"]
            try:
                sid = (await cdp.call("Target.attachToTarget", {"targetId": tid, "flatten": True}))["sessionId"]
                await cdp.call("Page.enable", None, sid)
                await cdp.call("Emulation.setDeviceMetricsOverride",
                               {"width": int(w), "height": int(h), "deviceScaleFactor": 1, "mobile": False}, sid)
                url = "data:image/svg+xml;base64," + base64.b64encode(svg.encode("utf-8")).decode("ascii")
                await cdp.call("Page.navigate", {"url": url}, sid)
                for _ in range(400):                       # wait until the SVG document is there and complete
                    r = await cdp.call("Runtime.evaluate", {
                        "expression": "document.readyState + '|' + (document.documentElement ? document.documentElement.localName : '')",
                        "returnByValue": True}, sid, 15)
                    if (r.get("result") or {}).get("value") == "complete|svg":
                        break
                    await asyncio.sleep(0.05)
                else:
                    raise QuotexError("The chart page did not finish loading.")
                shot = await cdp.call("Page.captureScreenshot", {
                    "format": "png", "fromSurface": True,
                    "clip": {"x": 0, "y": 0, "width": int(w), "height": int(h), "scale": 1}}, sid, 60)
                png = base64.b64decode(shot["data"])
                if not png.startswith(b"\x89PNG"):
                    raise QuotexError("Chromium returned no picture.")
                return png
            finally:
                try:
                    await cdp.call("Target.closeTarget", {"targetId": tid}, None, 8)
                except Exception:
                    pass

    async def aclose(self):
        cdp, proc, prof = self.cdp, self.proc, self.profile
        self.cdp, self.proc, self.profile = None, None, None
        if cdp is not None and not cdp.closed:
            if self.ctx_id:
                try:
                    await cdp.call("Target.disposeBrowserContext", {"browserContextId": self.ctx_id}, None, 5)
                except Exception:
                    pass
            if not self.shared:
                try:
                    await asyncio.wait_for(cdp.call("Browser.close"), 3)
                except Exception:
                    pass
                await cdp.close()
        self.ctx_id = None
        if proc is not None:
            proc.stop()
        if prof:
            shutil.rmtree(prof, ignore_errors=True)


CHARTS = ChartEngine()


def charts_backend():
    """-> (info, None) when pictures can be drawn, (None, reason) when not. Called from a worker thread."""
    ok, err = CHARTS.ensure()
    return ({"engine": "chromium"} if ok else None), err


class _Layer:
    def __init__(self, z):
        self.z = z


class _Cv:
    """A pixel canvas that writes SVG: x to the right, y downwards, 1 unit = 1 pixel. Fonts are given in pixels.
    Drawing order = layer (bg < panel < ax), then z, then call order. `ax=cv.bg` draws on the background layer;
    `clip=(x, y, w, h)` clips one shape to a rectangle."""

    def __init__(self, W, H, base=CH_BG, glows=()):
        self.W, self.H, self.base = W, H, base
        self.bg, self.panel, self.ax = _Layer(0), _Layer(1), _Layer(10)
        self._items = []
        self._defs = []
        self._clips = {}
        for gi, (cx, cy, r, col, a) in enumerate(glows):          # soft radial glows
            stops = "".join(
                f'<stop offset="{t / 10:.1f}" stop-color="{_css(col)}" stop-opacity="{a * (1 - t / 10) ** 2:.4f}"/>'
                for t in range(11))
            self._defs.append(f'<radialGradient id="g{gi}" cx="0.5" cy="0.5" r="0.5">{stops}</radialGradient>')
            self._add(self.bg, -100, f'<circle cx="{_svgn(cx)}" cy="{_svgn(cy)}" r="{_svgn(r)}" fill="url(#g{gi})"/>')

    # ---------- internals ----------
    def _add(self, layer, z, svg):
        layer = layer or self.ax
        self._items.append((layer.z, z, len(self._items), svg))

    def _clip(self, clip):
        if not clip:
            return ""
        key = tuple(float(v) for v in clip)
        cid = self._clips.get(key)
        if cid is None:
            cid = f"c{len(self._clips)}"
            self._clips[key] = cid
            x, y, w, h = key
            self._defs.append(f'<clipPath id="{cid}"><rect x="{_svgn(x)}" y="{_svgn(y)}" width="{_svgn(w)}" '
                              f'height="{_svgn(h)}"/></clipPath>')
        return f' clip-path="url(#{cid})"'

    @staticmethod
    def _dash(ls, lw):
        """matplotlib style (0, (on, off, ...)) in line-width units -> stroke-dasharray in pixels."""
        if isinstance(ls, (tuple, list)) and len(ls) == 2 and isinstance(ls[1], (tuple, list)):
            return ' stroke-dasharray="' + ",".join(_svgn(max(0.01, v * lw)) for v in ls[1]) + '"'
        return ""

    def _stroke(self, ec, lw, ls="-", cap="butt", join="miter"):
        if ec is None or ec == "none" or not lw:
            return ""
        s = f' stroke="{_css(ec)}" stroke-width="{_svgn(lw)}"'
        if cap != "butt":
            s += f' stroke-linecap="{cap}"'
        if join != "miter":
            s += f' stroke-linejoin="{join}"'
        return s + self._dash(ls, lw)

    # ---------- primitives ----------
    def tw(self, s, px, w="normal"):
        return text_width(s, px, w)

    def T(self, x, y, s, px, color=CH_TEXT, w="normal", ha="left", va="center", z=6, a=1.0, it=False,
          stroke=None, ax=None, rot=0, clip=None):
        s = str(s)
        base = {"center": 0.276, "baseline": 0.0, "top": 0.74, "bottom": -0.21}.get(va, 0.276)
        anchor = {"left": "start", "center": "middle", "right": "end"}.get(ha, "start")
        width = text_width(s, px, w)
        attrs = (f'x="{_svgn(x)}" y="{_svgn(y + base * px)}" font-size="{_svgn(px)}" fill="{_css(color)}" '
                 f'text-anchor="{anchor}"')
        if w == "bold":
            attrs += ' font-weight="700"'
        if it:
            attrs += ' font-style="italic"'
        if a < 1.0:
            attrs += f' fill-opacity="{a:.3f}"'
        if stroke:
            attrs += (f' stroke="{_css(stroke[1])}" stroke-width="{_svgn(stroke[0])}" stroke-linejoin="round" '
                      f'paint-order="stroke"')
        if s.strip() and width > 0:
            attrs += f' textLength="{_svgn(width)}" lengthAdjust="spacingAndGlyphs"'
        if rot:
            attrs += f' transform="rotate({_svgn(-rot)} {_svgn(x)} {_svgn(y)})"'
        self._add(ax, z, f'<text {attrs} xml:space="preserve"{self._clip(clip)}>{_html.escape(s, quote=False)}</text>')

    def R(self, x, y, w, h, fc="none", ec="none", lw=1.0, r=0.0, z=3, ls="-", ax=None, clip=None):
        rr = f' rx="{_svgn(r)}" ry="{_svgn(r)}"' if r else ""
        self._add(ax, z, f'<rect x="{_svgn(x)}" y="{_svgn(y)}" width="{_svgn(max(w, 0))}" height="{_svgn(max(h, 0))}"{rr} '
                         f'fill="{_css(fc)}"{self._stroke(ec, lw, ls)}{self._clip(clip)}/>')

    def L(self, x1, y1, x2, y2, color, lw=1.0, z=4, ls="-", cap="butt", ax=None, clip=None):
        self._add(ax, z, f'<line x1="{_svgn(x1)}" y1="{_svgn(y1)}" x2="{_svgn(x2)}" y2="{_svgn(y2)}"'
                         f'{self._stroke(color, lw, ls, cap)}{self._clip(clip)}/>')

    def PL(self, pts, color, lw=1.0, z=4, ls="-", cap="butt", join="round", ax=None, clip=None):
        """Polyline through the given (x, y) points."""
        pts = list(pts)
        if len(pts) < 2:
            return
        d = " ".join(f"{_svgn(x)},{_svgn(y)}" for x, y in pts)
        self._add(ax, z, f'<polyline points="{d}" fill="none"{self._stroke(color, lw, ls, cap, join)}{self._clip(clip)}/>')

    def poly(self, pts, fc, ec="none", lw=0.0, z=4, ax=None, clip=None):
        pts = list(pts)
        if len(pts) < 3:
            return
        d = " ".join(f"{_svgn(x)},{_svgn(y)}" for x, y in pts)
        self._add(ax, z, f'<polygon points="{d}" fill="{_css(fc)}"{self._stroke(ec, lw, "-", "butt", "round")}'
                         f'{self._clip(clip)}/>')

    def dot(self, x, y, r, fc, ec="none", lw=0.0, z=5, ax=None):
        self._add(ax, z, f'<circle cx="{_svgn(x)}" cy="{_svgn(y)}" r="{_svgn(r)}" fill="{_css(fc)}"{self._stroke(ec, lw)}/>')

    def svg(self):
        body = "".join(it[3] for it in sorted(self._items, key=lambda t: t[:3]))
        defs = f"<defs>{''.join(self._defs)}</defs>" if self._defs else ""
        return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.W}" height="{self.H}" '
                f'viewBox="0 0 {self.W} {self.H}" font-family="{CH_FONT_STACK}" xml:space="preserve">'
                f'{defs}<rect width="{self.W}" height="{self.H}" fill="{_css(self.base)}"/>{body}</svg>')

    def png(self):
        return CHARTS.render(self.svg(), self.W, self.H)

    # ---------- shared pieces ----------
    def tag(self, right, cy, text, fill, tcolor="#000000", px=15, h=22, pad=9, ec="none", z=8):
        """A price tag whose right edge sits at `right`."""
        w = self.tw(text, px, "bold") + 2 * pad
        self.R(right - w, cy - h / 2, w, h, fc=fill, ec=ec, r=4, z=z)
        self.T(right - pad, cy + 0.5, text, px, tcolor, "bold", ha="right", z=z + 1)
        return w

    def pill(self, right, cy, text, px, fc, ec, tcolor, h=37, pad=10, lw=1.6, r=8, w_bold="bold"):
        w = self.tw(text, px, w_bold) + 2 * pad
        self.R(right - w, cy - h / 2, w, h, fc=fc, ec=ec, lw=lw, r=r, z=6)
        self.T(right - w / 2, cy + 0.5, text, px, tcolor, w_bold, ha="center", z=7)
        return w

    def brackets(self, x0, y0, x1, y1, color, ln=16, lw=3.0):
        for (x, y, dx, dy) in ((x0, y0, 1, 1), (x1, y0, -1, 1), (x0, y1, 1, -1), (x1, y1, -1, -1)):
            self.L(x, y, x + dx * ln, y, color, lw, z=9)
            self.L(x, y, x, y + dy * ln, color, lw, z=9)

    def logo(self, cx, cy, s=1.0):
        """The DARKMATTER planet: soft halo, black planet, two tilted rings, two small moons."""
        for k, (rad, a) in enumerate(((55, 0.045), (47, 0.06), (40, 0.075), (33, 0.09))):
            self.dot(cx, cy, rad * s, _rgba(CH_PURPLE, a), z=2)
        rot = math.radians(26.6)

        def ring(a, b, t0, t1, n=120):
            pts = []
            for k in range(n):
                t = t0 + (t1 - t0) * k / (n - 1)
                x, y = a * math.cos(t), b * math.sin(t)
                pts.append((cx + (x * math.cos(rot) + y * math.sin(rot)), cy + (-x * math.sin(rot) + y * math.cos(rot))))
            return pts
        rings = ((56 * s, 21 * s, CH_PURPLE, 2.6 * s), (50 * s, 16 * s, CH_CYAN, 1.4 * s))
        for a, b, col, lw in rings:                       # the back half of the rings
            self.PL(ring(a, b, math.pi, 2 * math.pi), col, lw, z=3, cap="round")
        self.dot(cx, cy, 26 * s, "#000000", CH_PURPLE, 2.6 * s, z=4)
        for a, b, col, lw in rings:                       # the front half passes over the planet
            self.PL(ring(a, b, 0, math.pi), col, lw, z=5, cap="round")
        ex, ey = ring(56 * s, 21 * s, 0, 0.0001, 2)[0]
        self.dot(ex, ey, 5.2 * s, CH_CYAN, z=6)
        ox, oy = ring(56 * s, 21 * s, math.pi, math.pi + 0.0001, 2)[0]
        self.dot(ox, oy, 3.6 * s, CH_AMBER, z=6)

    def wordmark(self, cx, cy, px):
        self.T(cx, cy, "DARKMATTER", px, CH_TEXT, "bold", ha="center", z=6, stroke=(px * 0.22, "#1e1639"))

    def sep(self, y, x0=33, x1=298, color="#111421"):
        self.L(x0, y, x1, y, color, 1.2, z=3)

    def gauge(self, cx, cy, conf, color):
        """Half-ring confidence gauge: flat side down, fills from the left."""
        def ring(r_out, r_in, t_from, t_to, fc, z, n=90):
            ts = [t_from + (t_to - t_from) * k / (n - 1) for k in range(n)]
            outer = [(cx + r_out * math.cos(t), cy - r_out * math.sin(t)) for t in ts]
            inner = [(cx + r_in * math.cos(t), cy - r_in * math.sin(t)) for t in reversed(ts)]
            self.poly(outer + inner, fc, z=z)
        ring(105, 71, math.pi, 0, _rgba(color, 0.10), 2)                # dark tinted frame
        ring(101, 83, math.pi, 0, "#151a28", 3)                         # track
        c = max(0.0, min(100.0, float(conf)))
        if c > 0:
            ring(101, 83, math.pi, math.pi - math.pi * c / 100.0, color, 4)  # value
        ring(83, 72, math.pi, 0, _rgba(color, 0.07), 3)                 # inner shade


# ---------------------------------------------------------------------


# =====================================================================
#  module: darkmatter/signals/darkmatter_engine_v3.py  -  DARKMATTER_ENGINE_V3  (KOMBINER V2 ULTRA)  ->  Python signal engine
# =====================================================================
EMPTY_VALUE = 2147483647.0

# ---- enums (MT4 values) -----------------------------------------------------
PRICE_CLOSE, PRICE_OPEN, PRICE_HIGH, PRICE_LOW, PRICE_MEDIAN, PRICE_TYPICAL, PRICE_WEIGHTED = range(7)
MODE_SMA, MODE_EMA, MODE_SMMA, MODE_LWMA = range(4)
FLOW, REVERSE = 0, 1                      # enum signal
INTRABAR, CLOSEDCANDLE = 0, 1             # enum signaltype
STO_LOWHIGH, STO_CLOSECLOSE = 0, 1
st_atr, st_ste, st_sam, st_nos = range(4)  # enCalcType
pr_close, pr_open, pr_high, pr_low, pr_median, pr_typical, pr_weighted, pr_average, pr_medianb = range(9)


def sinal_buffer(v) -> bool:
    """MQ4: value != 0 && value != EMPTY_VALUE"""
    return v is not None and v != 0 and v != EMPTY_VALUE


# =============================================================================
#  CONFIG  (defaults = defaults of the .mq4)
# =============================================================================
@dataclass
class EngineConfig:
    # --- general ---
    DirectionSignal: int = REVERSE       # REVERSE: red candle -> CALL ; FLOW: inverted
    Intervalo: int = 5                   # candles between signals
    Velas: int = 300                     # back-test window (bars)
    enviar_telegram: bool = True         # only affects LastSignal handling
    period_minutes: int = 1              # Period() of the chart (M1 = 1)
    digits: int = 5                      # Digits (used by candle-size filters)
    warmup: int = 120                    # bars skipped at the old end (indicator warm-up)

    # --- external combiners (only 1..4 take part in the final condition) ---
    COMBINER: bool = True
    COMBINER2: bool = False
    COMBINER3: bool = False
    COMBINER4: bool = False
    SignalType: int = INTRABAR
    SignalType2: int = INTRABAR
    SignalType3: int = INTRABAR
    SignalType4: int = INTRABAR
    IndiBufferCall: int = 4
    IndiBufferPut: int = 5
    IndiBufferCall2: int = 0
    IndiBufferPut2: int = 1
    IndiBufferCall3: int = 0
    IndiBufferPut3: int = 1
    IndiBufferCall4: int = 0
    IndiBufferPut4: int = 1

    # --- strategy switches (all False by default in the .mq4) ---
    MILBURNEstrategia: bool = False      # RSI 95% (RSI+S/R+candle)
    strategy1_Enabled: bool = False      # HULK SECRET REVERSAL
    KEN_Enabled: bool = False
    candlestrtegy: bool = False          # QUIRK: contradicts base candle colour -> never signals
    Pulback_Enabled: bool = False
    EXTREME: bool = False
    secret: bool = False                 # TIME-TURN
    strategy9_Enabled: bool = False
    FLOW: bool = False                   # FLOW strategy (input simnao FLOW)
    Reversal_Enabled: bool = False
    priceaction: bool = False
    HUNTER_Enabled: bool = False
    KILLER_Enabled: bool = False
    BERSERK: bool = False
    hulk_Enabled: bool = False
    professor_Enabled: bool = False
    beast_Enabled: bool = False
    sniper_Enabled: bool = False
    ninja_Enabled: bool = False
    hacker_Enabled: bool = False
    shadow: bool = False
    Value_Enabled: bool = False
    BAND: bool = False
    BB_Enabled: bool = False
    SO_Enabled: bool = False
    AtivarCruzamento: bool = False       # EMA cross
    EnabledRVI: bool = False
    EnabledMFI: bool = False
    EnabledWPR: bool = False
    AtivarMACD: bool = False
    SeR: bool = False                    # support & resistance
    AtivaDonchian: bool = False
    Cci_Enabled: bool = False
    Adx_Enabled: bool = False
    ativar_donforex: bool = False
    Filtro_Tendencia: bool = False       # QUIRK: has no effect in the original
    FILT_RET_: bool = False              # retraction filter
    HILL: bool = False                   # QUIRK: HILL/HILL1/HILL2/HILL3 never used in final condition
    Bloquea: bool = False
    FiltroVelas: bool = False
    AtivarTamanhoVela: bool = False
    AtivarTamanhoVela1: bool = False

    # --- MILBURN / RSI 95 ---
    PeriodoRSI: int = 3
    MaxRSI: int = 35                     # QUIRK: names look swapped, used as written
    MinRSI: int = 65
    mediaMovel: int = 14
    MILBURNRatio: int = 60
    # --- BB sets ---
    BB_Period1: int = 15; BB_Dev1: float = 3; BB_Shift1: int = 3      # strategy1
    BB2_Period: int = 15; BB2_Dev: float = 3; BB2_Shift: int = 3      # hacker
    BB4_Period: int = 10; BB4_Dev: float = 2; BB4_Shift: int = 3      # hunter
    # --- CCI ---
    PERIODOCCI_3: int = 14; MINCCI_3: int = -100; MAXCCI_3: int = 100
    PERIODOCCI_2: int = 4;  MINCCI_2: int = -100; MAXCCI_2: int = 100
    PERIODOCCI0_3: int = 3; MAXCCI_03: int = 80;  MINCCI_03: int = -80
    CCI1_Period: int = 6;  Apply_to1: int = PRICE_TYPICAL; CCI1_Oversold_Level: int = -160; CCI1_Overbought_Level: int = 160
    CCI2_Period: int = 6;  Apply_too2: int = PRICE_TYPICAL; CCI2_Oversold_Level: int = -120; CCI2_Overbought_Level: int = 120
    CCI3_Period: int = 4;  Apply_to3: int = PRICE_TYPICAL; CCI3_Oversold_Level: int = -100; CCI3_Overbought_Level: int = 100
    CCI4_Period: int = 3;  Apply_to4: int = PRICE_CLOSE;   CCI4_Oversold_Level: int = 10;   CCI4_Overbought_Level: int = 90
    CCI5_Period: int = 6;  Apply_to5: int = PRICE_TYPICAL; CCI5_Oversold_Level: int = -160; CCI5_Overbought_Level: int = 160
    CCI6_Period: int = 14; Apply_to6: int = PRICE_TYPICAL; CCI6_Oversold_Level: int = -100; CCI6_Overbought_Level: int = 100
    CCI_Period: int = 6; Apply_to: int = PRICE_TYPICAL; CCI_Overbought_Level: int = 160; CCI_Oversold_Level: int = -160
    # --- RSI levels (RSI period itself is hard-coded to 2 in the original) ---
    MinRSI_2: int = 15; MaxRSI_2: int = 85
    MinRSI_3: int = 15; MaxRSI_3: int = 85
    MinRSI_4: int = 10; MaxRSI_4: int = 90
    MinRSI_6: int = 50; MaxRSI_6: int = 50
    MinRSI_7: int = 8;  MaxRSI_7: int = 92
    MinRSI_8: int = 15; MaxRSI_8: int = 85
    MinRSI_9: int = 15; MaxRSI_9: int = 85
    # --- RVI / WPR / MFI ---
    PERIODORVI: int = 10; Linha: int = 1; MAXRVI: float = 0.1; MINRVI: float = -0.1
    PERIODRVI: int = 2; MINIRVI: float = -0.3; MAXIRVI: float = 0.3
    PERIODORVI3: int = 4; MINRVI3: float = -0.2; MAXRVI3: float = 0.2
    PERIODORVI4: int = 2; MINRVI4: float = -0.4; MAXRVI4: float = 0.4
    PERIODOMFI: int = 3; MAXMFI: int = 95; MINMFI: int = 5
    PERIODOWPR: int = 1; MAXWPR: int = -95; MINWPR: int = -5
    PERIODOWPR2: int = 4; MINWPR2: int = -90; MAXWPR2: int = -10
    PERIODOWPR3: int = 2; MINWPR3: int = -75; MAXWPR3: int = -25
    PERIODOWPR4: int = 7; MINWPR4: int = -90; MAXWPR4: int = -10
    # --- NINJA (all timeframes = current in this port) ---
    RSI1_Period: int = 2; RSI1_Price: int = PRICE_CLOSE; RSI1_MIN: int = 10; RSI1_MAX: int = 90
    RSI2_Period: int = 2; RSI2_Price: int = PRICE_CLOSE; RSI2_MIN: int = 10; RSI2_MAX: int = 90
    RSI3_Period: int = 3; RSI3_Price: int = PRICE_CLOSE; RSI3_MIN: int = 15; RSI3_MAX: int = 85
    # --- misc indicators ---
    MA_Period: int = 20; MA_Shift: int = 0; MA_Method: int = MODE_EMA; MA_Applied_Price: int = PRICE_CLOSE
    FilterShift: int = 0
    dist: int = 20                                    # EXTREME / FLOW look-back
    NumBars: int = 5; VC_MAX: float = 8.0; VC_MIN: float = -8.0   # value chart
    BB_Period: int = 10; BB_Deviations: float = 2.0; BB_Shift: int = 1; BB_Price: int = PRICE_CLOSE
    SO_KPeriod: int = 5; SO_DPeriod: int = 3; SO_Slowing: int = 3
    SO_Mode: int = MODE_SMA; SO_Price: int = STO_CLOSECLOSE; SO_MAX: int = 80; SO_MIN: int = 20
    FasterEMA: int = 1; SlowerEMA: int = 5
    MACD_Period1: int = 12; MACD_Period2: int = 26; MACD_Period3: int = 9
    period_adx: int = 14; level_adx: float = 60.0; price_adx: int = PRICE_CLOSE
    Periods: int = 15; Extremes: int = 3; Margins: int = -2   # donchian
    # --- fast MA band ---
    HalfLength: int = 10; Price: int = pr_close; period: int = 20
    Multiplier: float = 2.0; Type: int = st_atr; DevLookBack: int = 10; bandshift: int = 1
    # --- size / block filters ---
    MinPips: int = 400; maxPips: int = 0
    quantidade: int = 3
    min_size_donforex: int = 0


# =============================================================================
#  MT4-style series + indicators
# =============================================================================
class MT4Series:
    def __init__(self, candles: List[dict]):
        self.n = len(candles)
        self.t = [int(c["time"]) for c in candles]
        self.o = [float(c["open"]) for c in candles]
        self.h = [float(c["high"]) for c in candles]
        self.l = [float(c["low"]) for c in candles]
        self.c = [float(c["close"]) for c in candles]
        self.v = [float(c.get("volume", 0) or 0) for c in candles]
        self._cache: Dict[tuple, list] = {}

    # ---- series accessors (shift 0 = newest). Out of range -> 0.0 (like MT4 iX()) ----
    def _k(self, s: int) -> Optional[int]:
        k = self.n - 1 - s
        return k if 0 <= k < self.n else None

    def _g(self, arr, s):
        k = self._k(s)
        return arr[k] if k is not None else 0.0

    def T(self, s): k = self._k(s); return self.t[k] if k is not None else 0
    def O(self, s): return self._g(self.o, s)
    def H(self, s): return self._g(self.h, s)
    def L(self, s): return self._g(self.l, s)
    def C(self, s): return self._g(self.c, s)

    # ---- applied price ----
    def applied(self, p: int) -> list:
        key = ("px", p)
        if key not in self._cache:
            if p == PRICE_CLOSE: a = self.c
            elif p == PRICE_OPEN: a = self.o
            elif p == PRICE_HIGH: a = self.h
            elif p == PRICE_LOW: a = self.l
            elif p == PRICE_MEDIAN: a = [(h + l) / 2 for h, l in zip(self.h, self.l)]
            elif p == PRICE_TYPICAL: a = [(h + l + c) / 3 for h, l, c in zip(self.h, self.l, self.c)]
            else: a = [(h + l + 2 * c) / 4 for h, l, c in zip(self.h, self.l, self.c)]
            self._cache[key] = a
        return self._cache[key]

    # ---- iMA ----
    def _ma_arr(self, period, method, price):
        key = ("ma", period, method, price)
        if key in self._cache:
            return self._cache[key]
        p = self.applied(price)
        n = self.n
        out = [0.0] * n
        if method == MODE_SMA:
            s = 0.0
            for k in range(n):
                s += p[k]
                if k >= period: s -= p[k - period]
                if k >= period - 1: out[k] = s / period
        elif method == MODE_EMA:
            a = 2.0 / (period + 1)
            out[0] = p[0]
            for k in range(1, n):
                out[k] = p[k] * a + out[k - 1] * (1 - a)
        elif method == MODE_SMMA:
            if n >= period:
                s = sum(p[:period]); out[period - 1] = s / period
                for k in range(period, n):
                    out[k] = (out[k - 1] * (period - 1) + p[k]) / period
        else:  # LWMA
            w = period * (period + 1) / 2
            for k in range(period - 1, n):
                out[k] = sum(p[k - period + 1 + j] * (j + 1) for j in range(period)) / w
        self._cache[key] = out
        return out

    def iMA(self, period, ma_shift, method, price, shift):
        return self._g(self._ma_arr(period, method, price), shift + ma_shift)

    # ---- iRSI (Wilder, MT4 RSI.mq4 style) ----
    def iRSI(self, period, price, shift):
        key = ("rsi", period, price)
        if key not in self._cache:
            p = self.applied(price); n = self.n
            out = [0.0] * n
            if n > period:
                ag = al = 0.0
                for k in range(1, period + 1):
                    d = p[k] - p[k - 1]
                    ag += max(d, 0); al += max(-d, 0)
                ag /= period; al /= period
                for k in range(period, n):
                    if k > period:
                        d = p[k] - p[k - 1]
                        ag = (ag * (period - 1) + max(d, 0)) / period
                        al = (al * (period - 1) + max(-d, 0)) / period
                    if al != 0: out[k] = 100.0 - 100.0 / (1 + ag / al)
                    else: out[k] = 100.0 if ag != 0 else 50.0
            self._cache[key] = out
        return self._g(self._cache[key], shift)

    # ---- iCCI ----
    def iCCI(self, period, price, shift):
        key = ("cci", period, price)
        if key not in self._cache:
            p = self.applied(price); n = self.n
            out = [0.0] * n
            for k in range(period - 1, n):
                w = p[k - period + 1:k + 1]
                sma = sum(w) / period
                d = sum(abs(x - sma) for x in w) * 0.015 / period
                out[k] = (p[k] - sma) / d if d != 0 else 0.0
            self._cache[key] = out
        return self._g(self._cache[key], shift)

    # ---- iRVI (line 0 = main, 1 = signal) ----
    def iRVI(self, period, line, shift):
        key = ("rvi", period)
        if key not in self._cache:
            n = self.n
            num = [0.0] * n; den = [0.0] * n
            for k in range(3, n):
                num[k] = ((self.c[k] - self.o[k]) + 2 * (self.c[k - 1] - self.o[k - 1]) +
                          2 * (self.c[k - 2] - self.o[k - 2]) + (self.c[k - 3] - self.o[k - 3])) / 6
                den[k] = ((self.h[k] - self.l[k]) + 2 * (self.h[k - 1] - self.l[k - 1]) +
                          2 * (self.h[k - 2] - self.l[k - 2]) + (self.h[k - 3] - self.l[k - 3])) / 6
            main = [0.0] * n; sig = [0.0] * n
            for k in range(period + 2, n):
                su = sum(num[k - period + 1:k + 1]); sd = sum(den[k - period + 1:k + 1])
                main[k] = su / sd if sd != 0 else su
            for k in range(period + 5, n):
                sig[k] = (main[k] + 2 * main[k - 1] + 2 * main[k - 2] + main[k - 3]) / 6
            self._cache[key] = (main, sig)
        return self._g(self._cache[key][1 if line == 1 else 0], shift)

    # ---- iWPR ----
    def iWPR(self, period, shift):
        key = ("wpr", period)
        if key not in self._cache:
            n = self.n; out = [0.0] * n
            for k in range(period - 1, n):
                hh = max(self.h[k - period + 1:k + 1]); ll = min(self.l[k - period + 1:k + 1])
                out[k] = -100.0 * (hh - self.c[k]) / (hh - ll) if hh - ll != 0 else 0.0
            self._cache[key] = out
        return self._g(self._cache[key], shift)

    # ---- iMFI (tick volume) ----
    def iMFI(self, period, shift):
        key = ("mfi", period)
        if key not in self._cache:
            n = self.n; tp = self.applied(PRICE_TYPICAL); out = [0.0] * n
            for k in range(period, n):
                pos = neg = 0.0
                for j in range(k - period + 1, k + 1):
                    m = tp[j] * self.v[j]
                    if tp[j] > tp[j - 1]: pos += m
                    elif tp[j] < tp[j - 1]: neg += m
                out[k] = 100.0 - 100.0 / (1 + pos / neg) if neg != 0 else 100.0
            self._cache[key] = out
        return self._g(self._cache[key], shift)

    # ---- iBands (value at bar shift + bands_shift) ----
    def iBands(self, period, dev, bands_shift, price, mode, shift):
        """mode: 1 = upper, 2 = lower (MT4 MODE_UPPER / MODE_LOWER)"""
        key = ("bb", period, dev, price)
        if key not in self._cache:
            p = self.applied(price); n = self.n
            up = [0.0] * n; lo = [0.0] * n
            for k in range(period - 1, n):
                w = p[k - period + 1:k + 1]
                m = sum(w) / period
                sd = math.sqrt(sum((x - m) ** 2 for x in w) / period)
                up[k] = m + dev * sd; lo[k] = m - dev * sd
            self._cache[key] = (up, lo)
        up, lo = self._cache[key]
        return self._g(up if mode == 1 else lo, shift + bands_shift)

    # ---- iStochastic (SMA only) ----
    def iStochastic(self, kp, dp, slowing, price_field, mode, shift):
        key = ("sto", kp, dp, slowing, price_field)
        if key not in self._cache:
            n = self.n
            hi = self.c if price_field == STO_CLOSECLOSE else self.h
            lo = self.c if price_field == STO_CLOSECLOSE else self.l
            a = [0.0] * n; b = [0.0] * n
            for k in range(kp - 1, n):
                a[k] = self.c[k] - min(lo[k - kp + 1:k + 1])
                b[k] = max(hi[k - kp + 1:k + 1]) - min(lo[k - kp + 1:k + 1])
            main = [0.0] * n; sig = [0.0] * n
            for k in range(kp + slowing - 2, n):
                sb = sum(b[k - slowing + 1:k + 1])
                main[k] = 100.0 * sum(a[k - slowing + 1:k + 1]) / sb if sb != 0 else 100.0
            for k in range(kp + slowing + dp - 3, n):
                sig[k] = sum(main[k - dp + 1:k + 1]) / dp
            self._cache[key] = (main, sig)
        return self._g(self._cache[key][1 if mode == 1 else 0], shift)

    # ---- iATR (SMA of true range) ----
    def iATR(self, period, shift):
        key = ("atr", period)
        if key not in self._cache:
            n = self.n; tr = [0.0] * n; out = [0.0] * n
            tr[0] = self.h[0] - self.l[0]
            for k in range(1, n):
                tr[k] = max(self.h[k], self.c[k - 1]) - min(self.l[k], self.c[k - 1])
            for k in range(period - 1, n):
                out[k] = sum(tr[k - period + 1:k + 1]) / period
            self._cache[key] = out
        return self._g(self._cache[key], shift)

    # ---- iMACD (main / signal(SMA)) ----
    def iMACD(self, fast, slow, sig_p, price, mode, shift):
        key = ("macd", fast, slow, sig_p, price)
        if key not in self._cache:
            f = self._ma_arr(fast, MODE_EMA, price); s = self._ma_arr(slow, MODE_EMA, price)
            main = [a - b for a, b in zip(f, s)]
            sig = [0.0] * self.n
            for k in range(sig_p - 1, self.n):
                sig[k] = sum(main[k - sig_p + 1:k + 1]) / sig_p
            self._cache[key] = (main, sig)
        return self._g(self._cache[key][1 if mode == 1 else 0], shift)

    # ---- iADX main (APPROXIMATION of MT4 ADX.mq4: Wilder DM/TR, ADX = SMA(DX)) ----
    def iADX(self, period, price, shift):
        key = ("adx", period)
        if key not in self._cache:
            n = self.n; pdm = [0.0] * n; mdm = [0.0] * n; tr = [0.0] * n
            for k in range(1, n):
                up = self.h[k] - self.h[k - 1]; dn = self.l[k - 1] - self.l[k]
                pdm[k] = up if (up > dn and up > 0) else 0.0
                mdm[k] = dn if (dn > up and dn > 0) else 0.0
                tr[k] = max(self.h[k] - self.l[k], abs(self.h[k] - self.c[k - 1]), abs(self.l[k] - self.c[k - 1]))
            sp = sm = st = 0.0; dx = [0.0] * n
            for k in range(1, n):
                if k <= period:
                    sp += pdm[k]; sm += mdm[k]; st += tr[k]
                else:
                    sp = sp - sp / period + pdm[k]; sm = sm - sm / period + mdm[k]; st = st - st / period + tr[k]
                if k >= period and st != 0:
                    pdi = 100 * sp / st; mdi = 100 * sm / st
                    dx[k] = 100 * abs(pdi - mdi) / (pdi + mdi) if (pdi + mdi) != 0 else 0.0
            out = [0.0] * n
            for k in range(2 * period - 1, n):
                out[k] = sum(dx[k - period + 1:k + 1]) / period
            self._cache[key] = out
        return self._g(self._cache[key], shift)

    # ---- iFractals (standard 5-bar form; plateau variants not implemented) ----
    def iFractals(self, upper: bool, shift):
        k = self._k(shift)
        if k is None or k < 2 or k + 2 >= self.n:
            return 0.0
        a = self.h if upper else self.l
        cur = a[k]
        if upper:
            ok = cur > a[k - 1] and cur > a[k - 2] and cur > a[k + 1] and cur > a[k + 2]
        else:
            ok = cur < a[k - 1] and cur < a[k - 2] and cur < a[k + 1] and cur < a[k + 2]
        return cur if ok else 0.0

    # ---- iHighest / iLowest -> shift index. ties -> newest bar ----
    def iHighest(self, mode, count, start):
        arr = {"open": self.o, "high": self.h}[mode]
        best = None; bi = start
        for s in range(start, start + count):
            k = self._k(s)
            if k is None: break
            if best is None or arr[k] > best: best, bi = arr[k], s
        return bi

    def iLowest(self, mode, count, start):
        arr = {"open": self.o, "low": self.l}[mode]
        best = None; bi = start
        for s in range(start, start + count):
            k = self._k(s)
            if k is None: break
            if best is None or arr[k] < best: best, bi = arr[k], s
        return bi


# =============================================================================
#  ENGINE
# =============================================================================
class DarkmatterEngineV3:
    def __init__(self, cfg: Optional[EngineConfig] = None,
                 combiners: Optional[Dict[int, Callable[[int, int], float]]] = None,
                 donforex_fn: Optional[Callable[[int, bool], bool]] = None):
        self.cfg = cfg or EngineConfig()
        self.combiners = combiners or {}
        self.donforex_fn = donforex_fn
        self._warned = False

    # ---- external indicator (iCustom) ----
    def _combiner(self, idx, buf_call, buf_put, sig_type, i):
        fn = self.combiners.get(idx)
        if fn is None:
            if not self._warned:
                warnings.warn(f"COMBINER{idx} enabled but no hook supplied -> behaves like a missing "
                              f"iCustom indicator (no signals). Pass combiners={{{idx}: fn}}.")
                self._warned = True
            return False, False
        return sinal_buffer(fn(buf_call, i + sig_type)), sinal_buffer(fn(buf_put, i + sig_type))

    # ---- candle size helpers ----
    def _calctam(self):
        d = self.cfg.digits
        return 0.001 if d <= 3 else (0.00001 if d >= 4 else 0)

    # ---- Fast-MA band buffers ----
    def _prices_for_band(self, b: MT4Series, shift):
        p = self.cfg.Price
        if p == pr_close: return b.C(shift)
        if p == pr_open: return b.O(shift)
        if p == pr_high: return b.H(shift)
        if p == pr_low: return b.L(shift)
        if p == pr_median: return (b.H(shift) + b.L(shift)) / 2
        if p == pr_typical: return (b.H(shift) + b.L(shift) + b.C(shift)) / 3
        if p == pr_weighted: return (b.H(shift) + b.L(shift) + 2 * b.C(shift)) / 4
        if p == pr_average: return (b.O(shift) + b.H(shift) + b.L(shift) + b.C(shift)) / 4
        if p == pr_medianb: return (b.O(shift) + b.C(shift)) / 2
        raise NotImplementedError("Heiken-Ashi band prices are not ported")

    def _band_buffers(self, b: MT4Series, top: int):
        cfg = self.cfg
        if cfg.Type != st_atr:
            raise NotImplementedError("BAND Type st_ste/st_sam/st_nos depend on MT4 static work arrays; only st_atr ported")
        prices = {s: self._prices_for_band(b, s) for s in range(0, top + cfg.HalfLength + 2)}
        b1, b2, b3 = {}, {}, {}
        for i in range(top, -1, -1):
            sum1 = (cfg.HalfLength + 1) * prices.get(i, 0.0)
            sumw = float(cfg.HalfLength + 1)
            p = cfg.HalfLength
            for q in range(1, cfg.HalfLength + 1):
                sum1 += prices.get(i + q, 0.0) * p; sumw += p
                if q <= i:
                    sum1 += prices.get(i - q, 0.0) * p; sumw += p
                p -= 1
            b1[i] = sum1 / sumw
            rng = b.iATR(cfg.period, i + cfg.DevLookBack)
            b2[i] = b1[i] + rng * cfg.Multiplier
            b3[i] = b1[i] - rng * cfg.Multiplier
        return prices, b1, b2, b3

    # =========================================================================
    def run(self, candles: List[dict]) -> List[dict]:
        """Process the whole window oldest->newest exactly like the MQ4 loop
        (for i = Velas .. 0).  Returns one dict per processed bar."""
        cfg = self.cfg
        b = MT4Series(candles)
        n = b.n
        limit = min(cfg.Velas, n - 1 - cfg.warmup)
        if limit < 0:
            raise ValueError(f"need at least {cfg.warmup + 2} candles, got {n}")
        dfrom = b.T(0) - 60 * 60 * 24 * cfg.Velas

        cross_up: Dict[int, float] = {}
        cross_dn: Dict[int, float] = {}
        up_buf: Dict[int, float] = {}
        dn_buf: Dict[int, float] = {}
        Resist: Dict[int, float] = {}
        Suport: Dict[int, float] = {}
        gv = lambda d, k: d.get(k, EMPTY_VALUE)

        last_signal = 0
        results: List[dict] = []

        if cfg.BAND:
            bprices, buf1, buf2, buf3 = self._band_buffers(b, limit + cfg.bandshift + 3)

        for i in range(limit, -1, -1):
            if not (b.T(i) > dfrom):
                continue
            O0, H0, L0, C0 = b.O(i), b.H(i), b.L(i), b.C(i)

            # ---------------- indicators used by the strategies ----------------
            RVI = b.iRVI(cfg.PERIODORVI, cfg.Linha, i + 1)
            MFI = b.iMFI(cfg.PERIODOMFI, i)
            WPR = b.iWPR(cfg.PERIODOWPR, i)
            ema1 = b.iMA(cfg.mediaMovel, 0, MODE_LWMA, PRICE_HIGH, i)
            ema2 = b.iMA(cfg.mediaMovel, 0, MODE_LWMA, PRICE_LOW, i)
            CCI_3 = b.iCCI(cfg.PERIODOCCI_3, PRICE_TYPICAL, i)
            CCI_2 = b.iCCI(cfg.PERIODOCCI_2, PRICE_TYPICAL, i + 1)
            RSI = b.iRSI(2, PRICE_CLOSE, i)              # QUIRK: RSI_2..RSI_9 are ALL iRSI(period=2, close)
            RSI_2 = RSI_3 = RSI_4 = RSI_5 = RSI_6 = RSI_7 = RSI_8 = RSI_9 = RSI
            MA_1 = b.iMA(20, 0, MODE_SMMA, PRICE_CLOSE, i)
            RVI_3 = b.iRVI(cfg.PERIODRVI, 0, i)
            WPR_2 = b.iWPR(cfg.PERIODOWPR2, i)
            CCI_4 = b.iCCI(cfg.PERIODOCCI0_3, PRICE_TYPICAL, i)
            CCI_6 = b.iCCI(cfg.CCI1_Period, cfg.Apply_to1, i)
            CCI_7 = b.iCCI(cfg.CCI2_Period, cfg.Apply_too2, i)
            RVI_4 = b.iRVI(cfg.PERIODORVI3, 0, i)
            CCI_8 = b.iCCI(cfg.CCI3_Period, cfg.Apply_to3, i)
            CCI_9 = b.iCCI(cfg.CCI4_Period, cfg.Apply_to4, i)
            RVI_5 = b.iRVI(cfg.PERIODORVI4, 0, i)
            WPR_3 = b.iWPR(cfg.PERIODOWPR3, i)
            WPR_4 = b.iWPR(cfg.PERIODOWPR4, i)
            CCI_10 = b.iCCI(cfg.CCI5_Period, cfg.Apply_to5, i)
            CCI_11 = b.iCCI(cfg.CCI6_Period, cfg.Apply_to6, i)
            PadraoVelas = (b.O(i + 1) + H0 + L0 + b.C(i + 2)) / 4.0

            # EMA cross inputs (typical price)
            fN = b.iMA(cfg.FasterEMA, 0, MODE_EMA, PRICE_TYPICAL, i)
            fP = b.iMA(cfg.FasterEMA, 0, MODE_EMA, PRICE_TYPICAL, i + 1)
            fA = b.iMA(cfg.FasterEMA, 0, MODE_EMA, PRICE_TYPICAL, i - 1)   # i-1 at i=0 -> 0.0 (same as MT4)
            sN = b.iMA(cfg.SlowerEMA, 0, MODE_EMA, PRICE_TYPICAL, i)
            sP = b.iMA(cfg.SlowerEMA, 0, MODE_EMA, PRICE_TYPICAL, i + 1)
            sA = b.iMA(cfg.SlowerEMA, 0, MODE_EMA, PRICE_TYPICAL, i - 1)

            # ---------------- support / resistance (final LWMA version wins) ----------------
            velas = (O0 + H0 + L0 + C0) / 4
            if b.iFractals(True, i) > 0 and velas > ema1: Resist[i] = H0
            else: Resist[i] = gv(Resist, i + 1)
            if b.iFractals(False, i) > 0 and velas < ema2: Suport[i] = L0
            else: Suport[i] = gv(Suport, i + 1)

            hhb = b.iHighest("high", cfg.dist, i)
            llb = b.iLowest("low", cfg.dist, i)

            maximaEma = b.iMA(14, 1, MODE_EMA, PRICE_HIGH, i)
            minimaEma = b.iMA(14, 1, MODE_EMA, PRICE_LOW, i)

            # ---------------- fast MA band ----------------
            if cfg.BAND:
                s = cfg.bandshift
                bandup = (bprices.get(i + s, 0.0) < buf3.get(i + 1 + s, 0.0)) and (b.C(i + s) < b.O(i + s))
                banddown = (bprices.get(i + s, 0.0) > buf2.get(i + 1 + s, 0.0)) and (b.C(i + s) > b.O(i + s))
            else:
                bandup = banddown = True

            # ---------------- Donchian ----------------
            Maxima = Minima = 0.0
            fora_banda_up = fora_banda_dn = True
            if cfg.AtivaDonchian:
                cnt = 1   # QUIRK: input AtivaDonchian (ACTIVATE=1) is used as the look-back count
                Maxima = (b.O(b.iHighest("open", cnt, i)) + b.H(b.iHighest("high", cnt, i))) / 2
                Minima = (b.O(b.iLowest("open", cnt, i)) + b.L(b.iLowest("low", cnt, i))) / 2
                Maxima = Maxima - (Maxima - Minima) * -2 / 100
                Minima = Minima + (Minima - Maxima) * -2 / 100
                if cfg.Extremes == 1:
                    SsMax = b.H(b.iHighest("high", cfg.Periods, i)); SsMin = b.L(b.iLowest("low", cfg.Periods, i))
                elif cfg.Extremes == 3:
                    SsMax = (b.O(b.iHighest("open", cfg.Periods, i)) + b.H(b.iHighest("high", cfg.Periods, i))) / 2
                    SsMin = (b.O(b.iLowest("open", cfg.Periods, i)) + b.L(b.iLowest("low", cfg.Periods, i))) / 2
                else:
                    SsMax = b.O(b.iHighest("open", cfg.Periods, i)); SsMin = b.O(b.iLowest("open", cfg.Periods, i))
                smin = SsMin + (SsMax - SsMin) * cfg.Margins / 100
                smax = SsMax - (SsMax - SsMin) * cfg.Margins / 100
                alta = True           # QUIRK: re-declared 'true' on every bar in the original
                fora_banda_up = (C0 < smin and not alta)      # -> always False
                fora_banda_dn = (C0 > smax and alta)

            # ---------------- support & resistance switch ----------------
            Sup_i, Sup_i1 = gv(Suport, i), gv(Suport, i + 1)
            Res_i, Res_i1 = gv(Resist, i), gv(Resist, i + 1)
            ser_up = (not cfg.SeR) or ((L0 <= Sup_i and O0 >= C0) and (b.H(i + 1) >= Sup_i1))
            ser_dn = (not cfg.SeR) or ((H0 >= Res_i and O0 <= C0) and (b.L(i + 1) <= Res_i1))

            # ---------------- price action ----------------
            if cfg.priceaction:
                up_price = (H0 >= Sup_i) and (L0 <= Sup_i)
                dn_price = (L0 <= Res_i) and (H0 >= Res_i)
            else:
                up_price = dn_price = True

            # ---------------- strategy 1 (HULK SECRET) ----------------
            if cfg.strategy1_Enabled:
                bl0 = b.iBands(cfg.BB_Period1, cfg.BB_Dev1, cfg.BB_Shift1, PRICE_CLOSE, 2, i)
                bl1 = b.iBands(cfg.BB_Period1, cfg.BB_Dev1, cfg.BB_Shift1, PRICE_CLOSE, 2, i + 1)
                bu0 = b.iBands(cfg.BB_Period1, cfg.BB_Dev1, cfg.BB_Shift1, PRICE_CLOSE, 1, i)
                bu1 = b.iBands(cfg.BB_Period1, cfg.BB_Dev1, cfg.BB_Shift1, PRICE_CLOSE, 1, i + 1)
                up_strategy1 = C0 < bl0 and O0 > bl0 and b.O(i + 1) > bl1 and b.C(i + 1) > bl1 and CCI_3 < cfg.MINCCI_3
                dn_strategy1 = C0 > bu0 and O0 < bu0 and b.O(i + 1) < bu1 and b.C(i + 1) < bu1 and CCI_3 > cfg.MAXCCI_3
            else:
                up_strategy1 = dn_strategy1 = True

            # ---------------- KEN ----------------
            if cfg.KEN_Enabled:     # QUIRK: uses MinRSI(65)/MaxRSI(35) as written
                up_KEN = CCI_2 > cfg.MINCCI_2 and RSI_2 <= cfg.MinRSI
                dn_KEN = CCI_2 < cfg.MAXCCI_2 and RSI_2 >= cfg.MaxRSI
            else:
                up_KEN = dn_KEN = True

            # ---------------- SNIPER ----------------
            if cfg.sniper_Enabled:  # QUIRK: dn side uses MINWPR4
                up_sniper = CCI_10 < cfg.CCI5_Oversold_Level and WPR_4 <= cfg.MINWPR4
                dn_sniper = CCI_10 > cfg.CCI5_Overbought_Level and WPR_4 >= cfg.MINWPR4
            else:
                up_sniper = dn_sniper = True

            # ---------------- NINJA ----------------
            if cfg.ninja_Enabled:
                r1 = b.iRSI(cfg.RSI1_Period, cfg.RSI1_Price, i)
                r2 = b.iRSI(cfg.RSI2_Period, cfg.RSI2_Price, i)
                r3 = b.iRSI(cfg.RSI3_Period, cfg.RSI3_Price, i)
                up_ninja = r1 < cfg.RSI1_MIN and r2 < cfg.RSI2_MIN and r3 < cfg.RSI3_MIN
                dn_ninja = r1 > cfg.RSI1_MAX and r2 > cfg.RSI2_MAX and r3 > cfg.RSI3_MAX
            else:
                up_ninja = dn_ninja = True

            # ---------------- PROFESSOR ----------------
            if cfg.professor_Enabled:
                up_professor = CCI_8 < cfg.CCI3_Oversold_Level and RSI_9 <= cfg.MinRSI_9
                dn_professor = CCI_8 > cfg.CCI3_Overbought_Level and RSI_9 >= cfg.MaxRSI_9
            else:
                up_professor = dn_professor = True

            # ---------------- HULK ----------------
            if cfg.hulk_Enabled:
                up_hulk = CCI_7 < cfg.CCI2_Oversold_Level and RVI_4 <= cfg.MINRVI3
                dn_hulk = CCI_7 > cfg.CCI2_Overbought_Level and RVI_4 >= cfg.MAXRVI3
            else:
                up_hulk = dn_hulk = True

            # ---------------- HUNTER ----------------
            if cfg.HUNTER_Enabled:  # QUIRK: CCI/RSI thresholds are the "wrong" Min/Max -> almost always true
                bl0 = b.iBands(cfg.BB4_Period, cfg.BB4_Dev, cfg.BB4_Shift, PRICE_CLOSE, 2, i)
                bl1 = b.iBands(cfg.BB4_Period, cfg.BB4_Dev, cfg.BB4_Shift, PRICE_CLOSE, 2, i + 1)
                bu0 = b.iBands(cfg.BB4_Period, cfg.BB4_Dev, cfg.BB4_Shift, PRICE_CLOSE, 1, i)
                bu1 = b.iBands(cfg.BB4_Period, cfg.BB4_Dev, cfg.BB4_Shift, PRICE_CLOSE, 1, i + 1)
                up_HUNTER = (C0 < bl0 and O0 > bl0 and b.O(i + 1) > bl1 and b.C(i + 1) > bl1
                             and (CCI_4 < cfg.MAXCCI_03 and RSI_5 <= cfg.MaxRSI_2))
                dn_HUNTER = (C0 > bu0 and O0 < bu0 and b.O(i + 1) < bu1 and b.C(i + 1) < bu1
                             and (CCI_4 > cfg.MINCCI_03 and RSI_5 >= cfg.MinRSI_2))
            else:
                up_HUNTER = dn_HUNTER = True

            # ---------------- KILLER ----------------
            if cfg.KILLER_Enabled:
                up_KILLER = RSI_7 <= cfg.MinRSI_7
                dn_KILLER = RSI_7 >= cfg.MaxRSI_7
            else:
                up_KILLER = dn_KILLER = True

            # ---------------- REVERSAL ----------------
            if cfg.Reversal_Enabled:  # QUIRK: dn side uses MinRSI_3 / MinRSI_4
                up_sg = RSI_2 <= cfg.MinRSI_2 and RSI_3 <= cfg.MinRSI_3 and RSI_4 <= cfg.MinRSI_4
                dn_sg = RSI_2 >= cfg.MaxRSI_2 and RSI_3 >= cfg.MinRSI_3 and RSI_4 >= cfg.MinRSI_4
            else:
                up_sg = dn_sg = True

            # ---------------- BERSERK ----------------
            if cfg.BERSERK:
                up_berserk = RSI_8 <= cfg.MinRSI_8 and CCI_6 < cfg.CCI1_Oversold_Level
                dn_berserk = RSI_8 >= cfg.MaxRSI_8 and CCI_6 > cfg.CCI1_Overbought_Level
            else:
                up_berserk = dn_berserk = True

            # ---------------- CCI ----------------
            if cfg.Cci_Enabled:
                cci = b.iCCI(cfg.CCI_Period, cfg.Apply_to, i)
                up_cci = cci < cfg.CCI_Oversold_Level
                dn_cci = cci > cfg.CCI_Overbought_Level
            else:
                up_cci = dn_cci = True

            # ---------------- HACKER ----------------
            if cfg.hacker_Enabled:
                bl0 = b.iBands(cfg.BB2_Period, cfg.BB2_Dev, cfg.BB2_Shift, PRICE_CLOSE, 2, i)
                bl1 = b.iBands(cfg.BB2_Period, cfg.BB2_Dev, cfg.BB2_Shift, PRICE_CLOSE, 2, i + 1)
                bu0 = b.iBands(cfg.BB2_Period, cfg.BB2_Dev, cfg.BB2_Shift, PRICE_CLOSE, 1, i)
                bu1 = b.iBands(cfg.BB2_Period, cfg.BB2_Dev, cfg.BB2_Shift, PRICE_CLOSE, 1, i + 1)
                up_hacker = (C0 < bl0 and O0 > bl0 and b.O(i + 1) > bl1 and b.C(i + 1) > bl1
                             and CCI_11 < cfg.CCI6_Oversold_Level)
                dn_hacker = (C0 > bu0 and O0 < bu0 and b.O(i + 1) < bu1 and b.C(i + 1) < bu1
                             and CCI_11 > cfg.CCI6_Overbought_Level)
            else:
                up_hacker = dn_hacker = True

            # ---------------- STRATEGY 3 (strategy9) ----------------
            if cfg.strategy9_Enabled:
                up_strategy9 = RVI_3 <= cfg.MINIRVI and WPR_2 <= cfg.MINWPR2
                dn_strategy9 = RVI_3 >= cfg.MAXIRVI and WPR_2 >= cfg.MAXWPR2
            else:
                up_strategy9 = dn_strategy9 = True

            # ---------------- BEAST ----------------
            if cfg.beast_Enabled:
                up_beast = CCI_9 < cfg.CCI4_Oversold_Level and RVI_5 <= cfg.MINRVI4 and WPR_3 <= cfg.MINWPR3
                dn_beast = CCI_9 > cfg.CCI4_Overbought_Level and RVI_5 >= cfg.MAXRVI4 and WPR_3 >= cfg.MAXWPR3
            else:
                up_beast = dn_beast = True

            # ---------------- VALUE CHART ----------------
            if cfg.Value_Enabled:
                def _mva(nb, cb):
                    return sum((b.H(k) + b.L(k)) / 2.0 for k in range(cb, nb + cb)) / nb
                def _atr(nb, cb):
                    u = 0.2 * (sum(b.H(k) - b.L(k) for k in range(cb, nb + cb)) / nb)
                    return 0.00000001 if u == 0 else u
                vclose = (C0 - _mva(cfg.NumBars, i)) / _atr(cfg.NumBars, i)
                up_vc = vclose <= cfg.VC_MIN
                dn_vc = vclose >= cfg.VC_MAX
            else:
                up_vc = dn_vc = True

            # ---------------- Bollinger ----------------
            if cfg.BB_Enabled:
                up_bb = C0 < b.iBands(cfg.BB_Period, cfg.BB_Deviations, cfg.BB_Shift, cfg.BB_Price, 2, i)
                dn_bb = C0 > b.iBands(cfg.BB_Period, cfg.BB_Deviations, cfg.BB_Shift, cfg.BB_Price, 1, i)
            else:
                up_bb = dn_bb = True

            # ---------------- Stochastic ----------------
            if cfg.SO_Enabled:
                st = b.iStochastic(cfg.SO_KPeriod, cfg.SO_DPeriod, cfg.SO_Slowing, cfg.SO_Price, 1, i)
                up_so = st < cfg.SO_MIN
                dn_so = st > cfg.SO_MAX
            else:
                up_so = dn_so = True

            # ---------------- ADX ----------------
            if cfg.Adx_Enabled:
                adx = b.iADX(cfg.period_adx, cfg.price_adx, i)
                up_adx = adx <= cfg.level_adx
                dn_adx = adx >= cfg.level_adx
            else:
                up_adx = dn_adx = True

            # ---------------- MILBURN (RSI 95%) ----------------
            if cfg.MILBURNEstrategia:
                RSI_1 = b.iRSI(cfg.PeriodoRSI, PRICE_OPEN, i)
                up_MILBURN = bool(L0 <= Sup_i and O0 > C0 and ser_up and RSI_1 <= cfg.MinRSI)
                dn_MILBURN = bool(H0 >= Res_i and O0 < C0 and ser_dn and RSI_1 >= cfg.MaxRSI)
            else:
                up_MILBURN = dn_MILBURN = True

            # Filtro_Tendencia: QUIRK - only ever sets True, never blocks
            up_filter_ratio = dn_filter_ratio = True
            # HILL/HILL1/HILL2/HILL3: QUIRK - results never enter the final condition

            # ---------------- retraction filter ----------------
            up_MILBURN_ok = dn_MILBURN_ok = True
            if cfg.FILT_RET_:
                if C0 > O0:
                    dn_MILBURN_ok = (C0 - O0) / ((H0 - O0) + 0.0000001) > cfg.MILBURNRatio / 100
                else:
                    up_MILBURN_ok = (O0 - C0) / ((O0 - L0) + 0.0000001) > cfg.MILBURNRatio / 100

            # ---------------- MACD ----------------
            if cfg.AtivarMACD:
                MC = b.iMACD(cfg.MACD_Period1, cfg.MACD_Period2, cfg.MACD_Period3, PRICE_CLOSE, 0, i)
                MP = b.iMACD(cfg.MACD_Period1, cfg.MACD_Period2, cfg.MACD_Period3, PRICE_OPEN, 0, i)
                MS = b.iMACD(cfg.MACD_Period1, cfg.MACD_Period2, cfg.MACD_Period3, PRICE_CLOSE, 1, i)
            else:
                MC = MP = MS = 0.0

            # ---------------- external combiners ----------------
            up1, dn1 = (self._combiner(1, cfg.IndiBufferCall, cfg.IndiBufferPut, cfg.SignalType, i)
                        if cfg.COMBINER else (True, True))
            up2, dn2 = (self._combiner(2, cfg.IndiBufferCall2, cfg.IndiBufferPut2, cfg.SignalType2, i)
                        if cfg.COMBINER2 else (True, True))
            up3, dn3 = (self._combiner(3, cfg.IndiBufferCall3, cfg.IndiBufferPut3, cfg.SignalType3, i)
                        if cfg.COMBINER3 else (True, True))
            up4, dn4 = (self._combiner(4, cfg.IndiBufferCall4, cfg.IndiBufferPut4, cfg.SignalType4, i)
                        if cfg.COMBINER4 else (True, True))

            # ---------------- same-colour block ----------------
            candlesup = candlesdn = 0
            if cfg.Bloquea:
                for j in range(i + cfg.quantidade + 1, i - 1, -1):
                    if b.C(j + 1) >= b.O(j + 1): candlesup += 1
                    else: candlesup = 0
                    if b.C(j + 1) <= b.O(j + 1): candlesdn += 1
                    else: candlesdn = 0

            # ---------------- candle-size filters ----------------
            def tamanho_ok():
                ok = True
                if cfg.AtivarTamanhoVela:      # (High-Low) <= calctam()*MinPips
                    ok = ok and ((H0 - L0) <= self._calctam() * cfg.MinPips)
                if cfg.AtivarTamanhoVela1:     # (High-Low) >= calctam1()*maxPips
                    ok = ok and ((H0 - L0) >= self._calctam() * cfg.maxPips)
                return ok

            def padrao(direction):             # Padrao(): two same-colour candles (i+1, i)
                if direction == "call":
                    return b.C(i + 1) < b.O(i + 1) and b.C(i) < b.O(i)
                return b.C(i + 1) > b.O(i + 1) and b.C(i) > b.O(i)

            hook_don = lambda up: (self.donforex_fn(i, up) if self.donforex_fn else False)

            # =================== FINAL CALL CONDITION ===================
            call_cond = (
                gv(up_buf, i) == EMPTY_VALUE
                and C0 < O0
                and gv(cross_up, i + 1) == EMPTY_VALUE
                and gv(cross_dn, i + 1) == EMPTY_VALUE
                and bandup and up1 and up2 and up3 and up4
                and up_MILBURN and fora_banda_up and up_price and up_cci and up_adx and up_bb
                and up_strategy1 and up_KEN and up_sg and up_strategy9 and up_HUNTER and up_KILLER
                and up_berserk and up_hulk and up_professor and up_vc and up_beast and up_ninja
                and up_sniper and up_hacker
                and (not cfg.shadow or (PadraoVelas < minimaEma and C0 < O0))
                and (not cfg.candlestrtegy or (b.O(i + 1) > b.C(i + 1)))
                and (not cfg.candlestrtegy or (O0 < C0))
                and (not cfg.candlestrtegy or (b.C(i + 1) < O0))
                and (not cfg.candlestrtegy or (b.O(i + 1) > C0))
                and (not cfg.Pulback_Enabled or (RSI_6 <= cfg.MinRSI_6 and padrao("call")
                                                  and b.C(i + cfg.FilterShift) > MA_1))
                and (not cfg.EXTREME or (i == llb))
                and (not cfg.secret or (b.T(i) // 60) % 60 == 59)
                and (not cfg.secret or padrao("call"))
                and (not cfg.AtivaDonchian or (b.L(i) < Minima and C0 < O0))
                and (not cfg.EnabledRVI or RVI <= cfg.MINRVI)
                and (not cfg.EnabledMFI or MFI <= cfg.MINMFI)
                and (not cfg.EnabledWPR or WPR <= cfg.MAXWPR)
                and (not cfg.FLOW or (i == hhb))
                and (not cfg.AtivarMACD or MC < MP)
                and (not cfg.AtivarMACD or bool(MC))
                and (not cfg.AtivarMACD or MP > MS)
                and (not cfg.AtivarMACD or bool(MC))
                and (not cfg.AtivarCruzamento or fN > sN)
                and (not cfg.AtivarCruzamento or fP < sP)
                and (not cfg.AtivarCruzamento or fA > sA)
                and up_so and up_MILBURN_ok
                and (not cfg.ativar_donforex or hook_don(True))
                and gv(dn_buf, i) == EMPTY_VALUE
                and up_filter_ratio
                and tamanho_ok()
                and (not cfg.Bloquea or candlesdn < cfg.quantidade)
                and ser_up
                and (not cfg.FiltroVelas or (O0 < b.C(i + 1)))
            )

            if call_cond:
                if b.T(i) > last_signal + (cfg.period_minutes * cfg.Intervalo) * 60:
                    if cfg.DirectionSignal:
                        if cfg.enviar_telegram: last_signal = b.T(0)
                        cross_up[i] = b.L(i)
                    else:
                        if cfg.enviar_telegram: last_signal = b.T(0)
                        cross_dn[i] = b.H(i)
            else:
                if cfg.DirectionSignal:
                    if i == 0 and not cfg.enviar_telegram: cross_up.pop(i, None)
                else:
                    if i == 0 and not cfg.enviar_telegram: cross_dn.pop(i, None)

            # =================== FINAL PUT CONDITION ===================
            put_cond = (
                gv(up_buf, i) == EMPTY_VALUE
                and C0 > O0
                and banddown
                and gv(cross_up, i + 1) == EMPTY_VALUE
                and gv(cross_dn, i + 1) == EMPTY_VALUE
                and dn1 and dn2 and dn3 and dn4
                and dn_cci and dn_adx and dn_bb and dn_strategy1 and dn_KEN and dn_sg and dn_strategy9
                and dn_HUNTER and dn_KILLER and dn_berserk and dn_hulk and dn_professor and dn_vc
                and dn_beast and dn_ninja and dn_sniper and dn_hacker
                and (not cfg.shadow or (PadraoVelas > maximaEma and C0 > O0))
                and (not cfg.candlestrtegy or (b.O(i + 1) < b.C(i + 1)))
                and (not cfg.candlestrtegy or (O0 > C0))
                and (not cfg.candlestrtegy or (b.C(i + 1) > O0))
                and (not cfg.candlestrtegy or (b.O(i + 1) < C0))
                and (not cfg.Pulback_Enabled or (RSI_6 >= cfg.MaxRSI_6 and padrao("put")
                                                  and b.C(i + cfg.FilterShift) < MA_1))
                and (not cfg.EXTREME or (i == hhb))
                and (not cfg.secret or (b.T(i) // 60) % 60 == 59)
                and (not cfg.secret or padrao("put"))
                and (not cfg.AtivaDonchian or (b.H(i) > Maxima and C0 > O0))
                and (not cfg.EnabledRVI or RVI >= cfg.MAXRVI)
                and (not cfg.EnabledMFI or MFI >= cfg.MAXMFI)
                and (not cfg.EnabledWPR or WPR >= cfg.MINWPR)
                and (not cfg.FLOW or (i == llb))
                and dn1 and dn2
                and (not cfg.AtivarMACD or MC > MP)
                and (not cfg.AtivarMACD or bool(MC))
                and (not cfg.AtivarMACD or MP < MS)
                and (not cfg.AtivarMACD or bool(MC))
                and (not cfg.AtivarCruzamento or fN < sN)
                and (not cfg.AtivarCruzamento or fP > sP)
                and (not cfg.AtivarCruzamento or fA < sA)
                and dn_so and dn_MILBURN
                and (not cfg.ativar_donforex or hook_don(False))
                and gv(dn_buf, i) == EMPTY_VALUE
                and dn_filter_ratio and dn_MILBURN_ok and dn_price and fora_banda_dn
                and tamanho_ok()
                and (not cfg.Bloquea or candlesup < cfg.quantidade)
                and (not cfg.FiltroVelas or (O0 > b.C(i + 1)))
                and ser_dn
            )

            if put_cond:
                if b.T(i) > last_signal + (cfg.period_minutes * cfg.Intervalo) * 60:
                    if cfg.DirectionSignal:
                        if cfg.enviar_telegram: last_signal = b.T(0)
                        cross_dn[i] = b.H(i)
                    else:
                        if cfg.enviar_telegram: last_signal = b.T(0)
                        cross_up[i] = b.L(i)
            else:
                if cfg.DirectionSignal:
                    if i == 0 and not cfg.enviar_telegram: cross_dn.pop(i, None)
                else:
                    if i == 0 and not cfg.enviar_telegram: cross_up.pop(i, None)

            # ---------------- buffer re-affirm + entry-candle lock (end of loop body) ----------------
            if sinal_buffer(gv(cross_up, i)): cross_up[i] = b.L(i)
            if sinal_buffer(gv(cross_dn, i)): cross_dn[i] = b.H(i)
            if sinal_buffer(gv(up_buf, i)): up_buf[i] = b.L(i)
            if sinal_buffer(gv(dn_buf, i)): dn_buf[i] = b.H(i)

            if sinal_buffer(gv(cross_up, i + 1)) and not sinal_buffer(gv(up_buf, i + 1)) and gv(up_buf, i) == EMPTY_VALUE:
                last_signal = b.T(i + 2)
                up_buf[i] = b.L(i)
            if sinal_buffer(gv(cross_dn, i + 1)) and not sinal_buffer(gv(dn_buf, i + 1)) and gv(dn_buf, i) == EMPTY_VALUE:
                last_signal = b.T(i + 2)
                dn_buf[i] = b.H(i)

            sig = None
            if sinal_buffer(gv(cross_up, i)): sig = "CALL"
            elif sinal_buffer(gv(cross_dn, i)): sig = "PUT"
            results.append({"shift": i, "time": b.T(i), "signal": sig,
                            "call_cond": call_cond, "put_cond": put_cond})
        return results

    # =========================================================================
    def latest(self, candles: List[dict], shift: int = 0) -> dict:
        """Signal on a single bar. shift=0: forming candle (pre-alert style),
        shift=1: last CLOSED candle. Entry is the candle after `shift`."""
        for r in self.run(candles):
            if r["shift"] == shift:
                return r
        return {"shift": shift, "signal": None}


# -----------------------------------------------------------------------------


# =====================================================================
#  module: darkmatter/signals/indicators.py  -  LIVE SIGNAL: indicators (pure python, no extra packages).
# =====================================================================
# =====================================================================
#  LIVE SIGNAL  -  indicators (pure python, no extra packages)
# =====================================================================
def _ema(vals, n):
    k = 2.0 / (n + 1)
    out, e = [], None
    for v in vals:
        e = v if e is None else e + k * (v - e)
        out.append(e)
    return out


def _rs_to_rsi(g, l):
    if l == 0:
        return 50.0 if g == 0 else 100.0
    return 100.0 - 100.0 / (1.0 + g / l)


def _rsi_series(c, n=14):
    """Wilder RSI. Entries before index n are None."""
    out = [None] * len(c)
    if len(c) <= n:
        return out
    gain = loss = 0.0
    for i in range(1, n + 1):
        d = c[i] - c[i - 1]
        gain += d if d > 0 else 0.0
        loss += -d if d < 0 else 0.0
    gain /= n
    loss /= n
    out[n] = _rs_to_rsi(gain, loss)
    for i in range(n + 1, len(c)):
        d = c[i] - c[i - 1]
        gain = (gain * (n - 1) + (d if d > 0 else 0.0)) / n
        loss = (loss * (n - 1) + (-d if d < 0 else 0.0)) / n
        out[i] = _rs_to_rsi(gain, loss)
    return out


def _atr_series(h, l, c, n=14):
    """Wilder ATR (running mean while there are fewer than n bars)."""
    out, a = [], 0.0
    for i in range(len(c)):
        tr = h[i] - l[i] if i == 0 else max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))
        a = (a * i + tr) / (i + 1) if i < n else (a * (n - 1) + tr) / n
        out.append(a)
    return out


def _linreg(vals):
    """(slope per bar, r^2) of a least-squares line."""
    n = len(vals)
    sx, sxx = n * (n - 1) / 2.0, (n - 1) * n * (2 * n - 1) / 6.0
    sy = sum(vals)
    sxy = sum(i * v for i, v in enumerate(vals))
    den = n * sxx - sx * sx
    if den == 0:
        return 0.0, 0.0
    slope = (n * sxy - sx * sy) / den
    inter = (sy - slope * sx) / n
    ssr = sum((v - (inter + slope * i)) ** 2 for i, v in enumerate(vals))
    mean = sy / n
    sst = sum((v - mean) ** 2 for v in vals)
    return slope, (1.0 - ssr / sst if sst > 0 else 0.0)


def _sr_levels(h, l, a, price):
    """Cluster swing highs / lows of closed candles into levels.
    Returns (support, support_touches, resistance, resistance_touches); None when there is no level."""
    n, k = len(h), 3
    if n < 2 * k + 5:
        return None, 0, None, 0
    pts = []
    for i in range(max(k, n - 300), n - k):
        if h[i] == max(h[i - k:i + k + 1]):
            pts.append(h[i])
        if l[i] == min(l[i - k:i + k + 1]):
            pts.append(l[i])
    if not pts:
        return None, 0, None, 0
    pts.sort()
    tol = 0.25 * a
    groups, cur = [], [pts[0]]
    for p in pts[1:]:
        if p - cur[-1] <= tol:
            cur.append(p)
        else:
            groups.append(cur)
            cur = [p]
    groups.append(cur)
    levels = [(sum(g) / len(g), len(g)) for g in groups]
    below = [x for x in levels if x[0] <= price]
    above = [x for x in levels if x[0] > price]
    sup = max(below, key=lambda x: x[0]) if below else (None, 0)
    res = min(above, key=lambda x: x[0]) if above else (None, 0)
    return sup[0], sup[1], res[0], res[1]


# =====================================================================
#  module: darkmatter/signals/signal_settings.py  -  LIVE SIGNAL: settings constants and text helpers.
# =====================================================================
# =====================================================================
#  LIVE SIGNAL  -  settings and text helpers
# =====================================================================
OWNER_DEFAULT = "@Ankon2058"
TZ_DEFAULT_MIN = 6 * 60                    # UTC+06:00
SIGNAL_CANDLES_MIN, SIGNAL_CANDLES_MAX, SIGNAL_CANDLES_DEFAULT = 100, 1000, 300
MTG_MAX, MTG_DEFAULT = 3, 1

SIGNAL_SECOND = 42        # the scan for the next entry starts this many seconds into every minute ...
SIGNAL_LATEST_START = 44  # ... and never later than this (so a signal leads its entry candle by 16 s or more)
SIGNAL_MIN_LEAD = 15      # last guard: a signal that would lead its entry candle by less than this is dropped
MIN_CONF = 62.0           # weakest confidence that may be published as a signal
STALE_SEC = 8.0           # a market whose last tick is older than this is not analysed
MIN_BARS = 60             # closed candles needed before a market can be analysed
RESULT_GRACE = 1.5        # seconds to wait after a candle closes before judging it
BG_INTERVAL = 5.0         # background analysis cadence (status line), seconds
IND_WINDOW = 600          # candles fed to the indicators (history statistics use all loaded candles)
SERIES_KEEP = 1500        # closed candles kept per market while running

# ---------------------------------------------------------------------
#  SIGNAL STRATEGY  -  DARKMATTER_ENGINE_V3 (KOMBINER V2 ULTRA), see darkmatter_engine_v3.py
# ---------------------------------------------------------------------
# Overrides of the strategy's own defaults (every name of `EngineConfig` in darkmatter_engine_v3.py is allowed).
# COMBINER = False because the original default needs the external MT4 indicator "V8.0" (COMBINER 1), which is
# not part of the strategy file: with COMBINER = True and no hook installed (ENGINE_COMBINERS) the strategy can
# never give a signal. Switch the filters you want on here, e.g. {"COMBINER": False, "Cci_Enabled": True}.
ENGINE_CONFIG = {"COMBINER": False}
# External indicators for COMBINER .. COMBINER4:  {1: factory}.  factory(candles) is called once per market
# check with the candle list (oldest -> newest, dicts with time / open / high / low / close / volume) and
# must return fn(buffer_index, shift) -> float  (0 / None = no signal), exactly as the strategy file describes.
ENGINE_COMBINERS = {}
ENGINE_LIVE_BARS = 100       # newest candles the strategy walks through on every check (MT4 "Velas" for the live bot)
ENGINE_WARMUP = 120          # older candles that are only there so the indicators are warmed up

# ---------------------------------------------------------------------
#  SCORING  -  ranks the markets that have a DARKMATTER_ENGINE_V3 signal; it NEVER removes a signal
# ---------------------------------------------------------------------
# DARKMATTER_ENGINE_V3 decides IF a market has a signal. The score only decides WHICH of those markets is published
# (the highest score wins, payout breaks ties) and fills the STRENGTH line / chart gauge. It is a setup-quality
# rank, not a win probability. SCORE_MIN must stay >= MIN_CONF, otherwise MIN_CONF would silently drop signals.
SCORE_MIN, SCORE_MAX = 65.0, 92.0         # displayed strength range (score 0 -> SCORE_MIN, score 1 -> SCORE_MAX)
SCORE_WEIGHTS = {                          # relative weight of every factor (only the ratios matter)
    "RSI": 0.20,       # RSI(7) stretched against the signal (reversal) / with it (flow)
    "BAND": 0.20,      # distance from the 20-candle mean in standard deviations
    "SWING": 0.10,     # position inside the last 12 candles' range
    "WICK": 0.10,      # rejection wick on the live candle
    "BODY": 0.05,      # live candle body relative to ATR
    "LEVEL": 0.10,     # closeness to support (CALL) / resistance (PUT)
    "REGIME": 0.10,    # trend / range context (efficiency ratio + EMA20/50)
    "HISTORY": 0.15,   # how often THIS market went the signalled way after a candle of this colour
}
HISTORY_SHRINK = 20.0     # pseudo-candles pulling the market's own hit rate towards 50 %


def short_symbol(sym):
    """LTCUSD_otc -> LTCUSD (name used in the PARTIAL RESULT list)."""
    return re.sub(r"_otc$", "", str(sym), flags=re.I).upper().replace("_", "")



def _clamp(x, lo, hi):
    return lo if x < lo else hi if x > hi else x


def sig_symbol(sym):
    """NZDJPY_otc -> NZDJPY-OTC"""
    return re.sub(r"_otc$", "-OTC", str(sym), flags=re.I).upper().replace("_", "-")


def tz_label(minutes):
    sign = "-" if minutes < 0 else "+"
    m = abs(int(minutes))
    return f"UTC{sign}{m // 60:02d}:{m % 60:02d}"


# =====================================================================
#  module: darkmatter/signals/engine.py  -  LIVE SIGNAL: the signal engine = DARKMATTER_ENGINE_V3 (KOMBINER V2 ULTRA) and the reason text.
# =====================================================================
# =====================================================================
#  LIVE SIGNAL  -  the signal engine
# =====================================================================
# The strategy itself is darkmatter_engine_v3.py (a 1:1 port of the original MT4 expert advisor) and is used exactly as
# delivered. This module only
#   1. feeds it the live candles of one market (closed candles + the forming candle = MT4 shift 0),
#   2. turns its CALL / PUT verdict for the forming candle into the result dict that the session, the
#      messages and the charts use,
#   3. adds the market context shown in the message (trend, regime, support, resistance, ATR) - the strategy
#      does not depend on it,
#   4. writes the plain-English reason.
# The entry candle is the candle AFTER the one that is forming (the session publishes the signal ~16 s before).
ENGINE_NAME = "DARKMATTER_ENGINE_V3"

try:
    _BASE = EngineConfig(**ENGINE_CONFIG)
except TypeError as _e:                                   # unknown name in signal_settings.ENGINE_CONFIG
    raise ValueError(f"ENGINE_CONFIG in signal_settings.py is wrong: {_e}") from None

# Name of every on/off switch of the strategy that can take part in the signal -> text used in the reason.
# (Filtro_Tendencia and HILL are left out on purpose: in the original they never influence the signal.)
_FILTER_LABELS = {
    "COMBINER": "COMBINER 1 indicator", "COMBINER2": "COMBINER 2 indicator", "COMBINER3": "COMBINER 3 indicator",
    "COMBINER4": "COMBINER 4 indicator", "MILBURNEstrategia": "RSI 95% (RSI + support / resistance + candle)",
    "strategy1_Enabled": "HULK SECRET REVERSAL", "KEN_Enabled": "KEN", "candlestrtegy": "CANDLE STRATEGY",
    "Pulback_Enabled": "PULLBACK", "EXTREME": "EXTREME", "secret": "TIME-TURN", "strategy9_Enabled": "STRATEGY 3",
    "FLOW": "FLOW", "Reversal_Enabled": "REVERSAL", "priceaction": "PRICE ACTION", "HUNTER_Enabled": "HUNTER",
    "KILLER_Enabled": "KILLER", "BERSERK": "BERSERK", "hulk_Enabled": "HULK", "professor_Enabled": "PROFESSOR",
    "beast_Enabled": "BEAST", "sniper_Enabled": "SNIPER", "ninja_Enabled": "NINJA", "hacker_Enabled": "HACKER",
    "shadow": "SHADOW", "Value_Enabled": "VALUE CHART", "BAND": "FAST MA BAND", "BB_Enabled": "BOLLINGER BANDS",
    "SO_Enabled": "STOCHASTIC", "AtivarCruzamento": "EMA CROSS", "EnabledRVI": "RVI", "EnabledMFI": "MFI",
    "EnabledWPR": "WPR", "AtivarMACD": "MACD", "SeR": "SUPPORT & RESISTANCE", "AtivaDonchian": "DONCHIAN",
    "Cci_Enabled": "CCI", "Adx_Enabled": "ADX", "ativar_donforex": "DONFOREX ZONES",
    "FILT_RET_": "RETRACTION FILTER", "Bloquea": "SAME-COLOUR BLOCK", "FiltroVelas": "CANDLE FILTER",
    "AtivarTamanhoVela": "MAX CANDLE SIZE", "AtivarTamanhoVela1": "MIN CANDLE SIZE",
}
ACTIVE_FILTERS = tuple(label for key, label in _FILTER_LABELS.items() if getattr(_BASE, key, False))

_MISSING_HOOKS = [i for i, key in ((1, "COMBINER"), (2, "COMBINER2"), (3, "COMBINER3"), (4, "COMBINER4"))
                  if getattr(_BASE, key) and i not in ENGINE_COMBINERS]
# Shown in the header and as the skip reason of every market. None = the engine can run.
ENGINE_PROBLEM = (f"COMBINER {', '.join(map(str, _MISSING_HOOKS))} is switched ON but no indicator is installed "
                  "(ENGINE_COMBINERS in signal_settings.py) - the strategy cannot give any signal."
                  if _MISSING_HOOKS else None)
_INTERVAL = max(1, int(_BASE.Intervalo))      # candles between two signals (the strategy's own spacing rule)
if not (MIN_CONF <= SCORE_MIN < SCORE_MAX <= 100):
    raise ValueError("signal_settings: need MIN_CONF <= SCORE_MIN < SCORE_MAX <= 100, "
                     "otherwise the score could remove signals")
_SCORE_W = {k: float(v) for k, v in SCORE_WEIGHTS.items() if float(v) > 0}
_MIN_WARM = 10                                # candles that must stay older than the first walked candle


def _engine_candles(snap, cur):
    """The candle list the strategy expects (oldest -> newest, forming candle last)."""
    w = IND_WINDOW
    t, v = snap["t"][-w:] + [cur["t"]], snap["v"][-w:] + [cur.get("v")]
    o, h = snap["o"][-w:] + [cur["o"]], snap["h"][-w:] + [cur["h"]]
    l, c = snap["l"][-w:] + [cur["l"]], snap["c"][-w:] + [cur["c"]]
    candles = [{"time": t[k], "open": o[k], "high": h[k], "low": l[k], "close": c[k], "volume": v[k] or 0}
               for k in range(len(c))]
    return candles, o, h, l, c


def _engine_signal(candles, digits):
    """DARKMATTER_ENGINE_V3 on the forming (newest) candle -> "CALL" / "PUT" / None."""
    n = len(candles)
    # The walk never starts on the oldest candle: the strategy looks 2 candles back when it locks an entry
    # candle, and with no candle there the signal spacing breaks (MT4 always has that history).
    limit = max(0, min(ENGINE_LIVE_BARS, n - 1 - _MIN_WARM))
    limit -= limit % _INTERVAL     # the signal spacing is counted from the first walked candle, so that candle is
    #                                kept on the spacing grid: the newest candle can be a signal candle, as in MT4
    cfg = dataclasses.replace(_BASE, Velas=limit, warmup=max(0, min(ENGINE_WARMUP, n - 1 - limit)),
                              digits=digits, period_minutes=1)
    hooks = {idx: factory(candles) for idx, factory in ENGINE_COMBINERS.items()}
    rows = DarkmatterEngineV3(cfg, combiners=hooks).run(candles)
    last = rows[-1] if rows else None
    return last["signal"] if last is not None and last["shift"] == 0 else None


def _cooldown_ok(last, t, tf=60):
    """May a signal be published on the candle that opens at time t? `last` = (candle time, direction) of the
    market's latest published signal. Same spacing as the strategy: Intervalo candles."""
    return not last or (t - last[0]) // tf >= _INTERVAL


def _efficiency_ratio(cc, n=20):
    """Kaufman efficiency ratio over the last n closed candles (0 = pure noise, 1 = straight line)."""
    if len(cc) < n + 2:
        return 0.5
    path = sum(abs(cc[i] - cc[i - 1]) for i in range(len(cc) - n, len(cc)))
    return abs(cc[-1] - cc[-n - 1]) / path if path > 0 else 0.0


def _c01(x):
    """Clamp to 0..1 (NaN counts as 0)."""
    return 0.0 if not x > 0 else 1.0 if x > 1 else float(x)


def _history_rate(o, c, red, want_up):
    """Share of this market's CLOSED candles of the live candle's colour after which the next candle went the way
    the signal wants, pulled towards 50 % by HISTORY_SHRINK. -> (rate, samples)."""
    hit = tot = 0
    for i in range(len(c) - 1):
        if c[i] == o[i] or c[i + 1] == o[i + 1] or (c[i] < o[i]) != red:
            continue
        tot += 1
        hit += (c[i + 1] > o[i + 1]) == want_up
    return (hit + 0.5 * HISTORY_SHRINK) / (tot + HISTORY_SHRINK), tot


def _score(o, h, l, c, d, atr, sup, res_lv, er, gap):
    """Setup quality 0..1 of a signal that already exists, plus the sentences for the strongest factors.
    d = +1 CALL / -1 PUT, o/h/l/c include the forming candle (last), `gap` = (EMA20 - EMA50) / ATR.
    Every factor is 0..1 and only ranks markets against each other; nothing here can switch a signal off."""
    rev = _BASE.DirectionSignal == REVERSE
    up = d > 0
    price, a = c[-1], max(atr, 1e-12)
    parts = {}                                              # name -> (value 0..1, sentence)

    r = _rsi_series(c, 7)[-1]
    if r is not None:
        x = (50.0 - r) / 30.0 if up == rev else (r - 50.0) / 30.0   # reversal CALL: low RSI; flow CALL: high RSI
        parts["RSI"] = (_c01(x), f"RSI(7) is {'oversold' if r < 50 else 'overbought'} at {r:.0f}")
    if rev:
        w = c[-20:]
        m = sum(w) / len(w)
        sd = (sum((v - m) ** 2 for v in w) / len(w)) ** 0.5
        z = (price - m) / sd if sd > 0 else 0.0
        parts["BAND"] = (_c01(-z * d / 2.5), f"price is {abs(z):.1f} standard deviations "
                                              f"{'below' if z < 0 else 'above'} its 20-candle average")
        hi, lo = max(h[-12:]), min(l[-12:])
        pos = (price - lo) / (hi - lo) if hi > lo else 0.5
        parts["SWING"] = (_c01((0.35 - pos) / 0.35 if up else (pos - 0.65) / 0.35),
                          f"price sits at the {'bottom' if up else 'top'} of its last 12 candles")
        rng = h[-1] - l[-1]
        if rng > 0:
            wick = (min(o[-1], c[-1]) - l[-1]) if up else (h[-1] - max(o[-1], c[-1]))
            parts["WICK"] = (_c01(2.0 * wick / rng),
                             f"the live candle shows a long {'lower' if up else 'upper'} wick "
                             f"({'buyers' if up else 'sellers'} stepping in)")
        lvl = (price - sup) / a if up else (res_lv - price) / a
        parts["LEVEL"] = (_c01(1.0 - lvl / 1.5), f"price is close to {'support' if up else 'resistance'}")
    parts["BODY"] = (_c01(abs(c[-1] - o[-1]) / (1.5 * a)), "the live candle body is large compared with normal movement")
    aligned = d * gap
    if er >= 0.30:                                          # trending market
        x = 0.9 if (aligned > 0) == rev else 0.1            # reversal: a pullback WITH the trend is best
        txt = "a trending market, " + ("with the pullback running along the trend" if aligned > 0 else
                                       "the move is against the trend")
        if not rev:
            x, txt = (0.9, "a trending market and the move follows it") if aligned > 0 else \
                     (0.1, "a trending market but the move goes against it")
    elif er <= 0.15:
        x, txt = (0.8, "a ranging market, where reversals work best") if rev else (0.2, "a ranging market")
    else:
        x, txt = 0.5, "a mixed market"
    parts["REGIME"] = (x, txt)
    rate, n = _history_rate(o[:-1], c[:-1], c[-1] < o[-1], up)
    parts["HISTORY"] = (_c01((rate - 0.4) / 0.2),
                        f"this market went {'up' if up else 'down'} after {100 * rate:.0f}% of its recent "
                        f"{'red' if c[-1] < o[-1] else 'green'} candles ({n} seen)")

    used = [(k, v[0], _SCORE_W[k], v[1]) for k, v in parts.items() if k in _SCORE_W]
    tw = sum(w for _, _, w, _ in used)
    score = sum(x * w for _, x, w, _ in used) / tw if tw > 0 else 0.5
    best = sorted((u for u in used if u[1] >= 0.6), key=lambda u: -u[1] * u[2])[:3]
    return _c01(score), [t for _, _, _, t in best]


def strength_label(conf_int):
    if conf_int >= 85:
        return "VERY STRONG", "\U0001F7E2"
    if conf_int >= 75:
        return "STRONG", "\U0001F7E2"
    if conf_int >= 65:
        return "MODERATE", "\U0001F7E1"
    return "WEAK", "\U0001F7E0"


def _blank_result(symbol, skip=None):
    return {"symbol": symbol, "skip": skip, "dir": 0, "conf": 0.0, "trend": "SIDEWAYS", "regime": "MIXED",
            "candle": None, "drivers": [], "filters": ACTIVE_FILTERS, "price": None, "support": None, "resistance": None,
            "atr": None, "adx": None, "dec": 2}


def analyze_snapshot(snap):
    """Run the strategy on one market (closed candles + the forming candle) -> direction + market context."""
    res = _blank_result(snap["symbol"])
    res["dec"] = snap["dec"]
    cur = snap["cur"]
    if len(snap["c"]) < MIN_BARS:
        res["skip"] = "not enough candles"
        return res
    if cur is None or snap["tick_age"] > STALE_SEC or snap["bucket"] - cur["t"] >= 60:
        res["skip"] = "no live ticks"
        return res
    if ENGINE_PROBLEM:
        res["skip"] = ENGINE_PROBLEM
        return res
    candles, o, h, l, c = _engine_candles(snap, cur)
    price = cur["c"]
    res["price"] = price
    atr = _atr_series(h[:-1], l[:-1], c[:-1], 14)[-1]
    if atr < price * 2e-6:
        res["skip"] = "flat market"
        return res

    verdict = _engine_signal(candles, snap["dec"])
    if verdict is None:
        res["skip"] = "no setup"
        return res
    d = 1 if verdict == "CALL" else -1
    if not _cooldown_ok(snap.get("last_sig"), cur["t"]):
        res["skip"] = "cooling down"
        return res

    # market context (shown in the signal; the strategy itself does not depend on it)
    a = max(atr, price * 1e-6, 1e-12)
    er = _efficiency_ratio(c[:-1])
    regime = "TREND" if er >= 0.30 else "RANGE" if er <= 0.15 else "MIXED"
    gap = (_ema(c, 20)[-1] - _ema(c, 50)[-1]) / a
    trend = "BULLISH" if gap >= 0.5 else "BEARISH" if gap <= -0.5 else "SIDEWAYS"
    sup, _, res_lv, _ = _sr_levels(h[:-1], l[:-1], a, price)
    if sup is None:
        sup = min(min(l[-61:-1]), price - a)
    if res_lv is None:
        res_lv = max(max(h[-61:-1]), price + a)
    try:
        score, drivers = _score(o, h, l, c, d, a, sup, res_lv, er, gap)
    except Exception:                                    # a scoring problem must never cost a signal
        score, drivers = 0.5, []
    res.update(dir=d, conf=SCORE_MIN + (SCORE_MAX - SCORE_MIN) * score, drivers=drivers, trend=trend,
               regime=regime, atr=atr, support=sup, resistance=res_lv,
               candle="red" if cur["c"] < cur["o"] else "green")
    return res


def analyze_many(snaps):
    out = []
    for s in snaps:
        try:
            out.append(analyze_snapshot(s))
        except Exception as e:                           # never let one market kill the whole scan
            out.append(_blank_result(s.get("symbol", "?"), f"{type(e).__name__}: {e}"))
    return out


def _join_names(names):
    names = list(names)
    return names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]


def build_reason(res, fmt):
    """Plain-English explanation of a signal: what the strategy saw on the live candle and which of its
    confirmations are switched on (a signal only exists when all of them agree)."""
    call = res["dir"] > 0
    colour = res.get("candle") or ("red" if call else "green")
    side = "downside" if colour == "red" else "upside"
    sents = []
    if _BASE.DirectionSignal == REVERSE:
        sents.append(f"{ENGINE_NAME} reversal setup: the live candle is a {colour} candle pushing to the {side}, "
                     f"and the strategy trades the move back, {'a bounce up' if call else 'a pullback down'}.")
    else:
        sents.append(f"{ENGINE_NAME} flow setup: the live candle is a {colour} candle pushing to the {side}, "
                     f"and the strategy trades with that move, {'up' if call else 'down'}.")

    filters = res.get("filters") or ()
    if filters:
        n = len(filters)
        sents.append(f"The active confirmation agrees: {filters[0]}." if n == 1
                     else f"All {n} active confirmations agree: {_join_names(filters)}.")
    else:
        sents.append("No extra confirmation filter is switched on, so the signal rests on the live candle and "
                     f"the {_INTERVAL}-candle spacing between signals.")

    if res.get("drivers"):
        sents.append(f"Setup score {int(res['conf'] + 0.5)}: " + "; ".join(res["drivers"]) + ".")
    else:
        sents.append(f"Setup score {int(res['conf'] + 0.5)}: no single factor stands out, "
                     "the signal rests on the strategy trigger.")

    t_sign = 1 if res["trend"] == "BULLISH" else -1 if res["trend"] == "BEARISH" else 0
    if t_sign and t_sign != res["dir"]:
        sents.append(f"The entry goes against the {res['trend'].lower()} trend, so it is a counter-trend trade.")
    elif t_sign:
        sents.append(f"The entry is in the direction of the {res['trend'].lower()} trend.")
    elif res["regime"] == "RANGE":
        sents.append("The market is moving sideways in a range.")

    sup, rsi_ = res.get("support"), res.get("resistance")
    if sup is not None and rsi_ is not None:
        work, goal = (sup, rsi_) if call else (rsi_, sup)
        sents.append(f"Working level is {fmt(work)}, with {fmt(goal)} as the next level if it plays out.")
    return " ".join(sents)


# =====================================================================
#  module: darkmatter/signals/messages.py  -  LIVE SIGNAL: session statistics and the three message formats.
# =====================================================================
# =====================================================================
#  LIVE SIGNAL  -  session statistics and the three message formats
# =====================================================================
class SessionStats:
    def __init__(self, mtg):
        self.mtg = mtg
        self.log = []                 # {t, symbol, dir, win, step}
        self.pairs = {}               # symbol -> [wins, losses]

    def add(self, symbol, direction, entry_ts, win, step, payout=None):
        self.log.append({"t": entry_ts, "symbol": symbol, "dir": direction, "win": bool(win), "step": step,
                         "payout": payout})
        self.pairs.setdefault(symbol, [0, 0])[0 if win else 1] += 1

    @property
    def wins(self):
        return sum(1 for e in self.log if e["win"])

    @property
    def losses(self):
        return len(self.log) - self.wins

    def pair(self, symbol):
        w, l = self.pairs.get(symbol, [0, 0])
        return w, l


def _pct(w, l):
    return 100.0 * w / (w + l) if (w + l) else 0.0


def _hhmm(ts, tz):
    return datetime.fromtimestamp(ts, tz).strftime("%H:%M")


LINE = "\u2501" * 22


def is_otc(symbol):
    """Same rule as the market list: Quotex OTC markets end in _otc."""
    return str(symbol).lower().endswith("_otc")


LABEL_SEP = "  :  "


def frow(icon, label, value, vkind):
    """One `ICON LABEL  :  value` line. The text is the same as ever; only the terminal colours differ:
    the label part is white, the value after the colon gets its own colour."""
    head = f"{icon} {label}{LABEL_SEP}"
    return (head + value, ("seg", [(head, "white"), (value, vkind)]))


def seg_row(*parts):
    """A line built from (text, colour-kind) pieces; its text is the pieces joined."""
    return ("".join(t for t, _ in parts), ("seg", list(parts)))


def _pct_txt(w, l):
    return f"{_pct(w, l):.1f}%"


MONO_UP, MONO_LO, MONO_DIG = 0x1D670, 0x1D68A, 0x1D7F6


def mono(text):
    """ASCII letters / digits -> Unicode monospace letters (the font of the PARTIAL RESULT message)."""
    out = []
    for ch in text:
        o = ord(ch)
        if 65 <= o <= 90:
            out.append(chr(MONO_UP + o - 65))
        elif 97 <= o <= 122:
            out.append(chr(MONO_LO + o - 97))
        elif 48 <= o <= 57:
            out.append(chr(MONO_DIG + o - 48))
        else:
            out.append(ch)
    return "".join(out)


def signal_rows(sig, cfg, tz):
    """The SIGNAL message as (text, kind) rows; kind only decides the terminal colour."""
    dec = sig["dec"]
    fmt = lambda v: f"{v:.{dec}f}"
    call = sig["dir"] > 0
    conf = int(sig["conf"] + 0.5)
    label, dot_ = strength_label(conf)
    tier = {"VERY STRONG": "ok", "STRONG": "ok", "MODERATE": "warn", "WEAK": "bad"}[label]
    trend_icon = {"BULLISH": "\U0001F4C8", "BEARISH": "\U0001F4C9"}.get(sig["trend"], "\u2194\ufe0f")
    trend_kind = {"BULLISH": "ok", "BEARISH": "bad"}.get(sig["trend"], "warn")
    mtg = cfg["mtg"]
    mtg_txt = "NO MTG" if mtg == 0 else (f"STEP {mtg} IF REQUIRED" if mtg == 1 else f"UP TO STEP {mtg} IF REQUIRED")
    payout = "-" if sig["payout"] is None else f"{sig['payout']:g}%"
    return [
        (LINE, "sep"),
        ("\U0001F451 DARKMATTER-BOT SIGNAL \U0001F451", "title"),
        (LINE, "sep"),
        frow("\U0001F4CA", "ASSET", sig["symbol"], "white"),
        frow(trend_icon, "TREND", sig["trend"].title(), trend_kind),
        frow("\U0001F53A" if call else "\U0001F53B", "DIRECTION", "CALL \u2191" if call else "PUT \u2193",
             "ok" if call else "bad"),
        frow("\u23f1", "TIMEFRAME", "M1", "white"),
        frow("\U0001F4DF", "PRICE", fmt(sig["price"]), "white"),
        frow("\u23f0", "ENTRY", _hhmm(sig["entry_ts"], tz), "yel"),
        frow("\u26a1", "STRENGTH", f"{label.title()} {conf}% {dot_}", tier),
        frow("\U0001F6A8", "MTG", mtg_txt, "white"),
        frow("\U0001F48E", "PAYOUT", payout, "ok"),
        (LINE, "sep"),
        frow("\U0001F6E1", "SUPPORT", fmt(sig["support"]), "ok"),
        frow("\U0001F6A7", "RESISTANCE", fmt(sig["resistance"]), "bad"),
        (LINE, "sep"),
        frow("\U0001F451", "OWNER", cfg["owner"], "mag"),
        (LINE, "sep"),
        ("", "blank"),
        ("\U0001F4DD REASON", "head"),
        (sig["reason"], "reason"),
    ]


def result_rows(r, cfg, tz):
    """The RESULT message as (text, kind) rows."""
    dec = r["dec"]
    fmt = lambda v: f"{v:.{dec}f}"
    call = r["dir"] > 0
    win, step = r["win"], r["step"]
    green = r["close"] > r["open"]
    w, l = r["wins"], r["losses"]
    pw, pl = r["pair"]
    suffix = f" (MTG{step})" if step > 0 else ""
    k = "ok" if win else "bad"
    return [
        (LINE, "sep"),
        ("\U0001F451 DARKMATTER-BOT RESULT \U0001F451", "title"),
        (LINE, "sep"),
        frow("\U0001F4CA", "ASSET", r["symbol"], "white"),
        frow("\u23f0", "ENTRY", _hhmm(r["entry_ts"], tz), "yel"),
        frow("\U0001F3AF", "DIRECTION", "CALL \u2191" if call else "PUT \u2193", "ok" if call else "bad"),
        frow("\U0001F50D", "RESULT", ("\u2705\u2705 WIN" if win else "\u274c\u274c LOSS") + suffix, k),
        (LINE, "sep"),
        frow("\u269c\ufe0f", "OPEN PRICE", fmt(r["open"]), "white"),
        frow("\u2694\ufe0f", "CLOSE PRICE", fmt(r["close"]), "white"),
        frow("\U0001F530", "CANDLE COLOUR", "\U0001F7E2 GREEN" if green else "\U0001F534 RED",
             "ok" if green else "bad"),
        seg_row(("\U0001F680 WIN : ", "white"), (str(w), "ok"), ("  |  LOSS : ", "white"), (str(l), "bad"),
                (f"  ({_pct_txt(w, l)})", "white")),
        frow("\U0001F525", "CURRENT PAIR", f"{pw}x{pl}  ({_pct_txt(pw, pl)})", "yel"),
        (LINE, "sep"),
        frow("\u2699\ufe0f", "STATUS", "Succeed \u2705" if win else "Failed \u274c", k),
        (LINE, "sep"),
    ]


def session_rows(stats, tz, now_ts):
    """The PARTIAL RESULT message in Unicode monospace letters. In the terminal everything is white, only
    BUY is green and PUT is red."""
    sup = "\u00b9\u00b2\u00b3"
    entries = sorted(stats.log, key=lambda x: x["t"])

    def line(text):
        return (mono(text), "plain")

    def market_block(title, items, otc):
        rows = [line(f"\u274f {title} :"), (LINE, "sep")]
        if not items:
            rows.append(line(f"{title.replace('-', ' ')} NOT FOUND"))
        for e in items:
            mark = ("\u2705" + (sup[e["step"] - 1] if 1 <= e["step"] <= 3 else "")) if e["win"] else "\u274c"
            name = short_symbol(e["symbol"]) + ("-OTC" if otc else "")
            side = "BUY" if e["dir"] > 0 else "PUT"
            rows.append(seg_row((mono(f"\u2752{_hhmm(e['t'], tz)}-{name}-"), "plain"),
                                (mono(side), "ok" if side == "BUY" else "bad"),
                                (mark, "plain")))
        rows.append((LINE, "sep"))
        return rows

    rows = [
        (LINE, "sep"),
        line("\U0001F451 DARKMATTER-BOT \U0001F451"),
        line("\u26ab\ufe0f PARTIAL RESULT \u26ab\ufe0f"),
        (LINE, "sep"),
        line(f"\U0001F4C6 DATE : {datetime.fromtimestamp(now_ts, tz).strftime('%Y.%m.%d')}"),
        (LINE, "sep"),
    ]
    rows += market_block("REAL MARKET", [e for e in entries if not is_otc(e["symbol"])], False)
    rows += market_block("OTC-MARKET", [e for e in entries if is_otc(e["symbol"])], True)
    rows.append(line("\U0001F538 GALE Results:"))
    for k in range(stats.mtg + 1):
        w = sum(1 for e in stats.log if e["win"] and e["step"] == k)
        l = sum(1 for e in stats.log if not e["win"] and e["step"] == k)
        rows.append(line(f"\u25aa\ufe0f GALE {k}\u2192{w}x{l} \u22c5\u25c8\u22c5 ({int(_pct(w, l) + 0.5)}%)"))
    w, l = stats.wins, stats.losses
    rows += [
        (LINE, "sep"),
        line(f"\U0001F4CA Total Rate: {w}x{l}\u22c5\u25c8\u22c5 ({int(_pct(w, l) + 0.5)}%)"),
        (LINE, "sep"),
        line("\u2699\ufe0f Platform: DARKMATTER \u26ab\ufe0f"),
        (LINE, "sep"),
    ]
    return rows


def _join(rows):
    return "\n".join(t for t, _ in rows)


def build_signal_text(sig, cfg, tz):
    return _join(signal_rows(sig, cfg, tz))


def build_result_text(r, cfg, tz):
    return _join(result_rows(r, cfg, tz))


def build_session_text(stats, tz, now_ts):
    return _join(session_rows(stats, tz, now_ts))


# =====================================================================
#  module: darkmatter/notify/telegram_notify.py  -  TELEGRAM: optional delivery of signals / results / session result (standard library only).
# =====================================================================
# =====================================================================
#  TELEGRAM  -  optional delivery of signals / results / session result (standard library only)
# =====================================================================
TG_API = "https://api.telegram.org"
TG_TIMEOUT = 15          # seconds for one HTTP request
TG_PHOTO_TIMEOUT = 45    # seconds for one picture upload
TG_RETRIES = 3           # attempts per message
TG_MAX_LEN = 4096        # Telegram's limit for one message
TG_SAFE_LEN = 3600       # a longer text is split into parts of at most this many characters
TG_FLUSH_WAIT = 40.0     # seconds to wait for queued messages (and their chart images) when the session ends

_TG_TOKEN_RE = re.compile(r"[0-9]{5,15}:[A-Za-z0-9_-]{20,60}")
_TG_CHAT_RE = re.compile(r"-?[0-9]{5,20}|@[A-Za-z][A-Za-z0-9_]{4,31}")
TG_STATE = {}            # token / chat remembered for the rest of this run (never written to disk)


class TelegramError(Exception):
    def __init__(self, msg, code=None, retry_after=None, network=False):
        super().__init__(msg)
        self.code, self.retry_after, self.network = code, retry_after, network


def parse_tg_token(text):
    t = text.strip()
    return t if _TG_TOKEN_RE.fullmatch(t) else None


def parse_tg_chat(text):
    t = text.strip()
    return t if _TG_CHAT_RE.fullmatch(t) else None


def parse_on_off(text, default):
    t = text.strip().lower()
    if not t:
        return default
    if t in ("on", "y", "yes", "1", "true"):
        return True
    if t in ("off", "n", "no", "0", "false"):
        return False
    return None


def tg_mask(token):
    head, _, tail = token.partition(":")
    return f"{head}:{tail[:3]}\u2026{tail[-3:]}" if len(tail) > 8 else "***"


def _tg_request(token, method, data, content_type, base=TG_API, timeout=TG_TIMEOUT):
    """One Bot API request (blocking, run it in a thread). The token never appears in an error text."""
    url = f"{base}/bot{token}/{method}"
    req = urllib.request.Request(url, data=data, headers={"Content-Type": content_type})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
    except urllib.error.HTTPError as e:              # Telegram answers errors with a JSON body
        try:
            body = e.read()
        except Exception:
            body = b""
    except Exception as e:                           # no internet, DNS, TLS, timeout, ...
        why = str(getattr(e, "reason", None) or e).replace(token, "***")
        raise TelegramError(f"{type(e).__name__}: {why}"[:160], network=True) from None
    try:
        res = json.loads(body.decode("utf-8", "replace"))
        if not isinstance(res, dict):
            raise ValueError
    except ValueError:
        raise TelegramError("unreadable reply from Telegram", network=True) from None
    if res.get("ok"):
        return res.get("result")
    params = res.get("parameters") or {}
    raise TelegramError(str(res.get("description") or "request failed").replace(token, "***"),
                        code=res.get("error_code"), retry_after=params.get("retry_after"))


def _tg_call(token, method, payload, base=TG_API, timeout=TG_TIMEOUT):
    """JSON request (sendMessage, getMe, ...)."""
    return _tg_request(token, method, json.dumps(payload).encode("utf-8"), "application/json; charset=utf-8",
                       base, timeout)


def _tg_photo(token, chat_id, png, caption, parse_mode, base=TG_API, timeout=TG_PHOTO_TIMEOUT):
    """sendPhoto with a multipart upload (no extra library needed)."""
    boundary = "----DarkMatter" + uuid.uuid4().hex
    parts = []
    fields = {"chat_id": str(chat_id), "caption": caption}
    if parse_mode:
        fields["parse_mode"] = parse_mode
    for k, v in fields.items():
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n'.encode("utf-8")
                     + v.encode("utf-8") + b"\r\n")
    parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="photo"; filename="chart.png"\r\n'
                 f"Content-Type: image/png\r\n\r\n".encode("utf-8") + png + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode("utf-8"))
    return _tg_request(token, "sendPhoto", b"".join(parts), f"multipart/form-data; boundary={boundary}",
                       base, timeout)


async def tg_probe(token, method, payload, base=TG_API):
    """-> (result, error_text, is_network_error). Used by the setup screens."""
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None, _tg_call, token, method, payload, base), None, False
    except TelegramError as e:
        return None, str(e), e.network


class TelegramNotifier:
    """Sends messages from a background queue: the signal engine never waits for Telegram, the order of the
    messages is kept, temporary errors (network, 429, 5xx) are retried, and a text Telegram cannot parse as
    HTML is resent as plain text."""

    def __init__(self, token, chat_id, base=TG_API, on_error=None):
        self.token, self.chat_id, self.base, self.on_error = token, chat_id, base, on_error
        self.queue = None
        self.task = None
        self.pool = None
        self.sent = 0
        self.failed = 0

    def start(self):
        self.queue = asyncio.Queue()
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="telegram")
        self.task = asyncio.create_task(self._worker())

    def send(self, messages):
        """messages: list of (html, plain) or (html, plain, render). `render` is a function that returns PNG bytes;
        it runs in the background thread and the picture goes out with the text as its caption. Returns at once."""
        if self.queue is None:
            return
        for m in messages:
            self.queue.put_nowait(m)

    def _fail(self, why):
        self.failed += 1
        if self.on_error:
            try:
                self.on_error(why)
            except Exception:
                pass

    async def _call(self, method, payload):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.pool, _tg_call, self.token, method, payload, self.base)

    async def _deliver_photo(self, png, html_text, plain):
        """-> True when the picture was delivered. False: it could not be (the caller then sends the text alone)."""
        loop = asyncio.get_running_loop()
        use_html, last, attempt = True, "unknown error", 0
        while attempt < TG_RETRIES:
            attempt += 1
            try:
                await loop.run_in_executor(self.pool, _tg_photo, self.token, self.chat_id, png,
                                           html_text if use_html else plain, "HTML" if use_html else "", self.base)
                self.sent += 1
                return True
            except TelegramError as e:
                last = str(e)
                if e.code == 400 and use_html and "parse" in last.lower():
                    use_html = False
                    attempt -= 1
                    continue
                if e.code == 429:
                    await asyncio.sleep(min(float(e.retry_after or 1) + 0.5, 30.0))
                    continue
                if e.network or (e.code is not None and e.code >= 500):
                    await asyncio.sleep(1.5 * attempt)
                    continue
                break
        self._fail(f"chart image not sent ({last}) - the text goes out without it"[:200])
        return False

    async def _deliver(self, html_text, plain, render=None):
        if render is not None:
            png = None
            try:
                loop = asyncio.get_running_loop()
                png = await loop.run_in_executor(self.pool, render)
            except asyncio.CancelledError:
                raise
            except Exception as e:                   # drawing failed: the text must still reach the channel
                self._fail(f"chart image skipped ({type(e).__name__}: {e})"[:200])
            if png and await self._deliver_photo(png, html_text, plain):
                return
        use_html, last, attempt = True, "unknown error", 0
        while attempt < TG_RETRIES:
            attempt += 1
            payload = {"chat_id": self.chat_id}
            if use_html:
                payload["text"], payload["parse_mode"] = html_text, "HTML"
            else:
                payload["text"] = plain
            try:
                await self._call("sendMessage", payload)
                self.sent += 1
                return
            except TelegramError as e:
                last = str(e)
                if e.code == 400 and use_html and "parse" in last.lower():
                    use_html = False                 # formatting refused: resend as plain text
                    attempt -= 1
                    continue
                if e.code == 429:
                    await asyncio.sleep(min(float(e.retry_after or 1) + 0.5, 30.0))
                    continue
                if e.network or (e.code is not None and e.code >= 500):
                    await asyncio.sleep(1.5 * attempt)
                    continue
                break                                # 400 / 401 / 403 / 404: retrying cannot help
        self._fail(last)

    async def _worker(self):
        while True:
            item = await self.queue.get()
            try:
                await self._deliver(*item)
            except asyncio.CancelledError:
                raise
            except Exception as e:                   # one bad message must never stop the worker
                self._fail(f"{type(e).__name__}: {e}")
            finally:
                self.queue.task_done()

    async def close(self, timeout=TG_FLUSH_WAIT):
        """Wait (up to `timeout` s) until everything queued has been sent, then stop the worker."""
        if self.queue is None:
            return
        try:
            await asyncio.wait_for(self.queue.join(), timeout)
        except asyncio.TimeoutError:
            self._fail("some messages were still waiting when the session closed")
        finally:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.pool.shutdown(wait=False)
            self.queue = None


def _tg_esc(text):
    return html.escape(text, quote=False)


def _tg_emoji(text):
    """Telegram draws these two as colour emoji only with the emoji selector (as in the supplied format)."""
    return re.sub("([\u26a1\u23f1])(?!\ufe0f)", "\\1\ufe0f", text)


def _u16(text):
    """Telegram counts UTF-16 units (the monospace letters and most emoji are 2 units each)."""
    return len(text.encode("utf-16-le", "surrogatepass")) // 2


def _tg_chunks(text, limit=TG_SAFE_LEN):
    parts, cur = [], ""
    for ln in text.split("\n"):
        while _u16(ln) > limit:                      # a single endless line: cut it
            if cur:
                parts.append(cur)
                cur = ""
            parts.append(ln[:limit // 2])
            ln = ln[limit // 2:]
        if cur and _u16(cur) + 1 + _u16(ln) > limit:
            parts.append(cur)
            cur = ln
        else:
            cur = f"{cur}\n{ln}" if cur else ln
    if cur:
        parts.append(cur)
    return parts or [""]


def tg_signal_messages(sig, cfg, tz, render=None):
    """SIGNAL: the block in bold, the REASON paragraph in a collapsed (expandable) quote. With a chart `render` the
    picture carries the text as its caption; when the whole text is longer than a caption may be (1024), the
    picture gets the bold block and the REASON follows as the next message."""
    rows = signal_rows(sig, cfg, tz)
    head = _tg_emoji(_join([r for r in rows if r[1] != "reason"])).rstrip()
    reason = " ".join(t for t, k in rows if k == "reason").strip()
    room = TG_MAX_LEN - _u16(head) - 20
    if _u16(reason) > room:
        cut = reason[:max(0, room - 1)]
        while cut and _u16(cut) > room - 1:
            cut = cut[:-10]
        reason = cut.rstrip() + "\u2026"
    head_html = f"<b>{_tg_esc(head)}</b>"
    full_html, full_plain = head_html, head
    if reason:
        full_html += f"\n<blockquote expandable>{_tg_esc(reason)}</blockquote>"
        full_plain += "\n" + reason
    if render is None:
        return [(full_html, full_plain)]
    if _u16(full_plain) + 2 <= CH_PHOTO_CAPTION:
        return [(full_html, full_plain, render)]
    if _u16(head) + 2 > CH_PHOTO_CAPTION:                      # cannot be a caption at all: text only
        return [(full_html, full_plain)]
    msgs = [(head_html, head, render)]
    if reason:
        msgs.append((f"<blockquote expandable>{_tg_esc(reason)}</blockquote>", reason))
    return msgs


def tg_result_messages(r, cfg, tz, render=None):
    """RESULT: everything in bold (the chart image, when there is one, carries it as its caption)."""
    text = _tg_emoji(build_result_text(r, cfg, tz)).rstrip()
    html_text = f"<b>{_tg_esc(text)}</b>"
    if render is not None and _u16(text) + 2 <= CH_PHOTO_CAPTION:
        return [(html_text, text, render)]
    return [(html_text, text)]


def tg_session_messages(stats, tz, now_ts):
    """PARTIAL RESULT: bold (split into several messages only when it is very long)."""
    text = _tg_emoji(build_session_text(stats, tz, now_ts)).rstrip()
    return [(f"<b>{_tg_esc(part)}</b>", part) for part in _tg_chunks(text)]


TG_BAR = "\u2501" * 30


def _tg_banner(title, render):
    text = f"{TG_BAR}\n\U0001F518\u2503 {title}\u2503 \U0001F518\n{TG_BAR}"
    html_text = f"<b>{_tg_esc(text)}</b>"
    return [(html_text, text, render)] if render is not None else [(html_text, text)]


def tg_start_messages(render=None):
    """Sent when the live session starts: the bold BE READY banner (+ the session overview picture)."""
    return _tg_banner("BE READY - LIVE SESSION STARTING", render)


def tg_end_messages(render=None):
    """Sent after the PARTIAL RESULT of Ctrl+S / S: the bold SESSION END banner (+ the session summary picture)."""
    return _tg_banner("SUCCESSFULLY - LIVE SESSION END", render)


# =====================================================================
#  module: darkmatter/notify/charts_session.py  -  CHARTS: SESSION OVERVIEW (start) and SESSION SUMMARY (end) images.
# =====================================================================
#  session dashboards: SESSION OVERVIEW (start) and SESSION SUMMARY (end), 1800 x 900
# ---------------------------------------------------------------------
def session_id(start_ts, tz):
    return "DM-" + datetime.fromtimestamp(start_ts, tz).strftime("%Y%m%d-%H%M")


def session_equity(log):
    """Replays the finished signals on a virtual account that risks CH_STAKE_PCT % of its balance on every
    signal (a win pays the market's payout, a loss costs the stake; an MTG chain counts as one signal).
    -> (balances incl. the start, per-signal list of (win, payout))."""
    entries = sorted(log, key=lambda e: e["t"])
    known = [e["payout"] for e in entries if e.get("payout")]
    fallback = sum(known) / len(known) if known else 90.0
    bal, pts, seq = CH_START_BAL, [CH_START_BAL], []
    for e in entries:
        p = float(e.get("payout") or fallback)
        stake = bal * CH_STAKE_PCT / 100.0
        bal += stake * p / 100.0 if e["win"] else -stake
        pts.append(bal)
        seq.append((bool(e["win"]), p))
    return pts, seq


def session_numbers(stats, start_ts, now_ts):
    pts, seq = session_equity(stats.log)
    pcts = [(b / CH_START_BAL - 1.0) * 100.0 for b in pts]
    peak_b, mdd = pts[0], 0.0
    for b in pts:
        peak_b = max(peak_b, b)
        mdd = max(mdd, (peak_b - b) / peak_b * 100.0)
    best = run = 0
    for w, _ in seq:
        run = run + 1 if w else 0
        best = max(best, run)
    pays = [p for _, p in seq]
    return {"pts": pts, "pcts": pcts, "seq": seq, "final": pts[-1], "growth": pcts[-1],
            "peak": max(pcts), "low": min(pcts), "mdd": mdd, "best": best,
            "avg_pay": (sum(pays) / len(pays)) if pays else None,
            "secs": max(0, int(now_ts - start_ts)), "wins": stats.wins, "losses": stats.losses}


def fmt_duration(secs):
    h, rem = divmod(int(secs), 3600)
    m = rem // 60
    if h:
        return f"{h}h {m:02d}m" if m else f"{h}h"
    return f"{m}m" if m else f"{int(secs)}s"


def _fit_px(cv, text, px, max_w, w="bold"):
    while px > 12 and cv.tw(text, px, w) > max_w:
        px -= 1
    return px


def _page_base(cv, status, scol):
    cv.brackets(28, 28, 1772, 872, CH_PURPLE, ln=46, lw=3.0)
    cv.L(64, 74, 1736, 74, "#111421", 1.3, z=3)
    cv.L(64, 808, 1736, 808, "#111421", 1.3, z=3)
    cv.T(64, 47, spaced("SIGNAL ENGINE") + "      " + CH_ENGINE_TAG, 13, CH_GRAY, w="bold")
    w = cv.tw(status, 17, "bold")
    cv.dot(1736 - w - 22, 47, 11, _rgba(scol, 0.13), z=5)
    cv.dot(1736 - w - 22, 47, 5, scol, z=6)
    cv.T(1736, 47, status, 17, scol, "bold", ha="right")
    cv.L(800, 108, 800, 790, "#111421", 1.3, z=3)


def _page_left(cv, welcome, lines, pill_text, pill_col, date_text, icon):
    cv.logo(400, 290, 1.8)
    cv.wordmark(400, 433, 68)
    cv.T(400, 485, spaced("QUANTITATIVE SIGNAL ENGINE"), 14, CH_PURPLE, "bold", ha="center")
    seg = 40
    for k in range(seg):                                           # thin gradient rule, fading at both ends
        t0, t1 = k / seg, (k + 1) / seg
        fade = 1.0 - abs(2 * (t0 + t1) / 2 - 1.0) ** 2
        cv.L(255 + 290 * t0, 518, 255 + 290 * t1 + 0.6, 518, _mix(CH_PURPLE, CH_CYAN, t0) + (0.15 + 0.8 * fade,),
             2.0, z=3)
    cv.T(400, 563, welcome, 25, CH_TEXT, "bold", ha="center")
    for i, ln in enumerate(lines):
        cv.T(400, 601 + 25 * i, ln, 17, CH_GRAY2, ha="center")
    cv.R(237, 667, 326, 55, fc=_mix("#04050a", pill_col, 0.10) + (1.0,), ec=pill_col, lw=2.0, r=13, z=4)
    if icon == "dot":
        cv.dot(264, 694, 10, pill_col, z=6)
        cv.T(293, 695, pill_text, 25, pill_col, "bold", z=6)
    else:
        cv.R(262, 684, 20, 20, fc=CH_TEXT, r=2, z=6)
        cv.T(301, 695, pill_text, 25, CH_TEXT, "bold", z=6)
    cv.T(400, 759, date_text, 16, CH_GRAY, ha="center")


def _page_bg(W=1800, H=900, seed=7):
    """Faint candle silhouettes along the bottom of the page."""
    return [(W, 0, 800, CH_PURPLE, 0.07)]


def _ghost_candles(cv):
    import random
    rnd = random.Random(7)
    y, ys = 810.0, []
    for i in range(46):
        x = 40 + i * 38
        o = y
        y = max(640.0, min(850.0, y + rnd.uniform(-26, 22)))
        c = y
        hi, lo = max(o, c) + rnd.uniform(2, 14), min(o, c) - rnd.uniform(2, 14)
        col = CH_GREEN if c < o else CH_RED
        cv.L(x, hi, x, lo, _rgba(col, 0.07), 1.2, z=1, ax=cv.bg)
        cv.R(x - 6, min(o, c), 12, max(4.0, abs(c - o)), fc=_rgba(col, 0.07), z=1, ax=cv.bg)
        ys.append(y)
    cv.PL([(40 + i * 38, y_) for i, y_ in enumerate(ys)], _rgba(CH_CYAN, 0.10), 1.5, z=1)


def render_overview_png(d):
    """SESSION OVERVIEW dashboard (the 'BE READY' picture). Returns PNG bytes."""
    cv = _Cv(1800, 900, "#04050a", glows=_page_bg())
    _ghost_candles(cv)
    tz, tzs = d["tz"], tz_short(d["tz_min"])
    stamp = datetime.fromtimestamp(d["now"], tz)
    _page_base(cv, "ENGINE ONLINE", CH_GREEN)
    _page_left(cv, "Welcome aboard, Trader.",
               ["Session initialized. The engine is now scanning", "the market for high-probability setups."],
               "SESSION STARTED", CH_GREEN, f"{stamp.strftime('%d %b %Y')}   \u00b7   {stamp.strftime('%H:%M:%S')} ({tzs})",
               "dot")
    cv.T(850, 118, spaced("SESSION OVERVIEW"), 15, CH_GRAY2, "bold")
    cv.T(1736, 118, f"SESSION ID   {d['sid']}", 14, CH_GRAY, ha="right")
    cv.L(850, 145, 1736, 145, "#111421", 1.3, z=3)
    mtg = d["mtg"]
    minpay = d.get("min_payout")
    cards = [
        ("TIMEFRAME", "M1", "1-Minute Candles", CH_PURPLE),
        ("MARKETS", str(d["markets"]), "OTC Pairs Monitored" if d.get("all_otc") else "Markets Monitored", CH_CYAN),
        ("MIN CONFIDENCE", f"{d['min_conf']:.0f}%", "Signal Threshold", CH_GREEN),
        ("MIN PAYOUT", "-" if minpay is None else f"\u2265 {minpay:g}%", "Payout Filter", CH_AMBER),
        ("MARTINGALE", "NO MTG" if mtg == 0 else f"{mtg} STEP" + ("S" if mtg > 1 else ""), "Max Recovery Depth",
         CH_PURPLE),
        ("MODE", "LIVE", "Auto Signal Delivery", CH_GREEN),
        ("TIMEZONE", tzs, "Session Clock", CH_CYAN),
        ("ANALYSIS", "RSI\u00b7ADX\u00b7EMA", "ATR \u00b7 Regression \u00b7 S/R", CH_AMBER),
    ]
    for i, (lab, val, sub, acc) in enumerate(cards):
        x = 850 + (i % 4) * 226
        y = 166 + (i // 4) * 186
        cv.R(x, y, 208, 168, fc=CH_CARD, ec="#131624", lw=1.4, r=10, z=3)
        cv.L(x + 18, y, x + 52, y, acc, 3.0, z=5, cap="round")
        cv.T(x + 18, y + 29, spaced(lab), 12, CH_GRAY, w="bold")
        cv.T(x + 18, y + 89, val, _fit_px(cv, val, 42, 176), CH_TEXT, "bold")
        cv.T(x + 18, y + 141, sub, 14, CH_GRAY2)
    cv.R(850, 545, 886, 205, fc=CH_CARD, ec="#131624", lw=1.4, r=10, z=3)
    cv.T(872, 576, spaced("SYSTEM STATUS"), 13, CH_GRAY2, "bold")
    cv.L(872, 601, 1714, 601, "#111421", 1.3, z=3)
    lat = d.get("latency_ms")
    tg_ok = lat is not None
    cols = [("MARKET FEED", "CONNECTED", CH_GREEN, True), ("SIGNAL ENGINE", "ACTIVE", CH_GREEN, True),
            ("RISK FILTER", "ENABLED", CH_GREEN, True),
            ("TELEGRAM LINK", "STABLE" if tg_ok else "UNSTABLE", CH_GREEN if tg_ok else CH_AMBER, True),
            ("LATENCY", f"{lat} ms" if tg_ok else "-", CH_TEXT, False)]
    for i, (lab, val, col, has_dot) in enumerate(cols):
        x = 872 + [0, 168, 337, 505, 674][i]
        cv.T(x, 651, spaced(lab), 12, CH_GRAY, w="bold")
        if has_dot:
            cv.dot(x + 6, 694, 8, _rgba(col, 0.13), z=5)
            cv.dot(x + 6, 694, 4, col, z=6)
            cv.T(x + 23, 693.5, val, _fit_px(cv, val, 19, 140 - 23), col, "bold")
        else:
            cv.T(x, 693.5, val, 19, col, "bold")
        if i:
            cv.L(x - 14, 628, x - 14, 716, "#111421", 1.3, z=3)
    cv.T(64, 837, "\u00a9 DARKMATTER   \u2022   Quantitative Signal Engine", 13, CH_FOOT)
    cv.T(1736, 837, "Trade responsibly. Signals are not guaranteed.   |   For informational purposes only", 13,
         CH_FOOT, ha="right", it=True)
    return cv.png()


def _nice_step(raw):
    for s in (0.25, 0.5, 1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10, 12, 15, 20, 25, 30, 40, 50, 60, 80, 100, 150, 200,
              250, 300, 400, 500, 1000):
        if s >= raw * 0.95:
            return s
    return raw


def render_summary_png(d):
    """SESSION SUMMARY dashboard (the 'SESSION END' picture). Returns PNG bytes."""
    n = d["nums"]
    g = n["growth"]
    acc = CH_GREEN if g > 0.005 else CH_RED if g < -0.005 else CH_AMBER
    cv = _Cv(1800, 900, "#04050a", glows=_page_bg())
    _ghost_candles(cv)
    tz, tzs = d["tz"], tz_short(d["tz_min"])
    t0, t1 = datetime.fromtimestamp(d["start"], tz), datetime.fromtimestamp(d["now"], tz)
    _page_base(cv, "ENGINE STANDBY", CH_AMBER)
    _page_left(cv, "Session closed.",
               ["All trades are settled. The signal engine", "is now in standby mode."],
               "SESSION CLOSED", CH_PURPLE,
               f"{t1.strftime('%d %b %Y')}   \u00b7   {t0.strftime('%H:%M')} \u2192 {t1.strftime('%H:%M')} ({tzs})",
               "square")
    cv.T(850, 118, spaced("SESSION SUMMARY"), 15, CH_GRAY2, "bold")
    cv.T(1736, 118, f"SESSION ID   {d['sid']}", 14, CH_GRAY, ha="right")
    cv.L(850, 145, 1736, 145, "#111421", 1.3, z=3)
    # ---- account growth + equity curve
    cv.R(850, 160, 886, 320, fc=CH_CARD, ec="#131624", lw=1.4, r=10, z=3)
    cv.L(878, 160, 912, 160, acc, 3.0, z=5, cap="round")
    cv.T(878, 189, spaced("ACCOUNT GROWTH"), 12, CH_GRAY, w="bold")
    big = f"{g:+.2f}%"
    cv.T(878, 277, big, _fit_px(cv, big, 66, 330), acc, "bold", stroke=(5, _mix(CH_CARD, acc, 0.20)))
    head = "\u25b2 PROFITABLE SESSION" if g > 0.005 else "\u25bc SESSION IN DRAWDOWN" if g < -0.005 else \
        "\u25cf BREAK-EVEN SESSION"
    cv.T(878, 330, head, 19, acc, "bold")
    cv.T(878, 363, f"${CH_START_BAL:,.2f}   \u2192   ${n['final']:,.2f}", 15, CH_GRAY2)
    cv.L(878, 393, 1190, 393, "#131624", 1.3, z=3)
    cv.T(878, 418, f"PEAK  {n['peak']:+.2f}%", 15, CH_GREEN, "bold")
    cv.T(878, 444, f"LOW  {n['low']:.2f}%", 15, CH_RED if n["low"] < -0.005 else CH_GRAY2, "bold")
    cv.L(1222, 183, 1222, 457, "#131624", 1.3, z=3)
    cv.T(1246, 189, spaced("EQUITY CURVE"), 12, CH_GRAY, w="bold")
    vmax, vmin = max(0.0, n["peak"]), min(0.0, n["low"])
    span = max(vmax - vmin, 0.6)
    if vmax - vmin < 0.6:
        vmax = vmin + span
    step = _nice_step(span / 3.0)
    px0, px1, py0, py1 = 1242, 1655, 248, 403
    Yv = lambda v: py0 + (vmax - v) / (vmax - vmin) * (py1 - py0)
    k = math.ceil(vmin / step - 1e-9)
    while k * step <= vmax + 1e-9:
        v = k * step
        y = Yv(v)
        if abs(v) < 1e-9:
            cv.L(px0, y, px1 + 10, y, "#3a3f55", 1.2, z=3, ls=(0, (4, 4)))
        else:
            cv.L(px0, y, px1 + 10, y, "#141826", 1.0, z=2)
        cv.T(1677, y, (f"{v:+.0f}%" if v else "0%") if step >= 1 else (f"{v:+.1f}%" if v else "0%"), 14, CH_GRAY2)
        k += 1
    pts = n["pcts"]
    m = len(pts)
    xs = [px0 + (px1 - px0) * i / max(1, m - 1) for i in range(m)]
    ysv = [Yv(p) for p in pts]
    if m > 1:
        z0 = Yv(0.0)
        poly = [(xs[0], z0)] + list(zip(xs, ysv)) + [(xs[-1], z0)]
        cv.poly(poly, _rgba(acc, 0.20), z=3)
        cv.PL(list(zip(xs, ysv)), acc, 2.4, z=5, cap="round", join="round")
        for i in range(1, m):
            w_ = n["seq"][i - 1][0]
            cv.dot(xs[i], ysv[i], 5.2, CH_GREEN if w_ else CH_RED, "#04050a", 1.4, z=7)
        cv.dot(xs[-1], ysv[-1], 12.5, "none", CH_TEXT, 2.0, z=8)
    tagtxt = f"{g:+.2f}%"
    tw_ = cv.tw(tagtxt, 17, "bold") + 20
    cv.R(1652 - tw_ / 2, 314, tw_, 28, fc=acc, r=5, z=8)
    cv.T(1652, 328.5, tagtxt, 17, "#04050a", "bold", ha="center", z=9)
    cv.T(1232, 451, "START", 13, CH_FOOT, ha="left")
    cv.T(1693, 451, f"TRADE #{len(n['seq'])}", 13, CH_FOOT, ha="right")
    # ---- four counters
    wn, ls_ = n["wins"], n["losses"]
    tot = wn + ls_
    rate = int(100.0 * wn / tot + 0.5) if tot else 0
    cards = [("TOTAL TRADES", str(tot), CH_TEXT, "Executed Signals", CH_PURPLE),
             ("WINS", str(wn), CH_GREEN, "Profitable Trades", CH_GREEN),
             ("LOSSES", str(ls_), CH_RED, "Losing Trades", CH_RED),
             ("WIN RATE", f"{rate}%", CH_AMBER, f"{wn}W  \u00b7  {ls_}L", CH_AMBER)]
    for i, (lab, val, col, sub, accent) in enumerate(cards):
        x = 850 + i * 226
        cv.R(x, 495, 208, 130, fc=CH_CARD, ec="#131624", lw=1.4, r=10, z=3)
        cv.L(x + 18, 495, x + 52, 495, accent, 3.0, z=5, cap="round")
        cv.T(x + 18, 518, spaced(lab), 12, CH_GRAY, w="bold")
        cv.T(x + 18, 557, val, 46, col, "bold")
        if lab == "WIN RATE":
            cv.R(x + 18, 583, 172, 8, fc="#151a28", r=4, z=4)
            if rate > 0:
                cv.R(x + 18, 583, 172 * rate / 100.0, 8, fc=CH_AMBER, r=4, z=5)
        cv.T(x + 18, 606, sub, 14, CH_GRAY2)
    # ---- trade sequence
    cv.R(850, 640, 886, 145, fc=CH_CARD, ec="#131624", lw=1.4, r=10, z=3)
    cv.T(872, 665, spaced("TRADE SEQUENCE"), 12, CH_GRAY, w="bold")
    seq = n["seq"][-24:]
    for i, (w_, _) in enumerate(seq):
        x = 872 + i * 35
        col = CH_GREEN if w_ else CH_RED
        cv.R(x, 685, 28, 26, fc=_mix(CH_CARD, col, 0.20) + (1.0,), ec=col, lw=1.4, r=5, z=4)
        cv.T(x + 14, 698.5, "W" if w_ else "L", 13, col, "bold", ha="center", z=5)
    if not seq:
        cv.T(872, 698, "No finished signal in this session.", 14, CH_GRAY)
    cv.L(872, 724, 1714, 724, "#131624", 1.3, z=3)
    pay = "-" if n["avg_pay"] is None else f"{n['avg_pay']:.0f}%"
    best = n["best"]
    stats4 = [(f"{best} WIN" + ("S" if best != 1 else ""), CH_GREEN, "BEST STREAK"),
              (f"-{n['mdd']:.1f}%" if n["mdd"] > 0.049 else "0.0%", CH_RED if n["mdd"] > 0.049 else CH_GRAY2,
               "MAX DRAWDOWN"),
              (pay, CH_AMBER, "AVG PAYOUT"), (fmt_duration(n["secs"]), CH_TEXT, "DURATION")]
    for i, (val, col, lab) in enumerate(stats4):
        x = 872 + [0, 210, 420, 631][i]
        cv.T(x, 746, val, 22, col, "bold")
        cv.T(x, 770, spaced(lab), 11, CH_GRAY, w="bold")
        if i:
            cv.L(x - 13, 733, x - 13, 777, "#131624", 1.3, z=3)
    cv.T(64, 837, "\u00a9 DARKMATTER   \u2022   Quantitative Signal Engine", 13, CH_FOOT)
    cv.T(1736, 837, "Past results do not guarantee future performance.   |   Trade responsibly", 13, CH_FOOT,
         ha="right", it=True)
    return cv.png()


# =====================================================================
#  module: darkmatter/notify/charts_trade.py  -  CHARTS: the SIGNAL dashboard and the RESULT dashboard images.
# =====================================================================
#  trade charts: the SIGNAL dashboard (CALL / PUT ENTRY) and the RESULT dashboard (WIN / LOSS)
# ---------------------------------------------------------------------
_PX = (381, 80, 1556, 565)       # price chart  (x, y, w, h)
_RX = (381, 670, 1556, 115)      # RSI panel
_VX = (381, 810, 1556, 115)      # tick-volume panel
_XL = (-1.92, 0)                  # candle 0 sits 1.92 candles from the left edge; the right gutter is computed per chart
_GRID = "#101423"


def _nice_ticks(lo, hi, n=7):
    span = hi - lo
    if not span > 0:
        return [lo], 4
    raw = span / n
    mag = 10 ** math.floor(math.log10(raw))
    step = next(m * mag for m in (1, 2, 2.5, 5, 10) if m * mag >= raw)
    dec = max(0, int(math.ceil(-math.log10(step) - 1e-9)))
    if abs(step * 10 ** dec - round(step * 10 ** dec)) > 1e-6:
        dec += 1
    t = math.ceil(lo / step - 1e-9) * step
    out = []
    while t <= hi + 1e-12:
        out.append(round(t, 10))
        t += step
    return out, dec


def _frame(d):
    """Everything the chart needs, computed from the candles (indicators use ALL candles handed in)."""
    cs = d["candles"]
    closes = [float(c["c"]) for c in cs]
    e9, e20, e50 = _ema(closes, 9), _ema(closes, 20), _ema(closes, 50)
    rsi = _rsi_series(closes, 14)
    n = min(CH_SHOW, len(cs))
    a = len(cs) - n
    show = cs[a:]
    rng = sorted(max(1e-12, float(c["h"]) - float(c["l"])) for c in show)
    med = rng[len(rng) // 2]
    vols = []
    for c in show:
        v = c.get("v")
        try:
            v = float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            v = 0.0
        if not v > 0:                                    # no tick count for this candle: scaled candle range
            v = 20.0 + 80.0 * min(2.2, max(1e-12, float(c["h"]) - float(c["l"])) / med)
        vols.append(v)
    fr = {
        "n": n, "show": show, "vols": vols,
        "e9": e9[a:], "e20": e20[a:], "e50": e50[a:],
        "rsi": rsi[a:], "rsi_last": next((x for x in reversed(rsi) if x is not None), None),
        "highs": [float(c["h"]) for c in show], "lows": [float(c["l"]) for c in show],
    }
    ys = list(range(n))
    cl = closes[a:]
    slope, _ = _linreg(cl) if n > 2 else (0.0, 0.0)
    mean_x, mean_y = (n - 1) / 2.0, sum(cl) / n
    fit = [mean_y + slope * (i - mean_x) for i in ys]
    sd = math.sqrt(sum((cl[i] - fit[i]) ** 2 for i in ys) / n) if n else 0.0
    fr["chan"] = (fit, sd)
    return fr


def _draw_candles(cv, fr, X, Y, VY, pxc, clip_p, clip_v):
    """Candles (price panel) and tick-volume bars (volume panel): green = close >= open, red = close < open."""
    show = fr["show"]
    bw, vw = 0.72 * pxc, 0.62 * pxc
    y0 = VY(0)
    for i, c in enumerate(show):
        o, cl, hi, lo = float(c["o"]), float(c["c"]), float(c["h"]), float(c["l"])
        col = CH_GREEN if cl >= o else CH_RED
        x = X(i)
        cv.L(x, Y(hi), x, Y(lo), col, 1.2, z=2.0, ax=cv.panel, clip=clip_p)
        top, bot = Y(max(o, cl)), Y(min(o, cl))
        if bot - top < 1.4:                                   # doji / flat candle: still a visible line
            mid = (top + bot) / 2.0
            top, bot = mid - 0.7, mid + 0.7
        cv.R(x - bw / 2, top, bw, bot - top, fc=col, ec=col, lw=1.0, z=3.0, ax=cv.panel, clip=clip_p)
        vy = VY(fr["vols"][i])
        cv.R(x - vw / 2, vy, vw, max(0.0, y0 - vy), fc=col, z=2.0, ax=cv.panel, clip=clip_v)


def _spread(ys, gap, lo, hi):
    """Moves price tags apart so that no two are closer than `gap` pixels, keeping their order and staying as
    close to their real price level as possible. Returns the new positions in the order given."""
    n = len(ys)
    if n == 0:
        return []
    order = sorted(range(n), key=lambda i: ys[i])
    pos = [float(ys[i]) for i in order]
    for _ in range(200):
        moved = False
        for k in range(1, n):
            gapnow = pos[k] - pos[k - 1]
            if gapnow < gap - 1e-6:
                half = (gap - gapnow) / 2.0
                pos[k - 1] -= half
                pos[k] += half
                moved = True
        pos[0] = max(pos[0], lo)
        pos[-1] = min(pos[-1], hi)
        if not moved:
            break
    pos[0] = max(pos[0], lo)                              # final guarantee: gaps and bounds
    for k in range(1, n):
        pos[k] = max(pos[k], pos[k - 1] + gap)
    if pos[-1] > hi:
        pos[-1] = hi
        for k in range(n - 2, -1, -1):
            pos[k] = min(pos[k], pos[k + 1] - gap)
    out = [0.0] * n
    for rank, i in enumerate(order):
        out[i] = pos[rank]
    return out


def _trade_chart(cv, d, result):
    """Header, price chart (+EMA 9/20/50, S/R, regression channel), RSI 14 and tick-volume panels."""
    fr = _frame(d)
    show, n = fr["show"], fr["n"]
    call = d["dir"] > 0
    dcol = CH_GREEN if call else CH_RED
    ocol = (CH_GREEN if d.get("win") else CH_RED) if result else dcol       # accent colour of the page
    tz = d["tz"]
    dec = d["dec"]
    fmt = lambda v: f"{v:.{dec}f}"
    sup, res_ = d.get("support"), d.get("resistance")
    last = show[-1]
    cx0, cy0, cw, chh = _PX

    # ---- price tags live in their own gutter right of the last candle: they never cover a candle
    tagdefs = []                                               # (key, price, text, fill, text colour)
    if result:
        tagdefs.append(("entry", d["open"], f"ENTRY  {fmt(d['open'])}", "#e4e7ef", "#000000"))
        tagdefs.append(("close", d["close"], f"CLOSE  {fmt(d['close'])}", ocol, "#000000"))
    else:
        tagdefs.append(("entry", d["price"], f"ENTRY  {fmt(d['price'])}", dcol, "#000000"))
    if res_ is not None:
        tagdefs.append(("res", res_, f"R  {fmt(res_)}", CH_RED, "#ffffff"))
    if sup is not None:
        tagdefs.append(("sup", sup, f"S  {fmt(sup)}", CH_GREEN, "#000000"))
    tag_w = {k: cv.tw(t, 15, "bold") + 18 for k, _, t, _, _ in tagdefs}
    tag_right = cx0 + cw - 6
    tag_left_min = tag_right - max(tag_w.values())
    zone = 34 if result else 16                                # room right of the last candle for the marker
    x_line_end = tag_left_min - 14                             # where the horizontal price lines stop
    free_px = (cx0 + cw) - (tag_left_min - 8 - zone)           # last candle centre -> right edge of the plot
    pxc = (cw - free_px) / (n - 1 - _XL[0])
    xl = (_XL[0], n - 1 + free_px / pxc)

    # ---- y range
    lo = min(fr["lows"] + ([sup] if sup is not None else []) + ([d["close"], d["open"]] if result else []))
    hi = max(fr["highs"] + ([res_] if res_ is not None else []) + ([d["close"], d["open"]] if result else []))
    if not hi > lo:
        hi = lo + max(abs(lo) * 1e-4, 1e-6)
    pad = 0.05 * (hi - lo)
    yl = (lo - pad, hi + pad)
    X = lambda i: cx0 + (i - xl[0]) / (xl[1] - xl[0]) * cw
    Y = lambda p: cy0 + chh - (p - yl[0]) / (yl[1] - yl[0]) * chh
    frac_end = (x_line_end - cx0) / cw                         # axes fraction where the price lines stop
    xd_end = xl[0] + frac_end * (xl[1] - xl[0])                # the same place in candle units

    # ---- panels (price, RSI, tick volume): background, grid, candles, indicators, frame
    rx0, ry0, rw, rh = _RX
    vx0, vy0, vw_, vh = _VX
    P = cv.panel
    clip_p, clip_r, clip_v = _PX, _RX, _VX
    RY = lambda v: ry0 + rh * (1 - v / 100.0)
    ticks, tdec = _nice_ticks(yl[0], yl[1], 7)
    vmax = max(fr["vols"])
    vtop = max(200.0, math.ceil(vmax * 1.1 / 100.0) * 100.0)
    VY = lambda v: vy0 + vh * (1 - v / vtop)
    ids = [n - 1 - 9 * k for k in range(0, (n - 1) // 9 + 1)]
    for (px_, py_, pw_, ph_), bgc in ((_PX, CH_BG), (_RX, CH_PANEL), (_VX, CH_PANEL)):
        cv.R(px_, py_, pw_, ph_, fc=bgc, z=-5, ax=P)
        for i in ids:
            cv.L(X(i), py_, X(i), py_ + ph_, _GRID, 1.0, z=0.2, ax=P)
    for tk in ticks:
        cv.L(cx0, Y(tk), cx0 + cw, Y(tk), _GRID, 1.0, z=0.2, ax=P)
    _draw_candles(cv, fr, X, Y, VY, pxc, clip_p, clip_v)

    # ---- price chart: bands, S/R, channel, EMAs
    band = 0.075 * (yl[1] - yl[0])
    if res_ is not None:
        cv.R(cx0, Y(res_), x_line_end - cx0, Y(res_ - band) - Y(res_), fc=_rgba(CH_RED, 0.065), z=0.4, ax=P,
             clip=clip_p)
        cv.L(cx0, Y(res_), x_line_end, Y(res_), _rgba(CH_RED, 0.82), 1.6, z=1.2, ls=(0, (6, 5)), ax=P, clip=clip_p)
    if sup is not None:
        cv.R(cx0, Y(sup + band), x_line_end - cx0, Y(sup) - Y(sup + band), fc=_rgba(CH_GREEN, 0.065), z=0.4, ax=P,
             clip=clip_p)
        cv.L(cx0, Y(sup), x_line_end, Y(sup), _rgba(CH_GREEN, 0.82), 1.6, z=1.2, ls=(0, (6, 5)), ax=P, clip=clip_p)
    fit, sd = fr["chan"]
    xs = list(range(n))
    for off in (2.0 * sd, -2.0 * sd):
        cv.PL([(X(i), Y(fit[i] + off)) for i in xs], _rgba(CH_PURPLE, 0.40), 1.0, z=1.0, ls=(0, (1, 3)), ax=P,
              clip=clip_p)
    for key, col, lw in (("e50", CH_PURPLE, 2.2), ("e20", CH_CYAN, 2.2), ("e9", CH_AMBER, 2.0)):
        cv.PL([(X(i), Y(v)) for i, v in zip(xs, fr[key]) if v is not None], col, lw, z=4.2, cap="round", ax=P,
              clip=clip_p)
    cv.R(X(n - 1 - 0.62), cy0, X(n - 1 + 0.62) - X(n - 1 - 0.62), chh, fc=_rgba(ocol, 0.14), z=0.5, ax=P)
    wm = "DARKMATTER" if not result else ("WIN" if d.get("win") else "LOSS")
    cv.T(cx0 + cw / 2.0, cy0 + chh / 2.0, wm, 128 if not result else 190,
         _rgba(CH_TEXT if not result else ocol, 0.028 if not result else 0.05), "bold", ha="center", z=0.1, ax=P,
         rot=15, clip=clip_p)

    # ---- RSI panel
    rv = fr["rsi"]
    for lv, ls_, col in ((70, (0, (4, 4)), "#3a3f55"), (30, (0, (4, 4)), "#3a3f55"), (50, "-", "#1f2335")):
        cv.L(rx0, RY(lv), rx0 + rw, RY(lv), col, 0.9, z=1, ls=ls_, ax=P)
    for lim, keep, col in ((70, max, CH_RED), (30, min, CH_GREEN)):       # shaded overbought / oversold zones
        upper = [(X(i), RY(lim)) for i in range(n)]
        lower = [(X(i), RY(keep(lim, rv[i]) if rv[i] is not None else lim)) for i in reversed(range(n))]
        cv.poly(upper + lower, _rgba(col, 0.30), z=2, ax=P, clip=clip_r)
    run = []
    for i in range(n + 1):                                        # the RSI line breaks where there is no value
        if i < n and rv[i] is not None:
            run.append((X(i), RY(rv[i])))
        else:
            cv.PL(run, CH_CYAN, 1.9, z=3, cap="round", ax=P, clip=clip_r)
            run = []

    # ---- volume moving average
    vser = fr["vols"]
    vma = [sum(vser[max(0, i - 19):i + 1]) / (i + 1 - max(0, i - 19)) for i in range(n)]
    cv.PL([(X(i), VY(vma[i])) for i in range(n)], _rgba(CH_AMBER, 0.9), 1.3, z=4, ax=P, clip=clip_v)

    # ---- panel frames (drawn last so the candles never cover them)
    for (px_, py_, pw_, ph_) in (_PX, _RX, _VX):
        cv.R(px_, py_, pw_, ph_, fc="none", ec=CH_BORDER, lw=1.0, z=60, ax=P)

    # ---- header
    sym = str(d["symbol"]).upper()
    cv.T(381, 31, sym, 28, CH_TEXT, "bold")
    now_dt = datetime.fromtimestamp(d["now"], tz)
    stamp = now_dt.strftime("%Y-%m-%d %H:%M:%S") + f" ({tz_short(d['tz_min'])})"
    cv.T(381, 55, f"M1  \u00b7  {'TRADE RESULT' if result else 'BINARY SIGNAL'}  \u00b7  {stamp}", 15, CH_GRAY)
    seg = 70
    for k in range(seg):                                          # header rule: grey fading to the accent colour
        t0, t1 = k / seg, (k + 1) / seg
        cv.L(381 + 1556 * t0, 67, 381 + 1556 * t1 + 0.6, 67, _mix("#4a5068", ocol, t0 ** 1.5) + (0.9,), 1.3, z=3)
    cur_r = 1937
    if not result:
        w = cv.pill(cur_r, 48, ("\u25b2 CALL ENTRY" if call else "\u25bc PUT ENTRY"), 21, dcol, dcol, "#04050a",
                    h=42, pad=14, lw=0, r=9)
        cur_r -= w + 20
        pay = "-" if d.get("payout") is None else f"{d['payout']:g}%"
        w = cv.pill(cur_r, 47, f"PAYOUT  {pay}", 17, _rgba(CH_AMBER, 0.10), CH_AMBER, CH_AMBER)
        cur_r -= w + 20
        cv.pill(cur_r, 47, f"CONFIDENCE  {int(d['conf'] + 0.5)}%", 17, "#18122f", CH_PURPLE, CH_TEXT)
    else:
        win = bool(d.get("win"))
        w = cv.pill(cur_r, 48, ("\u2713 WIN" if win else "\u2715 LOSS"), 21, ocol, ocol, "#04050a", h=42, pad=14,
                    lw=0, r=9)
        cur_r -= w + 20
        pl = (f"+{d['payout']:g}%" if d.get("payout") is not None else "+-") if win else "-100%"
        w = cv.pill(cur_r, 47, f"P/L  {pl}", 17, _rgba(ocol, 0.10), ocol, ocol)
        cur_r -= w + 20
        side = "CALL \u25b2" if call else "PUT \u25bc"
        cv.pill(cur_r, 47, f"SIGNAL  {side}  {int(d['conf'] + 0.5)}%", 17, "#18122f", CH_PURPLE, CH_TEXT)

    # ---- legend inside the chart
    lx = 416
    for name, col in (("EMA 9", CH_AMBER), ("EMA 20", CH_CYAN), ("EMA 50", CH_PURPLE)):
        cv.L(lx, 111.5, lx + 9, 111.5, col, 2.0, z=9)
        wtxt = cv.tw(name, 15, "bold")
        cv.T(lx + 13, 111, name, 15, col, "bold", z=9)
        lx += 13 + wtxt + 26

    # ---- axis labels
    for tk in ticks:
        cv.T(1942, Y(tk), f"{tk:.{tdec}f}", 15, CH_GRAY2, ha="left", z=6)
    for lv in (70, 50, 30):
        cv.T(1942, _RX[1] + _RX[3] * (1 - lv / 100.0), str(lv), 14, CH_GRAY2, ha="left", z=6)
    for lv in (0, vtop / 2, vtop):
        cv.T(1942, _VX[1] + _VX[3] * (1 - lv / vtop), f"{lv:.0f}", 14, CH_GRAY2, ha="left", z=6)
    cv.T(394, 686, f"RSI 14   {fr['rsi_last']:.1f}" if fr["rsi_last"] is not None else "RSI 14", 14, CH_GRAY2,
         "bold", z=9)
    cv.T(394, 826, "TICK VOLUME", 14, CH_GRAY2, "bold", z=9)
    for i in ids:
        cv.T(X(i), 936, datetime.fromtimestamp(int(show[i]["t"]), tz).strftime("%H:%M"), 15, CH_GRAY2,
             ha="center", z=6)
    cv.brackets(cx0, cy0, cx0 + cw, cy0 + chh, ocol if result else CH_PURPLE)

    # ---- markers and tags
    xl_last = X(n - 1)
    ys_true = {k: Y(v) for k, v, _, _, _ in tagdefs}
    ys_tag = dict(zip([k for k, *_ in tagdefs],
                      _spread([ys_true[k] for k, *_ in tagdefs], 27.0, cy0 + 14, cy0 + chh - 14)))
    line_col = {"entry": (_rgba("#e4e7ef", 0.45) if result else _rgba(dcol, 0.60)), "close": _rgba(ocol, 0.85),
                "res": _rgba(CH_RED, 0.70), "sup": _rgba(CH_GREEN, 0.70)}

    def tag_row():
        """Price tags in the gutter; each one is joined to its real price level by a short leader line."""
        for key, price, text, fill, tc in sorted(tagdefs, key=lambda t: ys_tag[t[0]]):
            yt, y0 = ys_tag[key], ys_true[key]
            left = tag_right - tag_w[key]
            if key in ("entry", "close"):                    # entry / close level: dotted line up to the gutter
                cv.L(cx0 if key == "entry" else xl_last + zone * 0.5, y0, x_line_end, y0, line_col[key], 1.0, z=5,
                     ls=(0, (1, 2.5)))
            cv.L(x_line_end, y0, x_line_end + 8, yt, line_col[key], 1.2, z=6)
            cv.L(x_line_end + 8, yt, left - 1, yt, line_col[key], 1.2, z=6)
            cv.tag(tag_right, yt, text, fill, tc)

    if not result:
        if call:
            ay, tri = Y(last["l"]) + 34, [(xl_last, 0), (xl_last - 12, 22), (xl_last + 12, 22)]
        else:
            ay, tri = Y(last["h"]) - 34, [(xl_last, 22), (xl_last - 12, 0), (xl_last + 12, 0)]
        ay = min(max(ay, cy0 + 18), cy0 + chh - 18)
        cv.poly([(x, ay - 11 + dy) for x, dy in tri], dcol, ec="#04050a", lw=1.2, z=8)
        lab = f"ENTRY  {datetime.fromtimestamp(d['entry_ts'], tz).strftime('%H:%M')}"
        lw_ = cv.tw(lab, 15, "bold") + 24
        lcx = xl_last - 19 * pxc
        lcy = ay + (0.145 if call else -0.155) * chh
        lcy = min(max(lcy, cy0 + 22), cy0 + chh - 22)
        cv.L(lcx + lw_ / 2, lcy, xl_last, ay + (6 if call else -6), _rgba(dcol, 0.85), 1.3, z=7)
        cv.R(lcx - lw_ / 2, lcy - 14, lw_, 28, fc=CH_BG, ec=dcol, lw=1.5, r=5, z=8)
        cv.T(lcx, lcy + 0.5, lab, 15, dcol, "bold", ha="center", z=9)
        tag_row()
    else:
        win = bool(d.get("win"))
        o, c = d["open"], d["close"]
        yo, yc_ = ys_true["entry"], ys_true["close"]
        mx = xl_last + 17
        move = d["dir"] * (c - o) / (10.0 ** -max(1, dec - 1))
        lab = f"{'WIN' if win else 'LOSS'}  {move:+.1f} pips"
        lw_ = cv.tw(lab, 15, "bold") + 24
        lcx = xl_last - 19 * pxc
        lcy = yc_ + (0.17 if call else -0.17) * chh
        lcy = min(max(lcy, cy0 + 22), cy0 + chh - 22)
        cv.L(lcx + lw_ / 2, lcy, mx, yc_, _rgba(ocol, 0.9), 1.3, z=7)
        cv.R(lcx - lw_ / 2, lcy - 14, lw_, 28, fc=CH_BG, ec=ocol, lw=1.5, r=5, z=8)
        cv.T(lcx, lcy + 0.5, lab, 15, ocol, "bold", ha="center", z=9)
        tag_row()
        if abs(yo - yc_) >= 17:                               # the open dot is skipped when it would touch the circle
            cv.dot(mx, yo, 5.5, "#e4e7ef", "#04050a", 1.5, z=8)
        cv.dot(mx, yc_, 10.5, ocol, "#04050a", 1.5, z=9)
        cv.T(mx, yc_ + 0.5, "\u2713" if win else "\u2715", 15, "#04050a", "bold", ha="center", z=10)
    return fr


def _side_base(cv, accent, subtitle):
    cv.R(0, 0, 330, 1000, fc=CH_SIDE, z=1, ax=cv.bg)
    cv.L(329.5, 0, 329.5, 1000, CH_BORDER, 1.4, z=2, ax=cv.bg)
    cv.R(0, 0, 4, 1000, fc=accent, z=3, ax=cv.bg)
    cv.logo(165, 80)
    cv.wordmark(165, 155, 30)
    cv.T(165, 190, spaced(subtitle), 13, CH_PURPLE, ha="center")


def _side_rows(cv, items, ys):
    for (lab, val, col), y in zip(items, ys):
        cv.T(36, y, lab, 16, CH_GRAY)
        cv.T(296, y, val, 16, col, "bold", ha="right")


def _tint(color, a=0.10):
    return _mix(CH_SIDE, color, a) + (1.0,)


def _footer_main(cv, right_text):
    cv.T(381, 974, "DARKMATTER  \u2022  Quantitative Signal Engine", 13, CH_FOOT)
    cv.T(1937, 974, right_text, 13, CH_FOOT, ha="right")


def render_signal_png(d):
    """The SIGNAL dashboard (CALL / PUT ENTRY). Returns PNG bytes. Raises when the chart cannot be drawn."""
    cv = _Cv(2060, 1000, CH_BG, glows=((2060, 0, 900, CH_PURPLE, 0.07),))
    fr = _trade_chart(cv, d, False)
    call = d["dir"] > 0
    dcol = CH_GREEN if call else CH_RED
    dec = d["dec"]
    fmt = lambda v: f"{v:.{dec}f}"
    _side_base(cv, CH_PURPLE, "SIGNAL ENGINE \u00b7 M1")
    label = "CALL \u25b2" if call else "PUT \u25bc"
    w = cv.tw(label, 34, "bold") + 52
    cv.R(165 - w / 2, 222, w, 66, fc=_tint(dcol), ec=dcol, lw=2.0, r=12, z=4)
    cv.T(165, 255.5, label, 34, dcol, "bold", ha="center", z=6)
    cv.gauge(165, 410, d["conf"], dcol)
    cv.T(165, 387, f"{int(d['conf'] + 0.5)}%", 38, CH_TEXT, "bold", ha="center")
    cv.T(165, 424, spaced("CONFIDENCE"), 12, CH_GRAY, ha="center")
    for y in (450, 570, 690, 810, 908):
        cv.sep(y)
    sup, res_ = d.get("support"), d.get("resistance")
    rsi = fr["rsi_last"]
    adx = d.get("adx")
    rcol = CH_RED if (rsi is not None and rsi > 70) else CH_GREEN if (rsi is not None and rsi < 30) else CH_TEXT
    adx_lab = "-" if adx is None else ("STRONG" if adx >= 25 else "MODERATE" if adx >= 20 else "WEAK")
    rsi_txt = f"{rsi:.0f}" if rsi is not None else "-"
    adx_txt = f"{adx:.0f} {adx_lab}" if adx is not None else "-"
    e9, e20, e50 = fr["e9"][-1], fr["e20"][-1], fr["e50"][-1]
    slope = fr["e20"][-1] - fr["e20"][-2] if len(fr["e20"]) > 1 else 0.0
    up = slope > 0
    stack, scol = (("BULLISH", CH_GREEN) if e9 > e20 > e50 else ("BEARISH", CH_RED) if e9 < e20 < e50
                   else ("MIXED", CH_AMBER))
    atr = d.get("atr")
    atr_txt = (f"{atr:.{dec}f}" if atr is not None else "-") + (" \u00b7 UP" if up else " \u00b7 DOWN" if slope < 0
                                                               else " \u00b7 FLAT")
    ecol = CH_GREEN if up else CH_RED if slope < 0 else CH_AMBER
    pay = "-" if d.get("payout") is None else f"{d['payout']:g}%"
    mtg = d["mtg"]
    entry = datetime.fromtimestamp(d["entry_ts"], d["tz"]).strftime("%H:%M")
    _side_rows(cv, [("Entry Price", fmt(d["price"]), CH_TEXT),
                    ("Support", "-" if sup is None else fmt(sup), CH_GREEN),
                    ("Resistance", "-" if res_ is None else fmt(res_), CH_RED)], (477, 511, 544))
    _side_rows(cv, [("RSI \u00b7 ADX", f"{rsi_txt} \u00b7 {adx_txt}", rcol),
                    ("ATR \u00b7 EMA20", atr_txt, ecol),
                    ("EMA Stack", stack, scol)], (596, 630, 664))
    _side_rows(cv, [("Payout", pay, CH_AMBER), ("MTG Steps", str(mtg), CH_TEXT),
                    ("Entry Time", f"{entry} ({tz_short(d['tz_min'])})", CH_TEXT)], (717, 751, 785))
    _side_ma_footer(cv, 828)
    stamp = datetime.fromtimestamp(d["now"], d["tz"]).strftime("%Y-%m-%d %H:%M:%S")
    _footer_main(cv, f"Generated {stamp} ({tz_short(d['tz_min'])})   |   For informational purposes only")
    return cv.png()


def _side_ma_footer(cv, y_title):
    cv.T(165, y_title, spaced("MOVING AVERAGES"), 12, "#5b6174", ha="center")
    for (x0, x1), col, txt in (((41, 104), CH_AMBER, "9"), ((133, 196), CH_CYAN, "20"), ((225, 289), CH_PURPLE, "50")):
        cv.L(x0, y_title + 30, x1, y_title + 30, col, 4.0, z=5, cap="round")
        cv.T((x0 + x1) / 2, y_title + 50, txt, 18, col, "bold", ha="center")
    cv.T(165, y_title + 102, "Trade responsibly. Signals are not guaranteed.", 12, "#5b6174", ha="center", it=True)
    cv.T(165, y_title + 131, "\u00a9 DARKMATTER", 14, CH_DIM, "bold", ha="center")


def render_result_png(d):
    """The RESULT dashboard (WIN / LOSS). Returns PNG bytes. Raises when the chart cannot be drawn."""
    win = bool(d["win"])
    ocol = CH_GREEN if win else CH_RED
    call = d["dir"] > 0
    dec = d["dec"]
    fmt = lambda v: f"{v:.{dec}f}"
    cv = _Cv(2060, 1000, CH_BG, glows=((2060, 0, 900, CH_PURPLE, 0.06),))
    _trade_chart(cv, d, True)
    _side_base(cv, ocol, "TRADE RESULT")
    label = "WIN" if win else "LOSS"
    w = cv.tw(label, 38, "bold") + 44
    cv.R(165 - w / 2, 220, w, 70, fc=_tint(ocol), ec=ocol, lw=2.0, r=12, z=4)
    cv.T(165, 255.5, label, 38, ocol, "bold", ha="center", z=6)
    cv.dot(165, 363, 60, _rgba(ocol, 0.10), z=3, ax=cv.bg)
    cv.dot(165, 363, 56, _tint(ocol, 0.07), ocol, 6.5, z=4, ax=cv.bg)
    if win:
        cv.PL([(143, 364), (160, 382), (190, 345)], ocol, 9, z=6, cap="round", join="round")
    else:
        cv.L(145, 343, 185, 383, ocol, 9, z=6, cap="round")
        cv.L(185, 343, 145, 383, ocol, 9, z=6, cap="round")
    cv.T(165, 438, spaced("SETTLED"), 12, CH_GRAY, ha="center")
    for y in (460, 607, 757, 878):
        cv.sep(y)
    o, c = d["open"], d["close"]
    move = d["dir"] * (c - o) / (10.0 ** -max(1, dec - 1))
    pl = (f"+{d['payout']:g}%" if d.get("payout") is not None else "+-") if win else "-100%"
    step = d["step"]
    t_in = datetime.fromtimestamp(d["entry_ts"], d["tz"]).strftime("%H:%M")
    t_out = datetime.fromtimestamp(d["entry_ts"] + 60 * (step + 1), d["tz"]).strftime("%H:%M")
    tzs = tz_short(d["tz_min"])
    _side_rows(cv, [("Direction", "CALL \u25b2" if call else "PUT \u25bc", CH_GREEN if call else CH_RED),
                    ("Entry Price", fmt(o), CH_TEXT), ("Close Price", fmt(c), ocol),
                    ("Move", f"{move:+.1f} pips", ocol)], (484, 517, 550, 583))
    _side_rows(cv, [("Profit / Loss", pl, ocol), ("MTG Step", f"{step} / {d['mtg']}", CH_TEXT),
                    ("Entry Time", f"{t_in} ({tzs})", CH_TEXT), ("Exit Time", f"{t_out} ({tzs})", CH_TEXT)],
                (633, 665, 699, 732))
    cv.T(165, 774, spaced("SESSION SCORE"), 12, "#5b6174", ha="center")
    wn, ls_ = d["wins"], d["losses"]
    rate = int(100.0 * wn / (wn + ls_) + 0.5) if (wn + ls_) else 0
    for x, val, col, lab in ((66, str(wn), CH_GREEN, "WINS"), (165, str(ls_), CH_RED, "LOSSES"),
                             (264, f"{rate}%", CH_AMBER, "WIN RATE")):
        cv.T(x, 816, val, 26, col, "bold", ha="center")
        cv.T(x, 846, spaced(lab), 11, "#5b6174", ha="center", w="bold")
    cv.T(165, 908, "Trade responsibly. Signals are not guaranteed.", 12, "#5b6174", ha="center", it=True)
    cv.T(165, 943, "\u00a9 DARKMATTER", 14, CH_DIM, "bold", ha="center")
    settled = datetime.fromtimestamp(d["now"], d["tz"]).strftime("%H:%M")
    _footer_main(cv, f"Settled {settled} ({tzs})   |   For informational purposes only")
    return cv.png()


# ---------------------------------------------------------------------


# =====================================================================
#  module: darkmatter/signals/keyboard.py  -  LIVE SIGNAL: keyboard watcher (Ctrl+S or S stops the engine).
# =====================================================================
# =====================================================================
#  LIVE SIGNAL  -  keyboard: Ctrl+S (or S) stops the engine
# =====================================================================
def is_stop_key(ch, vk=0, ctrl=0):
    """Ctrl+S arrives as \\x13; some consoles report it as the S key with Ctrl held. A plain S also stops
    (a few terminals / editors swallow Ctrl+S before the program sees it)."""
    if ch in ("\x13", "s", "S"):
        return True
    return vk == 0x53 and bool(ctrl & 0x000C)             # VK_S + LEFT_CTRL / RIGHT_CTRL


def is_peek_key(ch, vk=0, ctrl=0):
    """Ctrl+P arrives as \\x10 (or as the P key with Ctrl held). A plain P works too: VS Code keeps Ctrl+P
    for its own "Quick Open" and never passes it to the terminal."""
    if ch in ("\x10", "p", "P"):
        return True
    return vk == 0x50 and bool(ctrl & 0x000C)             # VK_P + LEFT_CTRL / RIGHT_CTRL


class KeyWatcher:
    """Watches the keyboard in a daemon thread. Ctrl+S / S sets `pressed` and asks the event loop to call
    `on_stop`; Ctrl+P / P asks it to call `on_peek` (the thread keeps running). Windows: real console key events (ReadConsoleInput), msvcrt as backup;
    Linux / macOS / Termux: cbreak mode with XON/XOFF off, so Ctrl+S reaches the program."""

    def __init__(self, loop, on_stop, on_peek=None):
        self.loop, self.on_stop, self.on_peek = loop, on_stop, on_peek
        self._done = threading.Event()
        self._peek_at = 0.0
        self.pressed = threading.Event()
        self.dead = False                                 # True when no keyboard backend works
        self._thread = None

    def _fire(self):
        self.pressed.set()
        self._done.set()
        try:
            self.loop.call_soon_threadsafe(self.on_stop)
        except RuntimeError:                              # loop already closed
            pass

    def _peek(self):
        """Instant-result key. Auto-repeat of a held key is ignored (0.6 s)."""
        now = time.monotonic()
        if self.on_peek is None or now - self._peek_at < 0.6:
            return
        self._peek_at = now
        try:
            self.loop.call_soon_threadsafe(self.on_peek)
        except RuntimeError:                              # loop already closed
            pass

    # ---- Windows: console input records ----
    def _win_api(self):
        import ctypes
        from ctypes import wintypes

        class KEY(ctypes.Structure):
            _fields_ = [("bKeyDown", wintypes.BOOL), ("wRepeatCount", wintypes.WORD),
                        ("wVirtualKeyCode", wintypes.WORD), ("wVirtualScanCode", wintypes.WORD),
                        ("uChar", wintypes.WCHAR), ("dwControlKeyState", wintypes.DWORD)]

        class EVENT(ctypes.Union):
            _fields_ = [("KeyEvent", KEY), ("raw", ctypes.c_byte * 16)]

        class RECORD(ctypes.Structure):
            _fields_ = [("EventType", wintypes.WORD), ("Event", EVENT)]

        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        k32.GetStdHandle.restype = wintypes.HANDLE
        k32.GetStdHandle.argtypes = [wintypes.DWORD]
        k32.GetConsoleMode.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        k32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        k32.WaitForSingleObject.restype = wintypes.DWORD
        k32.FlushConsoleInputBuffer.argtypes = [wintypes.HANDLE]
        k32.ReadConsoleInputW.argtypes = [wintypes.HANDLE, ctypes.POINTER(RECORD), wintypes.DWORD,
                                          ctypes.POINTER(wintypes.DWORD)]
        h = k32.GetStdHandle(wintypes.DWORD(-10))          # STD_INPUT_HANDLE
        mode = wintypes.DWORD()
        if not h or not k32.GetConsoleMode(h, ctypes.byref(mode)):
            raise OSError("stdin is not a console")
        k32.FlushConsoleInputBuffer(h)                     # forget the Enter of the last prompt
        rec, n = RECORD(), wintypes.DWORD(0)
        while not self._done.is_set():
            if k32.WaitForSingleObject(h, 100) != 0:       # WAIT_OBJECT_0 = input available
                continue
            if not k32.ReadConsoleInputW(h, ctypes.byref(rec), 1, ctypes.byref(n)) or n.value == 0:
                continue
            if rec.EventType != 1:                         # KEY_EVENT
                continue
            k = rec.Event.KeyEvent
            if not k.bKeyDown:
                continue
            if is_stop_key(k.uChar, k.wVirtualKeyCode, k.dwControlKeyState):
                self._fire()
                return
            if is_peek_key(k.uChar, k.wVirtualKeyCode, k.dwControlKeyState):
                self._peek()

    # ---- Windows backup: msvcrt ----
    def _win_msvcrt(self):
        import msvcrt
        while msvcrt.kbhit():
            msvcrt.getwch()
        while not self._done.is_set():
            if msvcrt.kbhit():
                ch = msvcrt.getwch()
                if is_stop_key(ch):
                    self._fire()
                    return
                if is_peek_key(ch):
                    self._peek()
                    continue
                if ch in ("\x00", "\xe0"):
                    msvcrt.getwch()                       # swallow the second code of arrow / function keys
            else:
                self._done.wait(0.05)

    def _win(self):
        try:
            self._win_api()
        except Exception:
            if self._done.is_set():
                return
            try:
                self._win_msvcrt()
            except Exception:
                self.dead = True

    # ---- Linux / macOS / Termux ----
    def _posix(self):
        try:
            import select
            import termios
            fd = sys.stdin.fileno()
            old = termios.tcgetattr(fd)
            new = termios.tcgetattr(fd)
            new[0] &= ~(termios.IXON | termios.IXOFF)
            new[3] &= ~(termios.ICANON | termios.ECHO)
            new[6][termios.VMIN], new[6][termios.VTIME] = 0, 0
            termios.tcsetattr(fd, termios.TCSADRAIN, new)
        except Exception:
            self.dead = True
            return
        try:
            while not self._done.is_set():
                if select.select([fd], [], [], 0.1)[0]:
                    data = os.read(fd, 64)
                    if data.startswith(b"\x1b"):           # arrow / function key sequence: not ours
                        continue
                    if b"\x13" in data or b"s" in data or b"S" in data:
                        self._fire()
                        return
                    if b"\x10" in data or b"p" in data or b"P" in data:
                        self._peek()
        except Exception:
            self.dead = True
        finally:
            try:
                termios.tcsetattr(fd, termios.TCSADRAIN, old)
            except Exception:
                pass

    def start(self):
        """True when a keyboard backend was started; False when stdin is not a terminal."""
        try:
            if not sys.stdin.isatty():
                return False
            target = self._win if sys.platform == "win32" else self._posix
            if sys.platform != "win32":
                import termios  # noqa: F401
        except Exception:
            return False
        self._thread = threading.Thread(target=target, daemon=True, name="keywatch")
        self._thread.start()
        return True

    def stop(self):
        self._done.set()
        if self._thread is not None:
            self._thread.join(1.5)
            self._thread = None


# =====================================================================
#  module: darkmatter/signals/live_series.py  -  LIVE SIGNAL: per-market live candles built from ticks.
# =====================================================================
# =====================================================================
#  LIVE SIGNAL  -  per-market live candles built from ticks
# =====================================================================
def _tick_count(x):
    """Quotex gives the tick count of a history candle as a number (or a list); None when unknown."""
    try:
        if isinstance(x, (list, tuple)):
            return len(x)
        v = float(x)
        return v if v == v and v >= 0 else None
    except (TypeError, ValueError):
        return None


class LiveSeries:
    """History + live candles of one market. A new candle opens at the previous candle's last price
    (the Quotex chart is gap-free), so the entry candle's open / close are what the trader sees."""

    def __init__(self, item, period=60):
        self.item = item
        self.symbol = item["symbol"]
        self.period = period
        self.closed, self.cur = [], None
        self.ticks = deque(maxlen=800)
        self.last_ts, self.last_price = 0.0, None
        self.last_mono, self.last_resub = 0.0, 0.0
        self.n_ticks = 0
        self.dec = 2

    def seed(self, candles, now):
        p = self.period
        b = int(now // p * p)
        rows = {int(c["t"]): c for c in candles}
        keep = ("o", "h", "l", "c")
        mk = lambda t: {"t": t, **{k: rows[t][k] for k in keep}, "v": _tick_count(rows[t].get("ticks"))}
        self.closed = [mk(t) for t in sorted(rows) if t < b]
        part = rows.get(b)
        self.cur = mk(b) if part else None
        d = price_decimals(candles[-100:]) if candles else 2
        self.dec = min(max(d, 2), 6)

    def add_tick(self, ts, price, mono):
        p = self.period
        b = int(ts // p * p)
        cur = self.cur
        if cur is None:
            if self.closed and self.closed[-1]["t"] >= b:
                if self.closed[-1]["t"] > b:
                    return False                              # late tick of an older candle
                cur = self.cur = self.closed.pop()            # the seeded last candle was still forming
            else:
                prev = self.closed[-1] if self.closed else None
                o = prev["c"] if (prev and prev["t"] == b - p) else price
                cur = self.cur = {"t": b, "o": o, "h": o, "l": o, "c": o, "v": 0}
        elif b > cur["t"]:
            self.closed.append(cur)
            if len(self.closed) > SERIES_KEEP:
                del self.closed[:len(self.closed) - SERIES_KEEP]
            o = cur["c"] if b == cur["t"] + p else price
            cur = self.cur = {"t": b, "o": o, "h": o, "l": o, "c": o, "v": 0}
        elif b < cur["t"]:
            return False
        if price > cur["h"]:
            cur["h"] = price
        if price < cur["l"]:
            cur["l"] = price
        cur["c"] = price
        cur["v"] = (cur.get("v") or 0) + 1                   # ticks of this candle (the tick-volume panel)
        self.ticks.append((ts, price))
        self.last_ts = max(self.last_ts, ts)
        self.last_price, self.last_mono = price, mono
        self.n_ticks += 1
        return True

    def candle_at(self, t):
        if self.cur is not None and self.cur["t"] == t:
            return dict(self.cur)
        for c in reversed(self.closed[-10:]):
            if c["t"] == t:
                return dict(c)
            if c["t"] < t:
                break
        return None

    def snapshot(self, now, keep):
        cl = self.closed[-keep:]
        cutoff = now - 60.0
        return {
            "symbol": self.symbol,
            "t": [x["t"] for x in cl], "v": [x.get("v") for x in cl],
            "o": [x["o"] for x in cl], "h": [x["h"] for x in cl],
            "l": [x["l"] for x in cl], "c": [x["c"] for x in cl],
            "cur": dict(self.cur) if self.cur else None,
            "ticks": [t for t in self.ticks if t[0] >= cutoff],
            "tick_age": (now - self.last_ts) if self.last_ts else 1e9,
            "now": now, "dec": self.dec, "bucket": int(now // self.period * self.period),
        }


# =====================================================================
#  module: darkmatter/ui/signal_terminal.py  -  LIVE SIGNAL: terminal look of the signal / result / session messages.
# =====================================================================
# =====================================================================
#  LIVE SIGNAL  -  terminal look: the exact message text, coloured line by line in the bot's theme
# =====================================================================
def _c(n, bold=False):
    return f"\033[{'1;' if bold else ''}38;5;{n}m"


K_WHITE = _c(255)
K_YELLOW, K_YELLOW_B, K_GREEN = _c(221), _c(220, True), _c(84)
K_RED, K_RED_B, K_ORANGE, K_ORANGE_B = _c(203), _c(203, True), _c(214), _c(214, True)
K_BLUE, K_PURPLE = _c(75), _c(141)

BAR_FULL, BAR_LIGHT = "\u2588", "\u2591"

# Message colours follow the bot's own theme (steel-blue lines, light text, green / red / amber
# for good / bad / waiting, sky-blue highlights).
OK_B, BAD_B, WARN_B, ACC_B = "\033[1;38;5;78m", "\033[1;38;5;203m", "\033[1;38;5;179m", "\033[1;38;5;75m"
KIND_COLOR = {"sep": BORDER, "title": TITLE, "asset": TITLE, "ok": OK_B, "bad": BAD_B, "warn": WARN_B,
              "acc": ACC_B, "text": TEXT, "dim": DIM, "head": ACC_B, "reason": TEXT}
# Colour scheme of the signal / result messages: labels white, values coloured.
KIND_COLOR.update({
    "white": "\033[1;38;5;255m",      # labels and neutral values (bold white)
    "plain": "\033[38;5;255m",        # plain white text (PARTIAL RESULT)
    "yel": "\033[1;38;5;220m",        # entry time
    "mag": "\033[1;38;5;213m",        # owner
    "head": "\033[1;38;5;220m",       # REASON heading
    "reason": "\033[38;5;255m",       # reason paragraph
})


def _paint_rows(rows):
    """Colour every row of a message. The text itself is never changed, so the terminal shows exactly
    the same text that goes to Telegram."""
    out = []
    width = max(30, min(term_width() - 2, 110))
    for text, kind in rows:
        if isinstance(kind, tuple):                   # ("seg", [(text, kind), ...]): label / value colours
            out.append("".join(f"{KIND_COLOR.get(k, '')}{t}{R}" for t, k in kind[1] if t))
            continue
        col = KIND_COLOR.get(kind)
        parts = wrap_text(text, width) if kind == "reason" else [text]
        for ln in parts:
            out.append(f"{col}{ln}{R}" if col and ln else ln)
    return out


def render_signal_terminal(sig, cfg, tz):
    return _paint_rows(signal_rows(sig, cfg, tz))


def render_result_terminal(r, cfg, tz):
    return _paint_rows(result_rows(r, cfg, tz))


def render_session_terminal(stats, tz, now_ts):
    return _paint_rows(session_rows(stats, tz, now_ts))


# =====================================================================
#  module: darkmatter/ui/fetch_line.py  -  LIVE SIGNAL: history download display (one coloured bar per market).
# =====================================================================
# =====================================================================
#  LIVE SIGNAL  -  history download display (one coloured bar per market)
# =====================================================================
class FetchLine:
    """`EUR/NZD (OTC)  [██████████] 100.0%  200 candles  2s`, redrawn in place while it downloads."""

    def __init__(self, label, lw, total, width=22):
        self.label, self.lw, self.total, self.width = label, lw, max(1, total), width
        self.t0 = time.monotonic()
        self.last_draw = 0.0
        self.got = 0
        self.tty = sys.stdout.isatty()

    def _line(self, got, final=False):
        frac = 1.0 if final else min(max(got / self.total, 0.0), 1.0)
        filled = int(round(self.width * frac))
        el = time.monotonic() - self.t0
        bar = (f"{K_WHITE}[{R}{K_GREEN}{BAR_FULL * filled}{R}{DIM}{BAR_LIGHT * (self.width - filled)}{R}{K_WHITE}]{R}")
        return (f"{K_YELLOW}{pad(self.label, self.lw)}{R} {bar} {K_YELLOW_B}{frac * 100:5.1f}%{R} "
                f"{K_GREEN}{got:>4} candles{R} {K_BLUE}{el:.0f}s{R}")

    def update(self, got, total=None):
        self.got = got
        now = time.monotonic()
        if not self.tty or now - self.last_draw < 0.05:
            return
        self.last_draw = now
        sys.stdout.write("\r\x1b[2K" + (self._line(got) if USE_COLOR else strip_ansi(self._line(got))))
        sys.stdout.flush()

    def finish(self, got):
        line = self._line(got, final=True)
        if self.tty:
            sys.stdout.write("\r\x1b[2K")
        put(line)
        flush()

    def abort(self):
        if self.tty:
            sys.stdout.write("\r\x1b[2K")
            sys.stdout.flush()


# =====================================================================
#  module: darkmatter/ui/status_line.py  -  LIVE SIGNAL: status line.
# =====================================================================
# =====================================================================
#  LIVE SIGNAL  -  status line, Ctrl+S key watcher
# =====================================================================
class StatusLine:
    """One line at the bottom that is redrawn in place; it is wiped before any message is printed."""

    def __init__(self):
        self.tty = sys.stdout.isatty()
        self.shown = False

    def draw(self, text):
        if not self.tty:
            return
        out = "\r\x1b[2K" + clip_ansi(text, max(20, term_width() - 1))
        sys.stdout.write(out if USE_COLOR else strip_ansi(out))
        sys.stdout.flush()
        self.shown = True

    def clear(self):
        if self.tty and self.shown:
            sys.stdout.write("\r\x1b[2K")
            sys.stdout.flush()
            self.shown = False


# =====================================================================
#  module: darkmatter/signals/live_session.py  -  LIVE SIGNAL: the session - history, live ticks, scan, signal, MTG tracking, result.
# =====================================================================
# =====================================================================
#  LIVE SIGNAL  -  the session: history, live ticks, scan, signal, MTG tracking, result
# =====================================================================
class LiveSignalSession:
    HEADER_TITLE = "LIVE SIGNAL ENGINE"
    def __init__(self, ws, cfg, time_fn=time.time, notifier=None):
        self.ws, self.cfg, self.time_fn = ws, cfg, time_fn
        self.tg = notifier                  # TelegramNotifier or None (feature off)
        tgc = cfg.get("tg") or {}
        self.want_sig = bool(notifier and tgc.get("chart_sig", True))      # chart image with every SIGNAL
        self.want_res = bool(notifier and tgc.get("chart_res", True))      # chart image with every RESULT
        self.charts_ok = False              # set by the flow once matplotlib / mplfinance imported fine
        self.charts_err = None
        self.started_ts = None              # server time when the session started
        self.tz = timezone(timedelta(minutes=cfg["tz_min"]))
        self.series = {}                    # symbol -> LiveSeries
        self.cool = {}                      # symbol -> (candle time, direction) of its latest signal (cooldown)
        self.skipped = []                   # [(market, reason)]
        self.offsets = deque(maxlen=61)     # server time - local time, from tick timestamps
        self.stats = SessionStats(cfg["mtg"])
        self.trade = None
        self.n_signals = 0
        self.results = []                   # last analysis, list of result dicts
        self.results_at = 0.0
        self.status = StatusLine()
        self.tick_interval = 0.25
        self._task = None
        self._scan_bucket = None
        self._bg_at = 0.0
        self._status_at = 0.0
        self._warned = set()

    # ---------------- clock (server time, learned from the tick timestamps) ----------------
    def offset(self):
        if not self.offsets:
            return 0.0
        s = sorted(self.offsets)
        return s[len(s) // 2]

    def now(self):
        return self.time_fn() + self.offset()

    # ---------------- ticks ----------------
    def on_tick(self, asset, ts, price):
        s = self.series.get(asset)
        if s is None or not (price == price) or price <= 0:
            return
        if ts > 1e11:                       # milliseconds -> seconds
            ts /= 1000.0
        if s.add_tick(ts, price, time.monotonic()):
            self.offsets.append(ts - self.time_fn())

    # ---------------- output ----------------
    def emit(self, text, color=TEXT):
        self.status.clear()
        put()
        for ln in text.split("\n"):
            put(f"{color}{ln}{R}")
        flush()

    def emit_lines(self, lines, blank=True):
        """Print ready-made (coloured) lines above the status line."""
        self.status.clear()
        if blank:
            put()
        for ln in lines:
            put(ln)
        flush()

    def telegram(self, build, *args):
        """Queue a Telegram message. Never raises and never waits: the engine must not depend on Telegram."""
        if self.tg is None:
            return
        try:
            self.tg.send(build(*args))
        except Exception as e:
            self.warn_once("tg-build", f"Telegram message could not be prepared: {type(e).__name__}: {e}")

    def note(self, msg, kind="warn"):
        self.status.clear()
        (warn if kind == "warn" else info)(msg)
        flush()

    def warn_once(self, key, msg):
        if key not in self._warned:
            self._warned.add(key)
            self.note(msg)

    # ---------------- step 1: history for every selected market, then live ticks ----------------
    async def load(self, picked):
        n = self.cfg["n"]
        self.ws.tick_cb = self.on_tick      # ticks of already loaded markets keep building candles meanwhile
        begin_screen()
        put()
        put(f"  {WARN}\u25cc Fetching {n} historical candles per market (deep fetch - please wait) \u2026{R}")
        names = {it["symbol"]: clip(it.get("name") or sig_symbol(it["symbol"]), 24) for it in picked}
        lw = max(dw(v) for v in names.values())
        for it in picked:
            label = names[it["symbol"]]
            line = FetchLine(label, lw, n)
            try:
                candles = await self.ws.get_candles(it["symbol"], 60, n, progress=line.update)
            except (KeyboardInterrupt, asyncio.CancelledError):
                line.abort()
                raise
            except QuotexError as e:
                line.abort()
                self.skipped.append((it, str(e).split(". ")[0]))
                put(f"  {K_RED_B}\u2716{R} {TEXT}{label}: {K_RED}{clip(self.skipped[-1][1], 60)}{R}")
                continue
            except Exception as e:          # one broken market must not stop the others
                line.abort()
                self.skipped.append((it, f"{type(e).__name__}: {e}"))
                put(f"  {K_RED_B}\u2716{R} {TEXT}{label}: {K_RED}{clip(self.skipped[-1][1], 60)}{R}")
                continue
            line.finish(len(candles))
            if len(candles) < MIN_BARS:
                why = f"only {len(candles)} candles returned (need {MIN_BARS}+)"
                self.skipped.append((it, why))
                put(f"  {K_ORANGE_B}!{R} {TEXT}{label}: {K_ORANGE}{why}{R}")
                continue
            s = LiveSeries(it)
            s.seed(candles, self.time_fn())
            self.series[it["symbol"]] = s
            put(f"  {K_PURPLE}\u2714{R} {TEXT}{label}: {len(candles)} candles{R}")
            flush()
        if not self.series:
            return False
        put()
        for sym in list(self.series):
            await self.ws.subscribe(sym, 60)
            await asyncio.sleep(0.02)
        async with Progress("Starting live tick monitor"):
            t0 = time.monotonic()
            while time.monotonic() - t0 < 10.0:
                if all(s.last_price is not None for s in self.series.values()):
                    break
                await asyncio.sleep(0.2)
        return True

    # ---------------- analysis ----------------
    def _market_info(self):
        try:
            live, otc = market_items(self.ws.instruments)
        except Exception:
            return {}
        return {x["symbol"]: x for x in live + otc}

    def _snapshots(self, now):
        keep = self.cfg["n"] + 10
        out = []
        for s in self.series.values():
            snap = s.snapshot(now, keep)
            snap["last_sig"] = self.cool.get(s.symbol)
            out.append(snap)
        return out

    def _running(self):
        return self._task is not None and not self._task.done()

    async def _analyze(self):
        snaps = self._snapshots(self.now())
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, analyze_many, snaps)

    async def _bg(self):
        try:
            self.results = await self._analyze()
            self.results_at = self.now()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.warn_once("bg", f"Background analysis error: {type(e).__name__}: {e}")

    async def _scan(self, bucket):
        try:
            entry_ts = (bucket + 1) * 60
            results = await self._analyze()
            self.results, self.results_at = results, self.now()
            if self.trade is not None:
                return
            if entry_ts - self.now() < SIGNAL_MIN_LEAD:
                self.note("Scan finished too late for this candle - waiting for the next one.")
                return
            mi = self._market_info()
            best, best_score = None, -1.0
            for r in results:
                if r["skip"] or r["conf"] < MIN_CONF:
                    continue
                m = mi.get(r["symbol"])
                if m is not None and m["open"] is False:
                    continue
                score = r["conf"] + ((m["payout"] or 0) / 100.0 if m else 0.0)   # payout only breaks ties
                if score > best_score:
                    best, best_score = r, score
            if best is not None:
                self._issue(best, entry_ts, mi.get(best["symbol"]))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.warn_once("scan", f"Scan error: {type(e).__name__}: {e}")

    # ---------------- chart images (Telegram only) ----------------
    def _render_fn(self, fn, data):
        return (lambda: fn(data))

    def _candles_for_chart(self, symbol, upto=None, last=None):
        """Copy of the market's candles (the drawing happens later in another thread): the closed candles
        before `upto` (or all of them) plus `last` / the forming candle."""
        s = self.series[symbol]
        cs = [dict(c) for c in s.closed if upto is None or c["t"] < upto][-(CH_SHOW + 120):]
        extra = last if last is not None else (dict(s.cur) if (upto is None and s.cur) else None)
        if extra is not None:
            cs.append(dict(extra))
        if len(cs) < 30:
            raise ValueError("not enough candles for a chart")
        return cs

    def _signal_render(self, sig):
        if not (self.want_sig and self.charts_ok):
            return None
        try:
            cs = self._candles_for_chart(sig["symbol"])
            data = {"candles": cs, "dir": sig["dir"], "conf": sig["conf"],
                    "price": sig.get("price") if sig.get("price") is not None else cs[-1]["c"],
                    "support": sig.get("support"), "resistance": sig.get("resistance"),
                    "atr": sig.get("atr"), "adx": sig.get("adx"), "dec": sig["dec"], "payout": sig.get("payout"),
                    "mtg": self.cfg["mtg"], "entry_ts": sig["entry_ts"], "tz": self.tz, "tz_min": self.cfg["tz_min"],
                    "now": self.now(), "symbol": sig["symbol"]}
            return self._render_fn(render_signal_png, data)
        except Exception as e:
            self.warn_once("chart-sig", f"Signal chart skipped: {type(e).__name__}: {e}")
            return None

    def _result_render(self, t, r, c):
        if not (self.want_res and self.charts_ok):
            return None
        try:
            start = t["entry_ts"] + 60 * t["step"]
            cs = self._candles_for_chart(t["symbol"], upto=start, last=c)
            data = {"candles": cs, "dir": t["dir"], "win": r["win"], "conf": t["conf"], "open": r["open"],
                    "close": r["close"], "price": t.get("price"), "support": t.get("support"),
                    "resistance": t.get("resistance"), "dec": r["dec"], "payout": t.get("payout"),
                    "mtg": self.cfg["mtg"], "step": t["step"], "entry_ts": t["entry_ts"], "wins": r["wins"],
                    "losses": r["losses"], "tz": self.tz, "tz_min": self.cfg["tz_min"], "now": self.now(),
                    "symbol": t["symbol"]}
            return self._render_fn(render_result_png, data)
        except Exception as e:
            self.warn_once("chart-res", f"Result chart skipped: {type(e).__name__}: {e}")
            return None

    async def send_start(self, picked):
        """Telegram: the BE READY banner with the session overview picture. Sent right after Enter on the setup
        screen (before the history is loaded) so the members can get ready."""
        if self.started_ts is None:
            self.started_ts = self.now()
        if self.tg is None:
            return
        render = None
        try:
            lat = None
            try:
                t0 = time.monotonic()
                me, err, _ = await asyncio.wait_for(tg_probe(self.tg.token, "getMe", {}, self.tg.base), 6.0)
                if err is None:
                    lat = max(1, int((time.monotonic() - t0) * 1000))
            except asyncio.TimeoutError:
                pass
            if self.charts_ok:
                pays = [it.get("payout") for it in picked if isinstance(it.get("payout"), (int, float))]
                data = {"tz": self.tz, "tz_min": self.cfg["tz_min"], "now": self.now(),
                        "sid": session_id(self.started_ts, self.tz), "mtg": self.cfg["mtg"],
                        "markets": len(picked),
                        "all_otc": all(str(it["symbol"]).lower().endswith("_otc") for it in picked),
                        "min_conf": float(MIN_CONF), "min_payout": min(pays) if pays else None,
                        "latency_ms": lat}
                render = self._render_fn(render_overview_png, data)
        except Exception as e:
            self.warn_once("chart-start", f"Session overview picture skipped: {type(e).__name__}: {e}")
        self.telegram(tg_start_messages, render)

    def _end_render(self, now_ts):
        if not self.charts_ok:
            return None
        try:
            start = self.started_ts if self.started_ts is not None else now_ts
            data = {"nums": session_numbers(self.stats, start, now_ts), "tz": self.tz,
                    "tz_min": self.cfg["tz_min"], "start": start, "now": now_ts,
                    "sid": session_id(start, self.tz)}
            return self._render_fn(render_summary_png, data)
        except Exception as e:
            self.warn_once("chart-end", f"Session summary picture skipped: {type(e).__name__}: {e}")
            return None

    # ---------------- signal ----------------
    def _issue(self, best, entry_ts, market):
        dec = best["dec"]
        fmt = lambda v: f"{v:.{dec}f}"
        sig = dict(best, entry_ts=entry_ts, payout=market["payout"] if market else None)
        sig["reason"] = build_reason(best, fmt)
        self.trade = {"symbol": best["symbol"], "dir": best["dir"], "entry_ts": entry_ts, "step": 0,
                      "dec": dec, "conf": best["conf"], "payout": sig.get("payout"), "price": sig.get("price"),
                      "support": sig.get("support"), "resistance": sig.get("resistance")}
        self.n_signals += 1
        self.cool[best["symbol"]] = (entry_ts - 60, best["dir"])      # starts this market's cooldown
        self.telegram(tg_signal_messages, sig, self.cfg, self.tz, self._signal_render(sig))
        self.emit_lines(render_signal_terminal(sig, self.cfg, self.tz))

    # ---------------- result / MTG ----------------
    def _resolve(self, now):
        t = self.trade
        if t is None:
            return
        start = t["entry_ts"] + 60 * t["step"]
        if now < start + 60 + RESULT_GRACE:
            return
        c = self.series[t["symbol"]].candle_at(start)
        d = t["dec"]
        if c is None:
            self.trade = None
            self.note(f"{sig_symbol(t['symbol'])}: no price data for the entry candle - signal ignored.")
            return
        o, cl = round(c["o"], d), round(c["c"], d)
        if o == cl:
            self.trade = None
            self.note(f"{sig_symbol(t['symbol'])}: candle closed flat (tie / refund) - signal ignored.")
            return
        win = (cl > o) if t["dir"] > 0 else (cl < o)
        if not win and t["step"] < self.cfg["mtg"]:
            t["step"] += 1                                  # same direction on the next candle
            msg = f"\U0001F525MTG STEP {t['step']} MONITORING \U0001F525"
            self.emit_lines([f"{WARN_B}{msg}{R}"])
            return
        self.stats.add(t["symbol"], t["dir"], t["entry_ts"], win, t["step"], t.get("payout"))
        self.trade = None
        r = {"symbol": t["symbol"], "entry_ts": t["entry_ts"], "dir": t["dir"], "win": win, "step": t["step"],
             "open": o, "close": cl, "dec": d, "wins": self.stats.wins, "losses": self.stats.losses,
             "pair": self.stats.pair(t["symbol"])}
        self.telegram(tg_result_messages, r, self.cfg, self.tz, self._result_render(t, r, c))
        self.emit_lines(render_result_terminal(r, self.cfg, self.tz))
        self.emit_lines([f"{WARN}\u25cc{R} {DIM}Scanning next signal \u2026{R}"], blank=False)

    # ---------------- housekeeping ----------------
    async def _watchdog(self):
        mono = time.monotonic()
        for s in self.series.values():
            if mono - max(s.last_mono, s.last_resub) > 20.0:
                s.last_resub = mono
                try:
                    await self.ws.subscribe(s.symbol, 60)
                except Exception:
                    pass                                    # socket briefly unavailable; retried in 20 s

    def _status_text(self, now):
        w, l = self.stats.wins, self.stats.losses
        score = (f"{DIM}W{R} {OK_B}{w}{R} {DIM}L{R} {BAD_B}{l}{R} "
                 f"{DIM}({_pct(w, l):.1f}%){R}")
        t = self.trade
        spin = "|/-\\"[int(time.monotonic() * 4) % 4]
        if t is not None:
            left = max(0, int(t["entry_ts"] + 60 * (t["step"] + 1) - now))
            side = "CALL" if t["dir"] > 0 else "PUT"
            what = "entry candle" if t["step"] == 0 else f"MTG {t['step']} candle"
            at = _hhmm(t["entry_ts"] + 60 * t["step"], self.tz)
            mid = (f"{ACCENT}\u23f3 Monitoring {what} {at} \u2026{R} "
                   f"{TITLE}{sig_symbol(t['symbol'])} {side}{R} "
                   f"{DIM}closes in {left}s \u00b7 MTG {t['step']}/{self.cfg['mtg']}{R}")
        else:
            ok = [r for r in self.results if not r["skip"]]
            best = max(ok, key=lambda r: r["conf"]) if ok else None
            wait = (SIGNAL_SECOND - now % 60) % 60
            top = (f" {DIM}\u00b7 best{R} {TEXT}{sig_symbol(best['symbol'])} "
                   f"{'CALL' if best['dir'] > 0 else 'PUT'} {int(best['conf'] + 0.5)}%{R}") if best else ""
            mid = (f"{WARN}{spin}{R} {TEXT}Scanning next signal \u2026{R} {DIM}{len(self.series)} markets \u00b7 "
                   f"next check {int(wait)}s{R}{top}")
        return f"  {mid}  {DIM}\u2502{R}  {score}  {DIM}\u2502  Ctrl+P = result \u00b7 Ctrl+S = stop{R}"

    async def _tick(self):
        now = self.now()
        self._resolve(now)
        sec = now % 60
        bucket = int(now // 60)
        if (self.trade is None and not self._running() and bucket != self._scan_bucket
                and SIGNAL_SECOND <= sec <= SIGNAL_LATEST_START):
            self._scan_bucket = bucket
            self._task = asyncio.create_task(self._scan(bucket))
        elif (not self._running() and time.monotonic() - self._bg_at >= BG_INTERVAL
              and not (SIGNAL_SECOND - 3 <= sec <= SIGNAL_LATEST_START + 1)):
            self._bg_at = time.monotonic()
            self._task = asyncio.create_task(self._bg())
        await self._watchdog()
        mono = time.monotonic()
        if mono - self._status_at >= 0.5:
            self._status_at = mono
            self.status.draw(self._status_text(now))

    async def _sleep(self, stop):
        try:
            await asyncio.wait_for(stop.wait(), self.tick_interval)
        except asyncio.TimeoutError:
            pass

    # ---------------- main loop ----------------
    def print_header(self):
        box = Box(self.HEADER_TITLE)
        box.row(f"{dot(OK)} {OK}RUNNING{R}   {DIM}\u2502{R}   {TEXT}{len(self.series)} markets{R}   {DIM}\u2502{R}   "
                f"{TEXT}{ENGINE_NAME}{R}   {DIM}\u2502{R}   {TEXT}{len(ACTIVE_FILTERS)} filter{'' if len(ACTIVE_FILTERS) == 1 else 's'}{R}   {DIM}\u2502{R}   "
                f"{TEXT}M1{R}   {DIM}\u2502{R}   "
                f"{TEXT}{self.cfg['n']} candles{R}")
        mtg_txt = "NO MTG" if self.cfg["mtg"] == 0 else "STEP " + str(self.cfg["mtg"])
        box.row(f"{kv('OWNER', self.cfg['owner'], 8)}   {kv('TIMEZONE', tz_label(self.cfg['tz_min']), 9)}   "
                f"{kv('MTG', mtg_txt, 4)}   {kv('MIN CONF', str(MIN_CONF) + '%', 9)}")
        if ENGINE_PROBLEM:
            for ln in wrap_text(ENGINE_PROBLEM, box.inner):
                box.row(f"{WARN}{ln}{R}")
        box.rule("MARKETS")
        tags = []
        for s in self.series.values():
            it = s.item
            tags.append(f"{ACCENT}{it['symbol']}{R} {DIM}{fmt_payout(it['payout'])}{R}")
        for ln in wrap_plain(tags, box.inner, "   "):
            box.row(ln)
        if self.skipped:
            box.rule("SKIPPED")
            for it, why in self.skipped:
                box.row(f"{WARN}{it['symbol']}{R} {DIM}{clip(why, box.inner - len(it['symbol']) - 2)}{R}")
        box.rule()
        box.row(f"{DIM}The best entry is published {60 - SIGNAL_LATEST_START}-{60 - SIGNAL_SECOND} seconds before the candle "
                f"starts, then watched until the result.{R}")
        box.row(f"{chip('Ctrl+P')} {DIM}(or{R} {chip('P')}{DIM}) instant session result - the session keeps running.{R}")
        box.row(f"{chip('Ctrl+S')} {DIM}(or{R} {chip('S')}{DIM}) stop the engine and show the session result.{R}")
        if self.tg is not None:
            box.row(f"{kv('TELEGRAM', 'ON  signals, results and the final session result are sent', 9)}")
            if not self.charts_ok:
                why = f" ({clip(self.charts_err, 50)})" if self.charts_err else ""
                box.row(f"{WARN}Chart images are off - Chromium could not draw them{why}{R}")
            else:
                pics = [n for n, on in (("signal", self.want_sig), ("result", self.want_res)) if on]
                box.row(f"{kv('CHARTS', ('image with every ' + ' and every '.join(pics)) if pics else 'off for signals and results', 9)}"
                        f"   {DIM}+ session start / end pictures{R}")
        put()
        box.show()

    async def run(self, stop):
        loop = asyncio.get_running_loop()
        watcher = KeyWatcher(loop, stop.set, self.show_partial)
        fallback = None

        def start_fallback(reason):
            nonlocal fallback
            if fallback is not None:
                return

            async def wait_line():
                try:
                    await ainput("")
                except (EOFError, KeyboardInterrupt):
                    pass
                stop.set()
            fallback = asyncio.create_task(wait_line())
            self.note(f"{reason} - press Enter to stop.", "info")

        if not watcher.start():
            start_fallback("Keyboard not available")
        if self.started_ts is None:
            self.started_ts = self.now()
        self.emit_lines([f"{WARN}\u25cc{R} {DIM}Scanning next signal \u2026{R}"], blank=False)
        try:
            while not stop.is_set():
                if watcher.pressed.is_set():                  # key seen by the thread (no loop wake-up needed)
                    stop.set()
                    break
                if watcher.dead:
                    start_fallback("Keyboard watcher stopped")
                await self._tick()
                await self._sleep(stop)
        finally:
            watcher.stop()
            if self._task is not None:
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
            if fallback is not None and not fallback.done():
                fallback.cancel()
            self.status.clear()
            self.ws.tick_cb = None

    def show_partial(self, final=False, send=False):
        """Print the PARTIAL RESULT. `final=False` is the Ctrl+P snapshot: nothing is stopped or changed.
        Only `send=True` (Ctrl+S / S) also sends it to Telegram."""
        if self.trade is not None:
            self.note(f"{sig_symbol(self.trade['symbol'])} is still running - it is not part of this result"
                      f"{'' if final else ' yet'}.")
        if not self.stats.log:
            self.note("No finished signal in this session yet.", "info")
        now_ts = self.now()
        self.emit_lines(render_session_terminal(self.stats, self.tz, now_ts))
        if send:
            self.telegram(tg_session_messages, self.stats, self.tz, now_ts)
            self.telegram(tg_end_messages, self._end_render(now_ts))
        if not final:
            self.emit_lines([f"{WARN}\u25cc Session still running \u2026{R} {DIM}Ctrl+S = stop{R}"], blank=False)

    def finish(self, send=True):
        """Print the final PARTIAL RESULT of the session. `send` (Ctrl+S / S / Enter fallback) also sends it
        to Telegram; after Ctrl+C it is only shown."""
        self.show_partial(final=True, send=send)


# =====================================================================
#  module: darkmatter/flows/setup_flow.py  -  Signal setup screen (timezone, MTG, Telegram, charts) and live_signal_flow.
# =====================================================================
_TZ_RE = re.compile(r"^(?:utc|gmt)?([+-])?([0-9]{1,2})(?::?([0-9]{2}))?$")


def parse_tz(text, default):
    """'' -> default, '6' / '+6' / 'UTC+06:00' / '+0530' / '-3' -> offset in minutes, else None."""
    t = text.strip().lower().replace(" ", "")
    if not t:
        return default
    m = _TZ_RE.match(t)
    if not m or not t.isascii():
        return None
    mins = int(m.group(3) or 0)
    if mins >= 60:
        return None
    total = (-1 if m.group(1) == "-" else 1) * (int(m.group(2)) * 60 + mins)
    return total if -12 * 60 <= total <= 14 * 60 else None


def parse_int_range(text, default, lo, hi):
    t = text.strip()
    if not t:
        return default
    if not (t.isascii() and t.isdecimal()):
        return None
    n = int(t)
    return n if lo <= n <= hi else None


def parse_mtg(text, default):
    t = text.strip().lower()
    if t in ("no", "none", "no mtg"):
        return 0
    return parse_int_range(t, default, 0, MTG_MAX)


def parse_owner(text, default):
    t = " ".join("".join(ch for ch in text if ch.isprintable()).split())[:40]
    return t or default


SETUP_DEFAULTS = {"n": SIGNAL_CANDLES_DEFAULT, "owner": OWNER_DEFAULT, "tz_min": TZ_DEFAULT_MIN,
                  "mtg": MTG_DEFAULT}

# The four plain settings (rows 1-4 of the list). `parse(text, current)` returns the new value, the current
# one for an empty text, or None when the text is not valid.
SETUP_FIELDS = {
    1: {"key": "n", "label": "HISTORY CANDLES", "hint": f"range {SIGNAL_CANDLES_MIN} - {SIGNAL_CANDLES_MAX}",
        "show": lambda v: str(v),
        "help": [f"{DIM}Historical 1-minute candles per market that the analysis uses.{R}",
                 f"{DIM}Range {SIGNAL_CANDLES_MIN} - {SIGNAL_CANDLES_MAX}  \u00b7  default {SIGNAL_CANDLES_DEFAULT}{R}"],
        "parse": lambda t, cur: parse_int_range(t, cur, SIGNAL_CANDLES_MIN, SIGNAL_CANDLES_MAX),
        "err": f"Please enter a whole number between {SIGNAL_CANDLES_MIN} and {SIGNAL_CANDLES_MAX}."},
    2: {"key": "owner", "label": "OWNER", "hint": "shown at the bottom of every signal",
        "show": lambda v: v,
        "help": [f"{DIM}Name shown at the bottom of every signal (up to 40 characters).{R}",
                 f"{DIM}Default {OWNER_DEFAULT}{R}"],
        "parse": lambda t, cur: parse_owner(t, cur),
        "err": "Please enter a name."},
    3: {"key": "tz_min", "label": "TIMEZONE", "hint": "entry times and the session date",
        "show": lambda v: tz_label(v),
        "help": [f"{DIM}Entry times and the session date use this timezone.{R}",
                 f"{DIM}Examples: {R}{chip('+6')} {chip('UTC+06:00')} {chip('+5:30')} {chip('-3')} {chip('0')}"],
        "parse": lambda t, cur: parse_tz(t, cur),
        "err": "Not a valid offset. Use hours (+6, -3) or hh:mm (+5:30), between -12:00 and +14:00."},
    4: {"key": "mtg", "label": "MTG STEPS", "hint": f"0 = no MTG, up to {MTG_MAX}",
        "show": lambda v: "NO MTG" if v == 0 else f"STEP {v}",
        "help": [f"{DIM}After a lost entry the bot watches the next candle in the SAME direction.{R}",
                 f"{chip('0')} {DIM}no MTG{R}   {chip('1')} {DIM}1 step{R}   {chip('2')} {DIM}2 steps{R}   "
                 f"{chip('3')} {DIM}3 steps{R}"],
        "parse": lambda t, cur: parse_mtg(t, cur),
        "err": f"Please enter 0, 1, 2 or {MTG_MAX}."},
}


def new_tg_state():
    return {"on": False, "token": None, "bot": None, "chat": None, "title": None,
            "chart_sig": True, "chart_res": True}


def tg_remember(tgs):
    """Remember a complete token + chat pair for the rest of this run (memory only, never written to disk)."""
    if tgs.get("token") and tgs.get("chat"):
        TG_STATE.update(token=tgs["token"], bot=tgs.get("bot"), chat=tgs["chat"], title=tgs.get("title"))


def tg_remember_token(token, bot):
    """A checked token is remembered even when the chat step is cancelled. A different bot drops the old chat."""
    if TG_STATE.get("token") != token:
        TG_STATE.clear()
    TG_STATE.update(token=token, bot=bot)


def draw_setup(picked, cfg, tgs, edit=None):
    """The LIVE SIGNAL setup screen: the selected markets and one list with every setting. `edit` adds a
    help box (label, help rows, optional Enter text) under the list for the setting being changed."""
    begin_screen()
    put()
    print_selection(picked)
    box = Box("LIVE SIGNAL SETUP")
    wide = box.inner >= 90                            # room for the NOTE column
    lw = 17
    vw = min(44, max(26, box.inner - 5 - lw - 46)) if wide else max(10, min(44, box.inner - 5 - lw))

    def row(num, label, value, hint, color):
        line = f"{chip(f'{num:>2}')} {TEXT}{pad(label, lw)}{R}{color}{pad(clip(value, vw - 1), vw)}{R}"
        if wide:
            line += f"{DIM}{hint}{R}"
        box.row(line)

    box.row(f"{DIM}{pad('#', 4)} {pad('SETTING', lw)}{pad('VALUE', vw)}{'NOTE' if wide else ''}{R}")
    box.rule()
    for num, f in SETUP_FIELDS.items():
        v = cfg[f["key"]]
        row(num, f["label"], f["show"](v), f["hint"], TEXT if v == SETUP_DEFAULTS[f["key"]] else ACCENT)
    row(5, "TELEGRAM", "ON" if tgs["on"] else "OFF", "send signals, results & session result",
        OK if tgs["on"] else DIM)
    if tgs["on"]:
        tok = tg_mask(tgs["token"]) + (f"  @{tgs['bot']}" if tgs.get("bot") else "") if tgs.get("token") else None
        chat = (str(tgs["chat"]) + (f"  {tgs['title']}" if tgs.get("title") else "")) if tgs.get("chat") else None
        row(6, "\u251c BOT TOKEN", tok or "not set", "from @BotFather", TEXT if tok else WARN)
        row(7, "\u251c CHAT ID", chat or "not set", "channel, group or person", TEXT if chat else WARN)
        row(8, "\u251c SIGNAL CHART", "ON" if tgs["chart_sig"] else "OFF", "chart image attached to every SIGNAL",
            OK if tgs["chart_sig"] else DIM)
        row(9, "\u2514 RESULT CHART", "ON" if tgs["chart_res"] else "OFF", "chart image attached to every RESULT",
            OK if tgs["chart_res"] else DIM)
    top = 9 if tgs["on"] else 5
    box.rule("CONTROLS")
    for items in (
        [f"{chip('Enter')} {DIM}start the live signal engine{R}", f"{chip(f'1-{top}')} {DIM}edit a setting{R}"],
        [f"{chip('r')} {DIM}reset to defaults{R}", f"{chip('q')} {DIM}cancel{R}",
         f"{ACCENT}blue{R}{DIM} = changed from default{R}"],
        [f"{DIM}Quick edit, number + value:{R}", f"{chip('1 500')}", f"{chip('2 @name')}", f"{chip('3 +5:30')}",
         f"{chip('4 2')}", f"{chip('5 on')}", f"{chip('8 off')}"],
    ):
        for ln in wrap_plain(items, box.inner, "   "):
            box.row(ln)
    put()
    box.show()
    if edit:
        ebox = Box(f"EDIT  \u00b7  {edit['label']}")
        for ln in edit["help"]:
            ebox.row(ln)
        enter = f"{chip('Enter')} {DIM}{edit['enter']}{R}   " if edit.get("enter") else ""
        ebox.row(f"{enter}{chip('q')} {DIM}back{R}")
        put()
        ebox.show()


async def _edit_field(picked, cfg, tgs, num, arg):
    """Change one of settings 1-4. `arg` is the quick-edit value (may be empty). Returns a notice or None."""
    f = SETUP_FIELDS[num]
    key, notice, text, ask = f["key"], None, arg, not arg
    while True:
        if ask:
            draw_setup(picked, cfg, tgs, {"label": f["label"], "help": f["help"],
                                          "enter": f"keeps {f['show'](cfg[key])}"})
            if notice:
                warn(notice)
            text = await ainput(ask_prompt(f"{f['label'].title()} [{f['show'](cfg[key])}]"))
            if text.lower() in BACK_WORDS:
                return None
        val = f["parse"](text, cfg[key])
        if val is not None:
            cfg[key] = val
            return None
        notice, ask = f["err"], True


async def _ask_tg_token(picked, cfg, tgs, keep, draw=None):
    """-> (token, bot username, notice) or None when cancelled. `keep` is the token Enter keeps (or None)."""
    notice = None
    help_ = [f"{DIM}Create a bot with @BotFather and paste its token (looks like 123456789:ABCdef...).{R}",
             f"{DIM}Add the bot to your channel / group (a channel needs it as administrator that may post).{R}"]
    draw = draw or draw_setup
    while True:
        draw(picked, cfg, tgs, {"label": "TELEGRAM BOT TOKEN", "help": help_,
                                      "enter": f"keeps {tg_mask(keep)}" if keep else None})
        if notice:
            warn(notice)
        text = await ainput(ask_prompt("Bot token"))
        if text.lower() in BACK_WORDS:
            return None
        if not text:
            if keep:
                return keep, (TG_STATE.get("bot") if TG_STATE.get("token") == keep else tgs.get("bot")), None
            notice = "Please paste the bot token."
            continue
        token = parse_tg_token(text)
        if token is None:
            notice = "That does not look like a bot token. Expected format: 123456789:ABCdef..."
            continue
        say("Checking the token with Telegram \u2026")
        flush()
        me, err, net = await tg_probe(token, "getMe", {})
        if err and not net:
            notice = f"Telegram rejected this token: {err}"
            continue
        note = f"Could not reach Telegram to check the token ({err}); it is used unchecked." if err else None
        return token, (me or {}).get("username"), note


async def _ask_tg_chat(picked, cfg, tgs, token, keep, draw=None):
    """-> (chat id, chat title, notice) or None when cancelled. `keep` is the chat id Enter keeps (or None)."""
    notice = None
    help_ = [f"{DIM}Chat ID of the channel / group / person, e.g. {R}{chip('-1001234567890')} {DIM}or{R} "
             f"{chip('@channelname')}",
             f"{DIM}A private chat only works after you pressed Start in your bot.{R}"]
    draw = draw or draw_setup
    while True:
        draw(picked, cfg, tgs, {"label": "TELEGRAM CHAT ID", "help": help_,
                                      "enter": f"keeps {keep}" if keep else None})
        if notice:
            warn(notice)
        text = await ainput(ask_prompt("Chat ID"))
        if text.lower() in BACK_WORDS:
            return None
        if not text:
            if not keep:
                notice = "Please enter the chat id."
                continue
            chat = keep
        else:
            chat = parse_tg_chat(text)
            if chat is None:
                notice = "Not a valid chat id. Use a number like -1001234567890 or a name like @mychannel."
                continue
        say("Checking the chat with Telegram \u2026")
        flush()
        info_, err, net = await tg_probe(token, "getChat", {"chat_id": chat})
        if err and not net:
            notice = (f"Telegram cannot open this chat: {err}. Add the bot to the chat first "
                      f"(as administrator for a channel) and check the id.")
            continue
        title = (info_.get("title") or info_.get("username") or info_.get("first_name")) if info_ else None
        note = f"Could not reach Telegram to check the chat ({err}); it is used unchecked." if err else None
        return chat, title, note


async def _tg_enable(picked, cfg, tgs, draw=None):
    """Switch Telegram ON: asks token, then chat id. Stays OFF when either step is cancelled."""
    r = await _ask_tg_token(picked, cfg, tgs, TG_STATE.get("token"), draw)
    if r is None:
        return "Telegram stays OFF."
    token, bot, n1 = r
    tg_remember_token(token, bot)
    keep = TG_STATE.get("chat")
    c = await _ask_tg_chat(picked, cfg, dict(tgs, token=token, bot=bot), token, keep, draw)
    if c is None:
        return "Telegram stays OFF."
    chat, title, n2 = c
    tgs.update(on=True, token=token, bot=bot, chat=chat, title=title)
    tg_remember(tgs)
    return n2 or n1


async def _tg_edit_token(picked, cfg, tgs, draw=None, chat_num=7):
    r = await _ask_tg_token(picked, cfg, tgs, tgs["token"], draw)
    if r is None:
        return None
    token, bot, n1 = r
    if token == tgs["token"]:
        tgs["bot"] = bot or tgs.get("bot")
        return n1
    # a different bot: the chat has to be checked again with the new token
    tg_remember_token(token, bot)
    c = await _ask_tg_chat(picked, cfg, dict(tgs, token=token, bot=bot, chat=None, title=None), token, None, draw)
    if c is None:
        tgs.update(token=token, bot=bot, chat=None, title=None)
        return f"The chat ID is not set for the new bot - choose {chat_num} to set it."
    chat, title, n2 = c
    tgs.update(token=token, bot=bot, chat=chat, title=title)
    tg_remember(tgs)
    return n2 or n1


async def _tg_edit_chat(picked, cfg, tgs, draw=None):
    c = await _ask_tg_chat(picked, cfg, tgs, tgs["token"], tgs.get("chat"), draw)
    if c is None:
        return None
    chat, title, n = c
    tgs.update(chat=chat, title=title)
    tg_remember(tgs)
    return n


async def _tg_toggle(picked, cfg, tgs, arg, draw=None, num=5):
    want = (not tgs["on"]) if not arg else parse_on_off(arg, None)
    if want is None:
        return f"Telegram: type on or off (or just {num} to switch it)."
    if want == tgs["on"]:
        return f"Telegram is already {'ON' if want else 'OFF'}."
    if not want:
        tgs["on"] = False
        return None
    return await _tg_enable(picked, cfg, tgs, draw)


def _chart_toggle(tgs, key, label, num, arg):
    want = (not tgs[key]) if not arg else parse_on_off(arg, None)
    if want is None:
        return f"{label}: type on or off (or just {num} to switch it)."
    tgs[key] = want
    return None


def setup_problem(tgs):
    if tgs["on"] and not (tgs.get("token") and tgs.get("chat")):
        return "Telegram is ON but the bot token or chat ID is missing - set 6 and 7, or switch it off with 5."
    return None


def setup_result(cfg, tgs):
    out = dict(cfg)
    out["tg"] = ({"token": tgs["token"], "chat": tgs["chat"], "bot": tgs.get("bot"), "title": tgs.get("title"),
                  "chart_sig": bool(tgs["chart_sig"]), "chart_res": bool(tgs["chart_res"])}
                 if tgs["on"] else None)
    return out


_SETUP_CMD = re.compile(r"([0-9]{1,2})(?:\s+(.+))?")


async def ask_signal_setup(picked):
    """LIVE SIGNAL settings as one list. Returns the settings dict, or None when cancelled with q."""
    cfg, tgs, notice, lib_ack = dict(SETUP_DEFAULTS), new_tg_state(), None, False
    while True:
        draw_setup(picked, cfg, tgs)
        if notice:
            warn(notice)
            notice = None
        text = (await ainput(ask_prompt("Select [Enter = start]"))).strip()
        low = text.lower()
        if low in BACK_WORDS:
            return None
        if low in ("", "s", "start", "go"):
            notice = setup_problem(tgs)
            if notice is None and tgs["on"] and not lib_ack:
                mods, err = await asyncio.get_running_loop().run_in_executor(None, charts_backend)
                if mods is None:                      # the pictures need Chromium
                    lib_ack = True
                    notice = ("Chart images need Chromium (Termux: pkg install chromium)  "
                              f"({clip(err or '', 60)}). Press Enter again to start with text only.")
                    continue
            if notice is None:
                return setup_result(cfg, tgs)
            continue
        lib_ack = False
        if low in ("r", "reset"):
            cfg, tgs, notice = dict(SETUP_DEFAULTS), new_tg_state(), "All settings are back to their defaults."
            continue
        top = 9 if tgs["on"] else 5
        m = _SETUP_CMD.fullmatch(text)
        if not m or not 1 <= int(m.group(1)) <= top:
            notice = f"Type a number from 1 to {top}, Enter to start, r to reset or q to cancel."
            continue
        num, arg = int(m.group(1)), (m.group(2) or "").strip()
        if num <= 4:
            notice = await _edit_field(picked, cfg, tgs, num, arg)
        elif num == 5:
            notice = await _tg_toggle(picked, cfg, tgs, arg)
        elif num == 6:
            notice = await _tg_edit_token(picked, cfg, tgs)
        elif num == 7:
            notice = await _tg_edit_chat(picked, cfg, tgs)
        elif num == 8:
            notice = _chart_toggle(tgs, "chart_sig", "Signal chart", 8, arg)
        else:
            notice = _chart_toggle(tgs, "chart_res", "Result chart", 9, arg)


async def live_signal_flow(ws, big):
    picked = await pick_markets(ws, "LIVE SIGNAL", "multi-market next-candle signals", big)
    if not picked:
        return
    cfg = await ask_signal_setup(picked)
    if not cfg:
        return
    notifier = None
    if cfg.get("tg"):
        notifier = TelegramNotifier(cfg["tg"]["token"], cfg["tg"]["chat"])
    session = LiveSignalSession(ws, cfg, notifier=notifier)
    if notifier is not None:
        notifier.on_error = lambda why: session.note(f"Telegram: {why}")
        notifier.start()
        async with Progress("Preparing the chart engine"):             # imports numpy / pandas / matplotlib once
            mods, err = await asyncio.get_running_loop().run_in_executor(None, charts_backend)
        session.charts_ok, session.charts_err = mods is not None, err
        await session.send_start(picked)               # BE READY banner + overview picture, right after Enter
    stop = asyncio.Event()
    started = False
    try:
        try:
            try:
                ok = await session.load(picked)
            except BaseException:
                ws.tick_cb = None
                raise
            if not ok:
                ws.tick_cb = None
                rows = [f"{it['symbol']}: {why}" for it, why in session.skipped]
                raise QuotexError("No market could be loaded for the live signal engine.\n" + "\n".join(rows))
            begin_screen()
            put()
            session.print_header()
            started = True
            await session.run(stop)
            put()
            session.finish()                          # Ctrl+S / S: shown here and sent to Telegram
        except (KeyboardInterrupt, asyncio.CancelledError):
            ws.tick_cb = None
            if started:                               # Ctrl+C: the session result is shown, not sent
                put()
                session.finish(send=False)
            raise
        if notifier is not None:
            await notifier.close()                    # deliver the last messages (incl. the session result)
    finally:
        if notifier is not None and notifier.queue is not None:
            await notifier.close(timeout=5.0)         # error / Ctrl+C: still deliver what is queued
    await pause()


# =====================================================================
#  module: darkmatter/trading/settings.py  -  AUTO TRADING: defaults and limits.
# =====================================================================
ACCOUNT_DEFAULT = "demo"            # "demo" | "live"   (the safe one is the default)
MTG_DEFAULT_AUTO = 1                # MTG steps, same default as the live signal
AMOUNT_DEFAULT, AMOUNT_MIN, AMOUNT_MAX = 1.0, 1.0, 100000.0     # main trade amount ($)
MULT_DEFAULT, MULT_MIN, MULT_MAX = 2.0, 1.0, 10.0               # amount factor per MTG step
SL_MIN = TP_MIN = 1.0               # smallest stop loss / take profit ($); None = off (default)

LOSS_MULTS = (2, 3, 4)              # "multiplier after loss": the stake after every loss = previous stake x this
LOSS_MULT_ON = 2                    # what switching it on selects (default is OFF = 0)

PERIOD = 60                         # M1 candles / one-minute trades
ORDER_OPTION_TYPE = 1               # TIME mode (3 = fast option, 100 = TIMER)
ENTRY_LEAD = 0.0                    # the order is sent when the candle opens (the send itself takes a few ms)
ARM_BEFORE = 3.0                    # seconds before the entry: make the asset the active asset of the session
ENTRY_MAX_LAG = 3.0                 # the main trade is skipped when it would start more than this late
MTG_MAX_LAG = 15.0                  # an MTG trade is skipped when it would start more than this late
OPEN_TIMEOUT = 8.0                  # seconds to wait for Quotex to confirm an order
BALANCE_SETTLE = 2.0                # a server balance frame counts only when it came this long after our last order event
RESULT_WAIT = 20.0                  # seconds after expiry to wait for the closed deal before reading the chart


def money(v, sign=False):
    """12.5 -> $12.50, -3 -> -$3.00 (sign=True also writes + for gains)."""
    if v is None:
        return "-"
    s = f"${abs(v):,.2f}"
    if v < 0:
        return "-" + s
    return ("+" + s) if sign and v > 0 else s


def amount_for(base, mult, step):
    """Stake of MTG step `step` (0 = main trade): 1$ x2 -> 1, 2, 4, 8."""
    return round(float(base) * float(mult) ** int(step), 2)


# =====================================================================
#  module: darkmatter/trading/broker.py  -  AUTO TRADING: places orders on Quotex and reports their results.
# =====================================================================
LOG_FILE = "quotex_trade_debug.log"      # written only when an order fails: what was sent, what came back


# ---------------------------------------------------------------------
#  Small parsing helpers
# ---------------------------------------------------------------------
def _num(v, default=0.0):
    try:
        x = float(v)
        return x if x == x else default
    except (TypeError, ValueError):
        return default


def iter_orders(data):
    """Order dicts inside an order event payload: a single order, a list of orders or {"deals": [...]}."""
    if isinstance(data, list):
        for x in data:
            if isinstance(x, dict) and x.get("id") is not None:
                yield x
    elif isinstance(data, dict):
        deals = data.get("deals")
        if isinstance(deals, list):
            for x in deals:
                if isinstance(x, dict) and x.get("id") is not None:
                    yield x
        elif data.get("id") is not None:
            yield data


def parse_result(order, amount):
    """Closed deal -> {"status": win | loss | draw, "profit": money made (+) or lost (-), prices ...}.

    Quotex reports the profit of a won deal; a lost deal has profit 0 (or negative) and a refunded one closes
    at its open price. `amount` is the stake we sent for it."""
    profit = _num(order.get("profit"))
    o, c = _num(order.get("openPrice")), _num(order.get("closePrice"))
    if o > 0 and c > 0 and o == c:
        status, money = "draw", 0.0
    elif profit > 0:
        status, money = "win", profit
    else:
        status, money = "loss", (profit if profit < 0 else -float(amount))
    return {"status": status, "profit": money, "open": o or None, "close": c or None, "raw": order}


# ---------------------------------------------------------------------
#  Broker
# ---------------------------------------------------------------------
class Broker:
    """One broker object per auto-trading session. At most one order is waiting for its confirmation at a time
    (the engine trades one signal at a time), any number can wait for their result."""

    CACHE_MAX = 60

    def __init__(self, ws, demo=True):
        self.ws = ws
        self.demo = bool(demo)
        self.pending = None            # {"req", "asset", "amount", "fut"}: the order waiting for its confirmation
        self.waiting = {}              # order id (str) -> {"fut", "amount"}
        self.results = {}              # order id (str) -> raw closed deal that arrived before anybody asked
        self.last_error = None
        self._attached = False
        # balance ledger: base (what the balance would be with no order of this session) + realised profit
        # - the stake of the open order. Server balance frames re-base it whenever they are trustworthy.
        self._base = None
        self._pnl = 0.0
        self._open = 0.0
        self._event_ts = 0.0
        self._frame_ts = 0.0

    # ----- life cycle -----
    def attach(self):
        self.ws.event_hook = self.on_event
        self._attached = True

    def detach(self):
        value = self.balance() if self._base is not None else None
        b = getattr(self.ws, "balance", None)
        if value is not None and isinstance(b, dict):        # later screens show the up-to-date balance
            b["demo" if self.demo else "live"] = value
            self.ws.balance_ts = time.time()
        if getattr(self.ws, "event_hook", None) == self.on_event:
            self.ws.event_hook = None
        self._attached = False
        for entry in list(self.waiting.values()):
            if not entry["fut"].done():
                entry["fut"].cancel()
        self.waiting.clear()
        if self.pending and not self.pending["fut"].done():
            self.pending["fut"].cancel()
        self.pending = None

    # ----- account -----
    async def select_account(self, demo):
        self.demo = bool(demo)
        self._base, self._pnl, self._open = None, 0.0, 0.0          # a new account starts a new ledger
        self._event_ts = self._frame_ts = 0.0
        await self.ws.change_account(self.demo)

    def _frame_value(self):
        """(balance of the selected account, time it arrived) from the latest server balance frame."""
        b = getattr(self.ws, "balance", None)
        if not isinstance(b, dict):
            return None, 0.0
        v = b.get("demo" if self.demo else "live")
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            return None, 0.0
        return float(v), float(getattr(self.ws, "balance_ts", 0.0) or 0.0)

    def balance(self):
        """The CURRENT balance of the selected account (like the Quotex page: an open order's stake is already
        taken off), or None when nothing is known yet.

        Quotex does not push a balance frame after every result, so the balance is kept as a ledger of this
        session's orders and corrected by every server balance frame that arrived after things settled."""
        value, ts = self._frame_value()
        if value is not None and ts > self._frame_ts and ts > self._event_ts + BALANCE_SETTLE:
            self._base = value + self._open - self._pnl
            self._frame_ts = ts
        if self._base is None:
            return value
        return round(self._base + self._pnl - self._open, 2)

    def opened(self, amount):
        """An order was accepted: its stake leaves the balance."""
        self.balance()                       # take a pending server frame in first
        self._open = float(amount)
        self._event_ts = time.time()

    def closed(self, profit):
        """An order finished with `profit` (+ won, - lost, 0 refunded)."""
        self.balance()
        self._pnl += float(profit)
        self._open = 0.0
        self._event_ts = time.time()

    def forget_open(self):
        """An order was lost track of (no result): drop its stake, the next server frame corrects the balance."""
        self.balance()
        self._open = 0.0
        self._event_ts = time.time()

    # ----- events (called from the socket reader, must never raise) -----
    def on_event(self, name, data):
        n = str(name).lower()
        is_open_evt = "orders/open" in n or n.endswith("orders/open")
        is_close_evt = "close" in n and "order" in n
        pend = self.pending
        if pend is not None and not pend["fut"].done():
            self._trace(pend, n, data)

        # server refused the order (whatever the event is called)
        if (pend is not None and isinstance(data, dict) and data.get("error")
                and any(k in n for k in ("order", "error", "trade"))):
            if not pend["fut"].done():
                pend["fut"].set_exception(QuotexError(f"Quotex refused the order: {data.get('error')}"))
            return

        if is_open_evt and not is_close_evt and isinstance(data, dict) and data.get("id") is not None:
            if pend is not None and not pend["fut"].done():
                rid = data.get("requestId")
                asset = data.get("asset")
                same_req = rid is None or str(rid) == str(pend["req"])
                same_asset = asset is None or str(asset) == str(pend["asset"])
                if same_req and same_asset:
                    pend["fut"].set_result(data)
            # an order that is already closed when it is reported (very short trades) falls through below

        for order in iter_orders(data):
            closed = (is_close_evt or isinstance(data, dict) and isinstance(data.get("deals"), list)
                      or str(order.get("status", "")).lower() == "closed")
            if not closed:
                continue
            oid = str(order.get("id"))
            entry = self.waiting.get(oid)
            if entry is not None:
                if not entry["fut"].done():
                    entry["fut"].set_result(order)
            else:
                self.results[oid] = order
                while len(self.results) > self.CACHE_MAX:
                    self.results.pop(next(iter(self.results)))

    # ----- diagnostics -----
    @staticmethod
    def _trace(entry, name, data):
        """Remember the first frame of every kind that arrives while an order waits for its confirmation."""
        tr = entry["trace"]
        if len(tr) >= 25 or name in entry["seen"]:
            return
        entry["seen"].add(name)
        if any(k in name for k in ("auth", "session", "token")):
            txt = "(hidden)"
        else:
            try:
                txt = json.dumps(data, default=str)
            except (TypeError, ValueError):
                txt = repr(data)
        tr.append((name, txt[:240]))

    @staticmethod
    def _log_failure(entry, sent, reason):
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"=== {time.strftime('%Y-%m-%d %H:%M:%S')}  {reason}\n  sent: {json.dumps(sent)}\n")
                for name, txt in entry["trace"]:
                    f.write(f"  recv {name}: {txt}\n")
        except OSError:
            pass

    # ----- placing an order -----
    @staticmethod
    def _settings_payload(asset, amount, expiry):
        """Chart state for TIME mode: the trade ends at the clock time `expiry` (isFastOption False)."""
        return {
            "chartId": "graph",
            "settings": {
                "chartId": "graph", "chartType": 2, "currentExpirationTime": int(expiry), "isFastOption": False,
                "isFastAmountOption": False, "isIndicatorsMinimized": False, "isIndicatorsShowing": True,
                "isShortBetElement": False, "chartPeriod": 4, "currentAsset": {"symbol": asset},
                "dealValue": amount, "dealPercentValue": 1, "isVisible": True, "timePeriod": PERIOD,
                "gridOpacity": 8, "isAutoScrolling": 1, "isOneClickTrade": True,
                "upColor": "#0FAF59", "downColor": "#FF6251",
            },
            "endTime": int(expiry),
        }

    async def arm(self, asset):
        """Make `asset` the active asset of this session (pyquotex does this in buy() before every order).
        A market that is not the one open in the Chrome tab is only followed, not active, until this is sent."""
        await self.ws.subscribe(asset, PERIOD)

    async def place(self, asset, direction, amount, expiry, timeout=8.0):
        """Open one binary order that expires at the absolute time `expiry` (unix seconds, a whole minute).
        direction: +1 CALL / -1 PUT.  Returns the confirmation dict (it holds the order id).
        Raises QuotexError when the order is refused, not confirmed in time, or the socket is not usable."""
        if not self._attached:
            raise QuotexError("The broker is not attached to the connection.")
        if self.pending is not None and not self.pending["fut"].done():
            raise QuotexError("Another order is still waiting for its confirmation.")
        amount = round(float(amount), 2)
        if amount <= 0:
            raise QuotexError(f"Invalid trade amount: {amount}")
        req = int(time.time())
        fut = asyncio.get_running_loop().create_future()
        entry = {"req": req, "asset": asset, "amount": amount, "fut": fut, "trace": [], "seen": set()}
        self.pending = entry
        payload = {
            "asset": asset, "amount": amount, "time": int(expiry), "action": "call" if direction > 0 else "put",
            "isDemo": 1 if self.demo else 0, "tournamentId": 0, "requestId": req,
            "optionType": ORDER_OPTION_TYPE,
        }
        try:
            await self.arm(asset)              # same order as pyquotex: follow asset -> settings -> tick -> order
            await self.ws._emit("settings/apply", self._settings_payload(asset, amount, expiry))
            await self.ws._emit("tick")
            await self.ws._emit("orders/open", payload)
            try:
                data = await asyncio.wait_for(fut, timeout)
            except asyncio.TimeoutError:
                seen = ", ".join(n for n, _ in entry["trace"]) or "none"
                raise QuotexError(
                    f"No confirmation from Quotex within {timeout:.0f}s - the order may not have been placed. "
                    f"Frames received meanwhile: {seen} (details in {LOG_FILE}).") from None
        except QuotexError as e:
            self._log_failure(entry, payload, str(e))
            raise
        finally:
            if self.pending is not None and self.pending["fut"] is fut:
                self.pending = None
        if not isinstance(data, dict) or data.get("id") is None:
            raise QuotexError("Quotex confirmed the order without an order id.")
        self.opened(amount)
        return data

    async def wait_result(self, order_id, amount, timeout):
        """Wait (up to `timeout` s) for the closed deal of `order_id`. -> parse_result dict, or None on timeout."""
        oid = str(order_id)
        cached = self.results.pop(oid, None)
        if cached is not None:
            return parse_result(cached, amount)
        fut = asyncio.get_running_loop().create_future()
        self.waiting[oid] = {"fut": fut, "amount": amount}
        try:
            order = await asyncio.wait_for(fut, max(0.5, timeout))
            return parse_result(order, amount)
        except asyncio.TimeoutError:
            return None
        finally:
            self.waiting.pop(oid, None)


# =====================================================================
#  module: darkmatter/trading/session.py  -  AUTO TRADING: the live signal session that also places the trades.
# =====================================================================
class AutoTradeSession(LiveSignalSession):
    HEADER_TITLE = "AUTO TRADING ENGINE"

    def __init__(self, ws, cfg, broker, time_fn=None, notifier=None):
        super().__init__(ws, cfg, time_fn=time_fn or time.time, notifier=notifier)
        self.broker = broker
        self.period = PERIOD            # candle length in seconds (a test can shorten it)
        self.pnl = 0.0                  # session profit / loss in $
        self.orders = []                # every order: {symbol, step, amount, status, profit}
        self.start_balance = broker.balance()
        self.stake_next = float(cfg["amount"])      # loss multiplier ON: the stake of the next trade
        self.stop_reason = None
        self._trade_task = None
        self._stop_evt = None

    # ---------------- money ----------------
    def amount_for(self, step):
        return amount_for(self.cfg["amount"], self.cfg["mult"], step)

    # "multiplier after loss": ONE chain over all trades. Every loss makes the next trade (MTG step or the next
    # signal) previous stake x factor, a profit brings it back to the base amount, a draw keeps the stake.
    @property
    def loss_factor(self):
        return int(self.cfg.get("loss_mult") or 0)

    def _first_stake(self):
        """Stake of the main trade of a new signal."""
        return self.stake_next if self.loss_factor else self.amount_for(0)

    def _step_stake(self, step):
        """Stake of MTG step `step` (the trade after a loss inside one signal)."""
        return self.stake_next if self.loss_factor else self.amount_for(step)

    def _update_stake(self, status, amount):
        if not self.loss_factor:
            return
        if status == "win":
            self.stake_next = float(self.cfg["amount"])
        elif status == "loss":
            self.stake_next = round(float(amount) * self.loss_factor, 2)
        else:                                   # draw: the stake was refunded, trade the same amount again
            self.stake_next = float(amount)

    @property
    def acct(self):
        return "DEMO" if self.broker.demo else "LIVE"

    def _limit_msg(self):
        sl, tp = self.cfg.get("sl"), self.cfg.get("tp")
        if sl and self.pnl <= -sl + 1e-9:
            return f"STOP LOSS reached: session P/L {money(self.pnl, True)} (limit -{money(sl)})."
        if tp and self.pnl >= tp - 1e-9:
            return f"TAKE PROFIT reached: session P/L {money(self.pnl, True)} (target {money(tp)})."
        return None

    def _stopping(self):
        return self._stop_evt is not None and self._stop_evt.is_set()

    def _request_stop(self, why):
        if self.stop_reason is None:
            self.stop_reason = why
        self.note(why + " The session stops.", "info" if "reached" in why else "warn")
        if self._stop_evt is not None:
            self._stop_evt.set()

    # ---------------- signal -> trade ----------------
    def _issue(self, best, entry_ts, market):
        super()._issue(best, entry_ts, market)         # prints / sends the signal and sets self.trade
        t = self.trade
        if t is None:
            return
        t["amount"] = self._first_stake()
        self._trade_task = asyncio.create_task(self._run_trade(t))

    def _resolve(self, now):
        """The live session judges candles here; in AUTO TRADING the result comes from Quotex (see _cycle)."""
        return

    async def _wait_until(self, ts):
        while True:
            left = ts - self.now()
            if left <= 0:
                return
            await asyncio.sleep(min(0.05, max(0.005, left)))

    async def _run_trade(self, t):
        try:
            await self._cycle(t)
        except asyncio.CancelledError:
            if self.trade is t:
                self.trade = None
            raise
        except Exception as e:                          # whatever happens, the engine must be released
            self.note(f"Auto trading error: {type(e).__name__}: {e}")
            if self.trade is t:
                self.trade = None

    async def _cycle(self, t):
        sym, name = t["symbol"], sig_symbol(t["symbol"])
        while True:
            step, amount = t["step"], t["amount"]
            start = t["entry_ts"] + self.period * step
            expiry = start + self.period
            if step == 0:
                # make this market the active one of the session a moment before the entry (a market that is not
                # the one open in the Chrome tab would otherwise not be tradable)
                await self._wait_until(start - ARM_BEFORE)
                try:
                    await self.broker.arm(sym)
                except Exception as e:
                    self.note(f"{name}: could not activate the market ({type(e).__name__}: {e}).", "info")
            await self._wait_until(start - ENTRY_LEAD)
            if step == 0 and self._stopping():
                self.note(f"{name}: stop requested before the entry - no trade.", "info")
                return self._end(t, None)
            lag = self.now() - start
            if lag > (ENTRY_MAX_LAG if step == 0 else MTG_MAX_LAG):
                self.note(f"{name}: too late to enter ({lag:.1f}s after the candle opened) - "
                          f"{'trade' if step == 0 else 'MTG step'} skipped.")
                return self._end(t, None)
            bal = self.broker.balance()
            if bal is not None and amount > bal + 1e-9:
                self.note(f"{name}: balance {money(bal)} is not enough for {money(amount)}.")
                if step == 0:
                    self._request_stop("Balance is too low for the next trade amount.")
                return self._end(t, None)
            try:
                order = await self.broker.place(sym, t["dir"], amount, expiry, OPEN_TIMEOUT)
            except QuotexError as e:
                self.note(f"{name}: order NOT placed - {e}")
                return self._end(t, None)
            oid = order.get("id")
            t["order_id"] = oid
            self._show_order(t, oid, amount, step, expiry)
            ct = order.get("closeTimestamp")
            if isinstance(ct, (int, float)) and not isinstance(ct, bool) and abs(ct - expiry) > 1.5:
                self.note(f"{name}: Quotex set the expiry to {_hhmm(ct, self.tz)}:{int(ct) % 60:02d} instead of "
                          f"{_hhmm(expiry, self.tz)}:00 - this trade will NOT end with the candle.")
            res = await self.broker.wait_result(oid, amount, expiry + RESULT_WAIT - self.now())
            if res is None:
                res = self._chart_result(t, start, amount)
                if res is None:
                    self.broker.forget_open()
                    self.note(f"{name}: no result for order {oid} - check it on the Quotex page.")
                    return self._end(t, None)
                self.note(f"{name}: Quotex sent no result in time - read from the chart (profit estimated).")
            self._book(t, res, amount, step)
            limit = self._limit_msg()
            if limit:
                self._request_stop(limit)
            if res["status"] == "draw":
                self.note(f"{name}: draw - the stake of {money(amount)} is refunded, signal ignored.", "info")
                return self._end(t, None)
            if res["status"] == "win":
                return self._end(t, True, res)
            if step < self.cfg["mtg"] and not self._stopping():
                t["step"] += 1
                t["amount"] = self._step_stake(t["step"])
                self.emit_lines([f"{WARN_B}\U0001F525MTG STEP {t['step']} - next trade "
                                 f"{money(t['amount'])} on the next candle \U0001F525{R}"])
                continue
            return self._end(t, False, res)

    def _chart_result(self, t, start, amount):
        """Fallback when Quotex sent no closed deal: judge the entry candle from our own chart."""
        c = self.series[t["symbol"]].candle_at(start)
        if c is None or self.now() < start + self.period:
            return None
        d = t["dec"]
        o, cl = round(c["o"], d), round(c["c"], d)
        if o == cl:
            return {"status": "draw", "profit": 0.0, "open": o, "close": cl, "raw": {}, "estimated": True}
        win = (cl > o) if t["dir"] > 0 else (cl < o)
        pay = t.get("payout")
        gain = round(amount * (pay if pay else 80.0) / 100.0, 2)
        return {"status": "win" if win else "loss", "profit": gain if win else -amount, "open": o, "close": cl,
                "raw": {}, "estimated": True}

    def _book(self, t, res, amount, step):
        self.pnl += res["profit"]
        self.broker.closed(res["profit"])
        self._update_stake(res["status"], amount)
        self.orders.append({"symbol": t["symbol"], "step": step, "amount": amount, "status": res["status"],
                            "profit": res["profit"]})
        word, col = {"win": ("WIN", OK_B), "loss": ("LOSS", BAD_B), "draw": ("DRAW", WARN_B)}[res["status"]]
        pc = OK_B if self.pnl > 0 else BAD_B if self.pnl < 0 else TEXT
        self.emit_lines([f"{col}\u25a0 {word}{R} {TEXT}{sig_symbol(t['symbol'])}{R} {col}{money(res['profit'], True)}{R}"
                         f" {DIM}\u00b7 {'main trade' if step == 0 else 'MTG %d' % step}{R}"
                         f" {DIM}\u00b7 session P/L{R} {pc}{money(self.pnl, True)}{R}"
                         f" {DIM}\u00b7 balance{R} {TEXT}{money(self.broker.balance())}{R}"], blank=False)

    def _show_order(self, t, oid, amount, step, expiry):
        side = "CALL" if t["dir"] > 0 else "PUT"
        what = "main trade" if step == 0 else f"MTG {step}"
        at = _hhmm(t["entry_ts"] + self.period * step, self.tz)
        bal = self.broker.balance()
        self.emit_lines([f"{ACCENT}\u25b6 ORDER{R} {TITLE}{sig_symbol(t['symbol'])} {side}{R} {TEXT}{money(amount)}{R}"
                         f" {DIM}\u00b7 {what} \u00b7 {self.acct} \u00b7 TIME {at} \u2192 {_hhmm(expiry, self.tz)}"
                         f" \u00b7 balance {money(bal)} \u00b7 id {str(oid)[:8]}{R}"])

    def _end(self, t, win, res=None):
        """Close the cycle. win None = no counted result (no trade, refused, draw, no result)."""
        try:
            if win is not None:
                self._publish_result(t, win, res)
        finally:
            self.trade = None
        if not self._stopping():
            self.emit_lines([f"{WARN}\u25cc{R} {DIM}Scanning next signal \u2026{R}"], blank=False)

    def _publish_result(self, t, win, res):
        d = t["dec"]
        start = t["entry_ts"] + self.period * t["step"]
        c = self.series[t["symbol"]].candle_at(start)
        o = (res or {}).get("open") or (round(c["o"], d) if c else t.get("price") or 0.0)
        cl = (res or {}).get("close") or (round(c["c"], d) if c else t.get("price") or 0.0)
        self.stats.add(t["symbol"], t["dir"], t["entry_ts"], win, t["step"], t.get("payout"))
        r = {"symbol": t["symbol"], "entry_ts": t["entry_ts"], "dir": t["dir"], "win": win, "step": t["step"],
             "open": o, "close": cl, "dec": d, "wins": self.stats.wins, "losses": self.stats.losses,
             "pair": self.stats.pair(t["symbol"])}
        render = self._result_render(t, r, c) if c else None
        self.telegram(tg_result_messages, r, self.cfg, self.tz, render)
        self.emit_lines(render_result_terminal(r, self.cfg, self.tz))

    # ---------------- screen ----------------
    def print_header(self):
        super().print_header()
        sl, tp = self.cfg.get("sl"), self.cfg.get("tp")
        box = Box("AUTO TRADING")
        acct_col = WARN if self.broker.demo is False else OK
        mult = ("x%d after loss" % self.loss_factor) if self.loss_factor else "x%g" % self.cfg["mult"]
        box.row(f"{kv('ACCOUNT', acct_col + self.acct + R, 8)}   {kv('BALANCE', money(self.broker.balance()), 8)}   "
                f"{kv('AMOUNT', money(self.cfg['amount']), 7)}   {kv('MULTIPLIER', mult, 11)}   "
                f"{kv('STOP LOSS', money(sl) if sl else 'OFF', 10)}   {kv('TAKE PROFIT', money(tp) if tp else 'OFF', 12)}")
        if self.loss_factor:
            ladder = "  \u2192  ".join(money(self.cfg["amount"] * self.loss_factor ** k) for k in range(4)) + " \u2026"
            note = f"every loss: previous stake x{self.loss_factor}; a profit goes back to {money(self.cfg['amount'])}"
        else:
            ladder = "  \u2192  ".join(money(self.amount_for(k)) for k in range(self.cfg["mtg"] + 1))
            note = f"main trade \u2192 MTG steps; every new signal starts again at {money(self.cfg['amount'])}"
        box.row(f"{kv('STAKES', ladder, 8)}   {DIM}({note}){R}")
        put()
        box.show()

    def _status_text(self, now):
        w, l = self.stats.wins, self.stats.losses
        t = self.trade
        spin = "|/-\\"[int(time.monotonic() * 4) % 4]
        if t is not None:
            left = max(0, int(t["entry_ts"] + self.period * (t["step"] + 1) - now))
            side = "CALL" if t["dir"] > 0 else "PUT"
            what = "entry" if t["step"] == 0 else f"MTG {t['step']}"
            mid = (f"{ACCENT}\u23f3 {what}{R} {TITLE}{sig_symbol(t['symbol'])} {side} {money(t.get('amount'))}{R} "
                   f"{DIM}closes in {left}s{R}")
        else:
            wait = (SIGNAL_SECOND - now % 60) % 60
            mid = f"{WARN}{spin}{R} {TEXT}Scanning{R} {DIM}{len(self.series)} markets \u00b7 next check {int(wait)}s{R}"
        pc = OK_B if self.pnl > 0 else BAD_B if self.pnl < 0 else TEXT
        nxt = f"  {DIM}\u2502{R}  {DIM}next stake{R} {TEXT}{money(self.stake_next)}{R}" if self.loss_factor else ""
        return (f"  {mid}  {DIM}\u2502{R}  {DIM}W{R} {OK_B}{w}{R} {DIM}L{R} {BAD_B}{l}{R} "
                f"{DIM}({_pct(w, l):.0f}%){R}  {DIM}\u2502{R}  {DIM}P/L{R} {pc}{money(self.pnl, True)}{R}"
                f"{nxt}  {DIM}\u2502{R}  {DIM}{self.acct}{R} {TEXT}{money(self.broker.balance())}{R}"
                f"  {DIM}\u2502  Ctrl+S = stop{R}")

    # ---------------- run / stop ----------------
    async def run(self, stop):
        self._stop_evt = stop
        try:
            await super().run(stop)
        except BaseException:
            await self._cancel_trade()
            raise
        await self._drain_trade()

    async def _cancel_trade(self):
        task = self._trade_task
        if task is not None and not task.done():
            if self.trade is not None:
                self.note("An order may still be open on Quotex - check it on the page.")
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _drain_trade(self):
        """Stop was requested: let the trade that is already open finish (no further MTG step)."""
        task = self._trade_task
        if task is None or task.done():
            return
        self.note("Stopping - waiting for the open trade to finish (no further MTG steps) \u2026", "info")
        try:
            await asyncio.wait_for(asyncio.shield(task), 2 * self.period + RESULT_WAIT + 10)
        except asyncio.TimeoutError:
            self.note("The open trade did not finish in time - check it on the Quotex page.")
            await self._cancel_trade()
        except (KeyboardInterrupt, asyncio.CancelledError):
            await self._cancel_trade()
            raise

    def trade_summary(self):
        bal = self.broker.balance()
        box = Box("AUTO TRADING RESULT")
        pc = OK if self.pnl > 0 else (BAD if self.pnl < 0 else TEXT)
        n = len(self.orders)
        wins = sum(1 for o in self.orders if o["status"] == "win")
        loss = sum(1 for o in self.orders if o["status"] == "loss")
        box.row(f"{kv('ACCOUNT', self.acct, 8)}   {kv('ORDERS', '%d  (%d won / %d lost)' % (n, wins, loss), 8)}   "
                f"{kv('NET P/L', pc + money(self.pnl, True) + R, 8)}")
        box.row(f"{kv('START', money(self.start_balance), 8)}   {kv('BALANCE', money(bal), 8)}"
                + (f"   {WARN}{self.stop_reason}{R}" if self.stop_reason else ""))
        put()
        box.show()

    def finish(self, send=True):
        super().finish(send=send)
        self.trade_summary()


# =====================================================================
#  module: darkmatter/flows/auto_flow.py  -  AUTO TRADING: market pick, the settings screen and the run (the trading itself is in trading/session.py).
# =====================================================================
AUTO_DEFAULTS = {"account": ACCOUNT_DEFAULT, "mtg": MTG_DEFAULT_AUTO, "amount": AMOUNT_DEFAULT, "mult": MULT_DEFAULT,
                 "loss_mult": 0, "sl": 0.0, "tp": 0.0, "n": SIGNAL_CANDLES_DEFAULT, "owner": OWNER_DEFAULT,
                 "tz_min": TZ_DEFAULT_MIN}          # sl / tp: 0 = OFF
TG_ROW, TG_TOKEN_ROW, TG_CHAT_ROW, TG_SIG_ROW, TG_RES_ROW = 11, 12, 13, 14, 15


# ---------------------------------------------------------------------
#  Parsers: (text, current) -> new value, the current value for an empty text, None when invalid
# ---------------------------------------------------------------------
_NUM_RE = re.compile(r"[0-9]+(?:[.,][0-9]{1,2})?")


def _to_float(t):
    t = t.strip().lower().replace(" ", "")
    for ch in ("$", "x", "\u00d7"):
        t = t.replace(ch, "")
    if not t.isascii() or not _NUM_RE.fullmatch(t):
        return None
    return float(t.replace(",", "."))


def parse_money(text, default, lo, hi):
    if not text.strip():
        return default
    v = _to_float(text)
    return round(v, 2) if v is not None and lo <= v <= hi else None


def parse_limit(text, default, lo):
    """Stop loss / take profit: off / 0 -> 0.0 (OFF), a dollar amount, '' -> unchanged."""
    t = text.strip().lower()
    if not t:
        return default
    if t in ("off", "no", "none", "0", "0.0", "-"):
        return 0.0
    return parse_money(t, default, lo, AMOUNT_MAX)


def parse_loss_mult(text, default):
    """Multiplier after loss: off / 0 -> 0 (OFF), on -> 2x (or keeps the chosen one), 2 / 3 / 4 (with or without x)."""
    t = text.strip().lower().replace(" ", "")
    if not t:
        return default
    if t in ("off", "no", "none", "0", "-"):
        return 0
    if t == "on":
        return default or LOSS_MULT_ON
    t = t.strip("x\u00d7")
    if t.isascii() and t.isdigit() and int(t) in LOSS_MULTS:
        return int(t)
    return None


def parse_account(text, default):
    t = text.strip().lower()
    if not t:
        return default
    if t in ("1", "live", "l", "real"):
        return "live"
    if t in ("2", "demo", "d", "practice"):
        return "demo"
    return None


def bal_value(ws, acct):
    b = getattr(ws, "balance", None)
    v = b.get("demo" if acct == "demo" else "live") if isinstance(b, dict) else None
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def bal_text(ws, acct):
    v = bal_value(ws, acct)
    return money(v) if v is not None else "not received yet"


def ladder_amounts(cfg):
    """Stakes of one signal that loses every step: main trade, then each MTG step."""
    f = cfg.get("loss_mult") or 0
    if f:
        return [round(cfg["amount"] * f ** k, 2) for k in range(cfg["mtg"] + 1)]
    return [amount_for(cfg["amount"], cfg["mult"], k) for k in range(cfg["mtg"] + 1)]


def ladder_text(cfg):
    return "  \u2192  ".join(money(v) for v in ladder_amounts(cfg))


def _d(*lines):
    return lambda ws, cfg: list(lines)


AUTO_FIELDS = {
    1: {"key": "account", "label": "ACCOUNT", "hint": lambda c: "LIVE = real money, DEMO = practice",
        "show": lambda v: "LIVE ACCOUNT" if v == "live" else "DEMO ACCOUNT",
        "help": lambda ws, cfg: [
            f"{chip('1')} {TEXT}Live Account{R}   {DIM}balance{R} {OK}{bal_text(ws, 'live')}{R}",
            f"{chip('2')} {TEXT}Demo Account{R}   {DIM}balance{R} {OK}{bal_text(ws, 'demo')}{R}"],
        "parse": parse_account, "err": "Type 1 (Live Account) or 2 (Demo Account)."},
    2: {"key": "mtg", "label": "MTG STEPS", "hint": lambda c: f"0 = no MTG, up to {MTG_MAX}",
        "show": lambda v: "NO MTG" if v == 0 else f"STEP {v}",
        "help": _d(f"{DIM}After a lost trade the bot trades the SAME direction again on the next candle.{R}",
                   f"{chip('0')} {DIM}no MTG{R}   {chip('1')} {DIM}1 step{R}   {chip('2')} {DIM}2 steps{R}   "
                   f"{chip('3')} {DIM}3 steps{R}"),
        "parse": lambda t, cur: parse_mtg(t, cur), "err": f"Please enter 0, 1, 2 or {MTG_MAX}."},
    3: {"key": "amount", "label": "TRADE AMOUNT", "hint": lambda c: "base stake of a main trade",
        "show": lambda v: money(v),
        "help": _d(f"{DIM}The main trade amount in $. Every new signal starts again with this amount, "
                   f"whatever the MTG did.{R}",
                   f"{DIM}Range {money(AMOUNT_MIN)} - {money(AMOUNT_MAX)}  \u00b7  default {money(AMOUNT_DEFAULT)}{R}"),
        "parse": lambda t, cur: parse_money(t, cur, AMOUNT_MIN, AMOUNT_MAX),
        "err": f"Please enter an amount between {money(AMOUNT_MIN)} and {money(AMOUNT_MAX)}, e.g. 1 or 2.5."},
    4: {"key": "mult", "label": "MTG MULTIPLIER", "hint": lambda c: ("not used while the loss multiplier is on" if c["loss_mult"]
                           else "stakes: " + ladder_text(c)),
        "show": lambda v: f"x{v:g}",
        "help": _d(f"{DIM}Each MTG step trades the previous stake times this number (x2: $1 lost \u2192 next $2).{R}",
                   f"{DIM}Range x{MULT_MIN:g} - x{MULT_MAX:g}  \u00b7  default x{MULT_DEFAULT:g}{R}"),
        "parse": lambda t, cur: parse_money(t, cur, MULT_MIN, MULT_MAX),
        "err": f"Please enter a number between {MULT_MIN:g} and {MULT_MAX:g}, e.g. 2 or 2.2."},
    5: {"key": "loss_mult", "label": "LOSS MULTIPLIER", "title": "MULTIPLIER AFTER LOSS",
        "hint": lambda c: ("stakes: " + ladder_text(c) + " \u2026") if c["loss_mult"]
        else "raise the stake after every loss (2x-4x)",
        "show": lambda v: "OFF" if not v else f"{v}x",
        "help": _d(f"{DIM}ON: after every loss the next trade is the previous stake times this number, "
                   f"whether it is an MTG step or the next signal.{R}",
                   f"{DIM}After a profit the stake goes back to the base amount. While it is ON it is used instead of "
                   f"the MTG multiplier.{R}",
                   f"{chip('off')} {DIM}off (default){R}   {chip('on')} {DIM}2x{R}   {chip('2')} {DIM}2x{R}   "
                   f"{chip('3')} {DIM}3x{R}   {chip('4')} {DIM}4x{R}"),
        "parse": lambda t, cur: parse_loss_mult(t, cur),
        "err": "Type off, on (= 2x), 2, 3 or 4."},
    6: {"key": "sl", "label": "STOP LOSS", "hint": lambda c: "stop when the session loses this much",
        "show": lambda v: money(v) if v else "OFF",
        "help": _d(f"{DIM}The session stops after the order that brings the session loss to this amount.{R}",
                   f"{chip('off')} {DIM}no stop loss (default){R}   {chip('10')} {DIM}stop at -$10{R}"),
        "parse": lambda t, cur: parse_limit(t, cur, SL_MIN),
        "err": f"Type off or an amount from {money(SL_MIN)}."},
    7: {"key": "tp", "label": "TAKE PROFIT", "hint": lambda c: "stop when the session gains this much",
        "show": lambda v: money(v) if v else "OFF",
        "help": _d(f"{DIM}The session stops after the order that brings the session profit to this amount.{R}",
                   f"{chip('off')} {DIM}no take profit (default){R}   {chip('20')} {DIM}stop at +$20{R}"),
        "parse": lambda t, cur: parse_limit(t, cur, TP_MIN),
        "err": f"Type off or an amount from {money(TP_MIN)}."},
    8: {"key": "n", "label": "HISTORY CANDLES", "hint": lambda c: f"range {SIGNAL_CANDLES_MIN} - {SIGNAL_CANDLES_MAX}",
        "show": lambda v: str(v),
        "help": _d(f"{DIM}Historical 1-minute candles per market that the analysis uses.{R}",
                   f"{DIM}Range {SIGNAL_CANDLES_MIN} - {SIGNAL_CANDLES_MAX}  \u00b7  default {SIGNAL_CANDLES_DEFAULT}{R}"),
        "parse": lambda t, cur: parse_int_range(t, cur, SIGNAL_CANDLES_MIN, SIGNAL_CANDLES_MAX),
        "err": f"Please enter a whole number between {SIGNAL_CANDLES_MIN} and {SIGNAL_CANDLES_MAX}."},
    9: {"key": "owner", "label": "OWNER", "hint": lambda c: "shown at the bottom of every signal",
        "show": lambda v: v,
        "help": _d(f"{DIM}Name shown at the bottom of every signal (up to 40 characters).{R}",
                   f"{DIM}Default {OWNER_DEFAULT}{R}"),
        "parse": lambda t, cur: parse_owner(t, cur), "err": "Please enter a name."},
    10: {"key": "tz_min", "label": "TIMEZONE", "hint": lambda c: "entry times and the session date",
        "show": lambda v: tz_label(v),
        "help": _d(f"{DIM}Entry times and the session date use this timezone.{R}",
                   f"{DIM}Examples: {R}{chip('+6')} {chip('UTC+06:00')} {chip('+5:30')} {chip('-3')} {chip('0')}"),
        "parse": lambda t, cur: parse_tz(t, cur),
        "err": "Not a valid offset. Use hours (+6, -3) or hh:mm (+5:30), between -12:00 and +14:00."},
}


# ---------------------------------------------------------------------
#  The settings screen
# ---------------------------------------------------------------------
def draw_auto_setup(ws, picked, cfg, tgs, edit=None):
    begin_screen()
    put()
    print_selection(picked)
    box = Box("AUTO TRADING SETUP")
    wide = box.inner >= 90
    lw = 17
    vw = min(44, max(26, box.inner - 5 - lw - 46)) if wide else max(10, min(44, box.inner - 5 - lw))

    def row(num, label, value, hint, color):
        line = f"{chip(f'{num:>2}')} {TEXT}{pad(label, lw)}{R}{color}{pad(clip(value, vw - 1), vw)}{R}"
        if wide:
            line += f"{DIM}{hint}{R}"
        box.row(line)

    box.row(f"{DIM}{pad('#', 4)} {pad('SETTING', lw)}{pad('VALUE', vw)}{'NOTE' if wide else ''}{R}")
    box.rule()
    for num, f in AUTO_FIELDS.items():
        v = cfg[f["key"]]
        value = f["show"](v)
        if num == 1:
            value += f"  {bal_text(ws, v)}"
        color = (WARN if (num == 1 and v == "live") else
                 TEXT if v == AUTO_DEFAULTS[f["key"]] else ACCENT)
        row(num, f["label"], value, f["hint"](cfg), color)
    row(TG_ROW, "TELEGRAM", "ON" if tgs["on"] else "OFF", "send signals, results & session result",
        OK if tgs["on"] else DIM)
    if tgs["on"]:
        tok = tg_mask(tgs["token"]) + (f"  @{tgs['bot']}" if tgs.get("bot") else "") if tgs.get("token") else None
        chat = (str(tgs["chat"]) + (f"  {tgs['title']}" if tgs.get("title") else "")) if tgs.get("chat") else None
        row(TG_TOKEN_ROW, "\u251c BOT TOKEN", tok or "not set", "from @BotFather", TEXT if tok else WARN)
        row(TG_CHAT_ROW, "\u251c CHAT ID", chat or "not set", "channel, group or person", TEXT if chat else WARN)
        row(TG_SIG_ROW, "\u251c SIGNAL CHART", "ON" if tgs["chart_sig"] else "OFF", "chart image attached to every SIGNAL",
            OK if tgs["chart_sig"] else DIM)
        row(TG_RES_ROW, "\u2514 RESULT CHART", "ON" if tgs["chart_res"] else "OFF", "chart image attached to every RESULT",
            OK if tgs["chart_res"] else DIM)
    top = TG_RES_ROW if tgs["on"] else TG_ROW
    box.rule("CONTROLS")
    for items in (
        [f"{chip('Enter')} {DIM}start auto trading{R}", f"{chip(f'1-{top}')} {DIM}edit a setting{R}"],
        [f"{chip('r')} {DIM}reset to defaults{R}", f"{chip('q')} {DIM}cancel{R}",
         f"{ACCENT}blue{R}{DIM} = changed from default{R}"],
        [f"{DIM}Quick edit, number + value:{R}", f"{chip('1 live')}", f"{chip('2 2')}", f"{chip('3 5')}",
         f"{chip('4 2.5')}", f"{chip('5 on')}", f"{chip('6 20')}", f"{chip('7 off')}", f"{chip('8 500')}",
         f"{chip('11 on')}"],
    ):
        for ln in wrap_plain(items, box.inner, "   "):
            box.row(ln)
    put()
    box.show()
    if edit:
        ebox = Box(f"EDIT  \u00b7  {edit['label']}")
        for ln in edit["help"]:
            ebox.row(ln)
        enter = f"{chip('Enter')} {DIM}{edit['enter']}{R}   " if edit.get("enter") else ""
        ebox.row(f"{enter}{chip('q')} {DIM}back{R}")
        put()
        ebox.show()


def _drawer(ws):
    return lambda picked, cfg, tgs, edit=None: draw_auto_setup(ws, picked, cfg, tgs, edit)


async def _auto_edit_field(ws, picked, cfg, tgs, num, arg):
    """Change one of settings 1-9. `arg` is the quick-edit value (may be empty). Returns a notice or None."""
    f = AUTO_FIELDS[num]
    key, notice, text, ask = f["key"], None, arg, not arg
    while True:
        if ask:
            draw_auto_setup(ws, picked, cfg, tgs, {"label": f.get("title", f["label"]), "help": f["help"](ws, cfg),
                                                   "enter": f"keeps {f['show'](cfg[key])}"})
            if notice:
                warn(notice)
            text = await ainput(ask_prompt(f"{f.get('title', f['label']).title()} [{f['show'](cfg[key])}]"))
            if text.lower() in BACK_WORDS:
                return None
        val = f["parse"](text, cfg[key])
        if val is not None:
            cfg[key] = val
            return None
        notice, ask = f["err"], True


def start_problem(ws, cfg, tgs):
    """A reason why the run cannot start (shown instead of starting), or None."""
    if tgs["on"] and not (tgs.get("token") and tgs.get("chat")):
        return (f"Telegram is ON but the bot token or chat ID is missing - set {TG_TOKEN_ROW} and {TG_CHAT_ROW}, "
                f"or switch it off with {TG_ROW}.")
    bal = bal_value(ws, cfg["account"])
    if bal is not None and cfg["amount"] > bal + 1e-9:
        return (f"The {cfg['account'].upper()} balance ({money(bal)}) is lower than the trade amount "
                f"({money(cfg['amount'])}).")
    return None


def risk_notice(ws, cfg):
    """Worst case: every step of one signal is lost. Returns a warning text or None (needs a second Enter)."""
    need = sum(ladder_amounts(cfg))
    bal = bal_value(ws, cfg["account"])
    if bal is not None and need > bal + 1e-9:
        return (f"Losing every step of one signal needs {money(need)}, the balance is {money(bal)}. "
                f"Press Enter again to start anyway.")
    return None


_CMD = re.compile(r"([0-9]{1,2})(?:\s+(.+))?")


async def ask_auto_setup(ws, picked):
    """AUTO TRADING settings as one list. Returns the settings dict, or None when cancelled with q."""
    cfg, tgs, notice = dict(AUTO_DEFAULTS), new_tg_state(), None
    lib_ack = risk_ack = False
    draw = _drawer(ws)
    while True:
        draw_auto_setup(ws, picked, cfg, tgs)
        if notice:
            warn(notice)
            notice = None
        text = (await ainput(ask_prompt("Select [Enter = start]"))).strip()
        low = text.lower()
        if low in BACK_WORDS:
            return None
        if low in ("", "s", "start", "go"):
            notice = start_problem(ws, cfg, tgs)
            if notice is not None:
                continue
            if tgs["on"] and not lib_ack:
                mods, err = await asyncio.get_running_loop().run_in_executor(None, charts_backend)
                if mods is None:
                    lib_ack = True
                    notice = ("Chart images need Chromium (Termux: pkg install chromium)  "
                              f"({clip(err or '', 60)}). Press Enter again to start with text only.")
                    continue
            if not risk_ack:
                notice = risk_notice(ws, cfg)
                if notice is not None:
                    risk_ack = True
                    continue
            if cfg["account"] == "live":
                draw_auto_setup(ws, picked, cfg, tgs, {
                    "label": "LIVE ACCOUNT - REAL MONEY",
                    "help": [f"{WARN}The bot will place REAL trades with REAL money "
                             f"({money(cfg['amount'])} base amount, "
                             + (f"x{cfg['loss_mult']} after every loss" if cfg["loss_mult"]
                                else f"MTG x{cfg['mult']:g}") + f").{R}",
                             f"{DIM}Type {R}{chip('YES')}{DIM} to start, anything else goes back.{R}"]})
                if (await ainput(ask_prompt("Confirm LIVE trading"))).strip().upper() != "YES":
                    notice = "Live trading was not confirmed."
                    continue
            return setup_result(cfg, tgs)
        lib_ack = risk_ack = False
        if low in ("r", "reset"):
            cfg, tgs, notice = dict(AUTO_DEFAULTS), new_tg_state(), "All settings are back to their defaults."
            continue
        top = TG_RES_ROW if tgs["on"] else TG_ROW
        m = _CMD.fullmatch(text)
        if not m or not 1 <= int(m.group(1)) <= top:
            notice = f"Type a number from 1 to {top}, Enter to start, r to reset or q to cancel."
            continue
        num, arg = int(m.group(1)), (m.group(2) or "").strip()
        if num in AUTO_FIELDS:
            notice = await _auto_edit_field(ws, picked, cfg, tgs, num, arg)
        elif num == TG_ROW:
            notice = await _tg_toggle(picked, cfg, tgs, arg, draw, TG_ROW)
        elif num == TG_TOKEN_ROW:
            notice = await _tg_edit_token(picked, cfg, tgs, draw, TG_CHAT_ROW)
        elif num == TG_CHAT_ROW:
            notice = await _tg_edit_chat(picked, cfg, tgs, draw)
        elif num == TG_SIG_ROW:
            notice = _chart_toggle(tgs, "chart_sig", "Signal chart", TG_SIG_ROW, arg)
        else:
            notice = _chart_toggle(tgs, "chart_res", "Result chart", TG_RES_ROW, arg)


# ---------------------------------------------------------------------
#  The flow
# ---------------------------------------------------------------------
async def auto_trade_flow(ws, big, ctx=None):
    picked = await pick_markets(ws, "AUTO TRADING", "trade the live signals automatically", big)
    if not picked:
        return
    cfg = await ask_auto_setup(ws, picked)
    if not cfg:
        return
    demo = cfg["account"] == "demo"
    broker = Broker(ws, demo=demo)
    broker.attach()
    prev_account = ctx.get("account") if ctx is not None else None
    notifier = None
    try:
        async with Progress(f"Switching to the {'DEMO' if demo else 'LIVE'} account"):
            await broker.select_account(demo)
            await asyncio.sleep(1.0)                      # the server answers with the balances
        if ctx is not None:
            ctx["account"] = f"{'DEMO' if demo else 'LIVE'} \u00b7 auto trading"
            set_ui_ctx(ctx)
        if cfg.get("tg"):
            notifier = TelegramNotifier(cfg["tg"]["token"], cfg["tg"]["chat"])
        session = AutoTradeSession(ws, cfg, broker, notifier=notifier)
        if notifier is not None:
            notifier.on_error = lambda why: session.note(f"Telegram: {why}")
            notifier.start()
            async with Progress("Preparing the chart engine"):
                mods, err = await asyncio.get_running_loop().run_in_executor(None, charts_backend)
            session.charts_ok, session.charts_err = mods is not None, err
            await session.send_start(picked)
        stop = asyncio.Event()
        started = False
        try:
            try:
                try:
                    ok = await session.load(picked)
                except BaseException:
                    ws.tick_cb = None
                    raise
                if not ok:
                    ws.tick_cb = None
                    rows = [f"{it['symbol']}: {why}" for it, why in session.skipped]
                    raise QuotexError("No market could be loaded for the auto trading engine.\n" + "\n".join(rows))
                begin_screen()
                put()
                session.print_header()
                started = True
                await session.run(stop)
                put()
                session.finish()
            except (KeyboardInterrupt, asyncio.CancelledError):
                ws.tick_cb = None
                if started:
                    put()
                    session.finish(send=False)
                raise
            if notifier is not None:
                await notifier.close()
        finally:
            if notifier is not None and notifier.queue is not None:
                await notifier.close(timeout=5.0)
    finally:
        broker.detach()
        if ctx is not None and prev_account is not None:
            ctx["account"] = prev_account
            set_ui_ctx(ctx)
    await pause()


# =====================================================================
#  module: darkmatter/ui/menus.py  -  Main menu / test menu item lists and the menu printer.
# =====================================================================
#  Menus
# ---------------------------------------------------------------------
MAIN_ITEMS = [
    ("1", "TEST BOT", "balance, history, live ticks, markets and diagnostics"),
    ("2", "LIVE SIGNAL", "multi-market AI signals for the next 1-minute candle"),
    ("3", "AUTO TRADING", "trade the live signals automatically (demo / live account, MTG)"),
    ("0", "EXIT", "close the bot and the browser bridge"),
]
TEST_ITEMS = [
    ("1", "BALANCE", "demo / live account balance"),
    ("2", "HISTORY", "historical candles (one or more markets)"),
    ("3", "LIVE", "live prices (one or more markets)"),
    ("4", "MARKETS", "live + OTC market list with payouts"),
    ("5", "DEBUG", "event log and debug samples"),
    ("0", "BACK", "return to the main menu"),
]


def print_menu(title, items, footer):
    begin_screen()
    put()
    box = Box(title)
    box.row()
    for key, name, desc in items:
        box.row(f" {chip(key)}  {TITLE}{name:<13}{R}{DIM}{desc}{R}")
    box.row()
    box.rule()
    box.row(footer)
    box.show()


# ---------------------------------------------------------------------


# =====================================================================
#  module: bootstrap.py
# =====================================================================
# =====================================================================
#  ENVIRONMENT SETUP  (Termux: Chromium + fonts are installed the first time, nothing else is needed)
# =====================================================================
TERMUX_REPO_PKGS = ["x11-repo", "tur-repo"]
TERMUX_BROWSER_PKG = "chromium"
TERMUX_FONT_PKGS = ["fontconfig", "ttf-dejavu", "noto-fonts"]     # best effort: a missing one is skipped


def _run_visible(cmd):
    """Run a command with its output on the screen. -> exit code (-1 when it could not be started)."""
    env = dict(os.environ, DEBIAN_FRONTEND="noninteractive")
    try:
        return subprocess.call(cmd, env=env)
    except OSError:
        return -1


def _ensure_termux_fonts_conf():
    """Let fontconfig also see the Android system fonts, so Chromium always has a font for the pictures."""
    if not is_termux() or not os.path.isdir("/system/fonts"):
        return
    path = os.path.join(os.path.expanduser("~"), ".config", "fontconfig", "fonts.conf")
    if os.path.exists(path):
        return
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write('<?xml version="1.0"?>\n<!DOCTYPE fontconfig SYSTEM "fonts.dtd">\n'
                    '<fontconfig>\n  <dir>/system/fonts</dir>\n</fontconfig>\n')
    except OSError:
        pass


def _termux_install():
    """pkg install of Chromium (+ its repositories + fonts). -> True when a Chromium binary exists afterwards."""
    if not shutil.which("pkg"):
        warn("The 'pkg' command was not found - install Chromium by hand.")
        return False
    put()
    info("Installing Chromium (this downloads a few hundred MB, it only happens once)...")
    _run_visible(["pkg", "install", "-y"] + TERMUX_REPO_PKGS)
    rc = _run_visible(["pkg", "install", "-y", TERMUX_BROWSER_PKG])
    if rc != 0:
        warn("Retrying after a package index update...")
        _run_visible(["pkg", "update", "-y"])
        _run_visible(["pkg", "install", "-y"] + TERMUX_REPO_PKGS)
        _run_visible(["pkg", "install", "-y", TERMUX_BROWSER_PKG])
    for fp in TERMUX_FONT_PKGS:
        _run_visible(["pkg", "install", "-y", fp])
    _ensure_termux_fonts_conf()
    return find_chromium() is not None


def termux_wake(on):
    """Keep the phone from killing Termux while the bot runs (termux-wake-lock is part of Termux)."""
    if not is_termux():
        return
    cmd = shutil.which("termux-wake-lock" if on else "termux-wake-unlock")
    if cmd:
        try:
            subprocess.run([cmd], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           timeout=10)
        except Exception:
            pass


async def ensure_environment(args):
    """Runs before the connection: makes sure a Chromium exists (Termux: offers to install it)."""
    _ensure_termux_fonts_conf()
    exe = find_chromium()
    if args.setup:
        if not is_termux():
            put()
            info("--setup is for Termux. Chromium: " + (exe or "not found - install Chrome / Chromium or set CHROME_PATH"))
            return exe is not None
        ok = _termux_install()
        put()
        if ok:
            good("Chromium is ready: " + str(find_chromium()))
        else:
            warn("Chromium could not be installed.")
        return ok
    if exe or args.direct:
        return True
    if not is_termux():
        return True                      # Bridge.start() explains what is missing
    put()
    warn("Chromium is not installed in Termux yet - bridge mode and the chart pictures need it.")
    answer = (await ainput(ask_prompt("Install it now? [Y/n]"))).lower()
    if answer in ("", "y", "yes"):
        ok = _termux_install()
        put()
        if ok:
            good("Chromium installed: " + str(find_chromium()))
        else:
            warn("Chromium could not be installed. Try:  pkg install x11-repo tur-repo && pkg install chromium")
        return ok
    return False


# ---------------------------------------------------------------------


# =====================================================================
#  module: darkmatter/flows/app.py  -  goodbye, run_action, test_menu, menu_loop and main().
# =====================================================================
def goodbye(ctx, big):
    screen_title("GOODBYE", f"session closed after {fmt_uptime(time.time() - ctx['started'])}", big)
    put()


async def run_action(coro_fn, args):
    """Run one menu action; errors never kill the menu (the error stays on screen until Enter)."""
    try:
        await coro_fn()
        return
    except QuotexError as e:
        fail(e)
    except (KeyboardInterrupt, asyncio.CancelledError, EOFError):
        raise
    except Exception as e:  # keep the menu alive on anything unexpected
        fail(f"Unexpected error: {type(e).__name__}: {e}")
        if args.debug:
            traceback.print_exc()
    await pause()


async def test_menu(ws, args, ctx, settings):
    big = args.big_menu
    notice = None
    while True:
        print_menu("TEST BOT", TEST_ITEMS, status_line(ws, ctx))
        if notice:
            warn(notice)
            notice = None
        choice = (await ainput(ask_prompt("Select"))).lower()
        if choice == "1":
            await run_action(lambda: balance_flow(ws, big), args)
        elif choice == "2":
            async def history():
                picked = await pick_markets(ws, "HISTORY", "historical candles (one or more markets)", big)
                if not picked:
                    return
                tf = await ask_timeframe(settings["tf"], picked)
                n = await ask_count(settings["n"], picked, tf) if tf else None
                if tf and n:
                    settings["tf"], settings["n"] = tf, n
                    await history_flow(ws, picked, tf, n)
            await run_action(history, args)
        elif choice == "3":
            async def live():
                picked = await pick_markets(ws, "LIVE", "live prices (one or more markets)", big)
                if picked:
                    await live_flow(ws, picked)
            await run_action(live, args)
        elif choice == "4":
            await run_action(lambda: show_market(ws, big), args)
        elif choice == "5":
            await run_action(lambda: debug_flow(ws, big), args)
        elif choice in ("0", *BACK_WORDS):
            return
        elif choice:
            notice = "Please choose a number from the list (0 goes back)."


async def menu_loop(ws, args, ctx):
    settings = {"tf": 60, "n": 100}
    notice = None
    while True:
        print_menu("MAIN MENU", MAIN_ITEMS, status_line(ws, ctx))
        if notice:
            warn(notice)
            notice = None
        try:
            choice = (await ainput(ask_prompt("Select"))).lower()
        except EOFError:
            return
        try:
            if choice == "1":
                await test_menu(ws, args, ctx, settings)
            elif choice == "2":
                await run_action(lambda: live_signal_flow(ws, args.big_menu), args)
            elif choice == "3":
                await run_action(lambda: auto_trade_flow(ws, args.big_menu, ctx), args)
            elif choice in ("0", "exit", "quit", "q"):
                goodbye(ctx, args.big_menu)
                return
            elif choice:
                notice = "Please choose 1 (TEST BOT), 2 (LIVE SIGNAL), 3 (AUTO TRADING) or 0 (EXIT)."
        except EOFError:
            return


async def main():
    ap = argparse.ArgumentParser(description="DARKMATTER - Quotex data bot")
    ap.add_argument("--debug", action="store_true", help="print every WebSocket frame")
    ap.add_argument("--direct", action="store_true", help="connect directly (no browser window)")
    ap.add_argument("--ssid", help="(direct mode) SSID copied from the browser")
    ap.add_argument("--live", action="store_true", help="(direct mode) use the live account")
    ap.add_argument("--big-menu", action="store_true", help="draw screen titles in the big colour font")
    ap.add_argument("--no-color", action="store_true", help="plain output without colours")
    ap.add_argument("--headed", action="store_true", help="show the Chromium window (needs a display, e.g. Termux:X11)")
    ap.add_argument("--headless", action="store_true", help="never show a Chromium window (login in the terminal)")
    ap.add_argument("--setup", action="store_true", help="install / check Chromium and fonts (Termux), then exit")
    ap.add_argument("--url", help="page Chromium opens first (default https://market-qx.info/en/sign-in)")
    args = ap.parse_args()
    if args.headed and args.headless:
        ap.error("--headed and --headless cannot be used together")
    if args.url:
        globals()["START_URL"] = args.url
        globals()["BASE"] = args.url.split("/", 3)[0] + "//" + args.url.split("/", 3)[2]

    enable_ansi()
    for stream in (sys.stdout, sys.stderr):  # make sure emoji / box characters can be printed
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    set_color(sys.stdout.isatty() and not args.no_color and "NO_COLOR" not in os.environ)
    set_clear(not args.debug)  # --debug keeps the raw frame log readable
    CHARTS.bind(asyncio.get_running_loop())
    try:
        proceed = await ensure_environment(args)
    except (EOFError, KeyboardInterrupt):
        return
    if args.setup:
        return
    if proceed is False and not args.direct:
        warn("Continuing without Chromium: only direct mode (--direct) can work.")

    ctx = {
        "started": time.time(),
        "debug": args.debug,
        "mode_label": "DIRECT \u00b7 WebSocket" if args.direct else "BRIDGE \u00b7 Chromium",
        "account": ("LIVE" if args.live else "DEMO") if args.direct else "as selected on the page",
        "state": "connecting",
    }
    set_ui_ctx(ctx)
    begin_screen()

    bridge = None
    ws = None
    termux_wake(True)
    try:
        try:
            if args.direct:
                ws = await direct_connect(args)
            else:
                bridge = Bridge(debug=args.debug, headless=True if args.headless else (False if args.headed else None))
                ws = await bridge.start()
        except Exception as e:
            ctx["state"] = "failed"
            set_ui_ctx(ctx)
            begin_screen()
            if isinstance(e, QuotexError):
                fail(e)
            else:
                fail(f"{type(e).__name__}: {e}")
                if args.debug:
                    traceback.print_exc()
            put()
            return

        ws.start_background_refresh()
        ctx["state"] = "connected"  # the next screen clears the connect messages and shows this
        set_ui_ctx(ctx)
        await menu_loop(ws, args, ctx)
    finally:
        if bridge:
            await bridge.close()
        elif ws:
            await ws.close()
        await CHARTS.aclose()
        termux_wake(False)


# =====================================================================
#  entry point
# =====================================================================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        put(f"\n{DIM}Bye.{R}")
