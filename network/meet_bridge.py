"""Meet Bridge — WebSocket server that receives speaker-attribution events
from the VoxTerm Chrome extension running in Google Meet.

The extension watches Meet's per-tile mic indicator and emits one of:
  {"type": "hello", "perfNow": ..., "wallNow": ..., "meetUrl": ...}
  {"type": "speak", "id": <participant>, "name": <display>, "speaking": bool, "t": <ms>}
  {"type": "leave", "id": <participant>, "name": <display>, "t": <ms>}

We keep a timeline of "who was speaking when" and let the transcription
pipeline query it at segment-finalization time to override the local
diarization label with the Meet speaker name.

Threading model: a daemon thread runs its own asyncio loop with the
websockets server. The Textual app calls `start()` / `stop()` from the
main thread, and the transcription worker thread calls `attribute()`.
A simple Lock around the timeline state is enough — handlers are
short, never await while holding it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class _SpeakInterval:
    name: str
    start_ms: float
    end_ms: float


class MeetBridge:
    # Min fraction of the segment that must overlap a single speaker's
    # interval for us to override the local diarization label. Keeps us
    # honest in fast back-and-forth where Meet's signal is uncertain.
    MIN_OVERLAP_FRACTION = 0.3

    # Cap on retained closed intervals. ~2000 entries covers a few hours
    # of normal conversation; older intervals are dropped FIFO.
    MAX_INTERVALS = 2000

    def __init__(self, port: int = 8765, host=None):
        self._port = port
        # Bind to both IPv4 and IPv6 loopback explicitly. Passing "localhost"
        # to websockets.serve() relies on getaddrinfo, and on macOS that
        # often returns only ::1 — but Chrome tries 127.0.0.1 first when
        # it sees "localhost", so the extension can't reach an IPv6-only
        # server. Listing both addresses dodges the resolution race.
        self._host = host if host is not None else ["127.0.0.1", "::1"]
        self._lock = threading.Lock()
        # participant_id -> {"name": str, "since_ms": float}
        self._active: dict[str, dict] = {}
        self._intervals: deque[_SpeakInterval] = deque(maxlen=self.MAX_INTERVALS)
        self._client_count = 0
        self._last_event_at: float = 0.0
        # Lifecycle
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event: asyncio.Event | None = None
        self._started = threading.Event()
        # User-facing event hooks. Each is called from the bridge's own
        # thread; consumers must marshal to their UI loop themselves.
        # on_listening(host: str, port: int, error: str | None)
        # on_client_change(count: int)
        self.on_listening = None
        self.on_client_change = None

    # ---------------------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run_loop, name="meet-bridge", daemon=True
        )
        self._thread.start()
        # Don't block forever on startup — if websockets import or bind fails,
        # we want the app to come up and surface the error in logs.
        self._started.wait(timeout=2.0)

    def stop(self) -> None:
        loop, stop_event = self._loop, self._stop_event
        if loop and stop_event and not stop_event.is_set():
            loop.call_soon_threadsafe(stop_event.set)
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _run_loop(self) -> None:
        try:
            asyncio.run(self._serve())
        except Exception:
            log.exception("MeetBridge event loop crashed")

    async def _serve(self) -> None:
        try:
            from websockets.asyncio.server import serve  # websockets >= 13
        except ImportError:
            try:
                from websockets.server import serve  # websockets 10–12
            except ImportError:
                log.warning(
                    "MeetBridge: 'websockets' package not installed; "
                    "Meet attribution disabled."
                )
                self._started.set()
                return

        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()
        host_label = ",".join(self._host) if isinstance(self._host, (list, tuple)) else str(self._host)
        try:
            server = await serve(self._handle, self._host, self._port)
        except OSError as e:
            log.warning("MeetBridge bind failed on %s:%d: %s", host_label, self._port, e)
            self._safe_callback(self.on_listening, host_label, self._port, str(e))
            self._started.set()
            return

        log.info("MeetBridge listening on ws://[%s]:%d/meet", host_label, self._port)
        self._safe_callback(self.on_listening, host_label, self._port, None)
        self._started.set()
        try:
            await self._stop_event.wait()
        finally:
            server.close()
            await server.wait_closed()
            self._flush_active("server shutdown")

    # ---------------------------------------------------------------------
    # WebSocket handler
    # ---------------------------------------------------------------------

    async def _handle(self, websocket) -> None:
        # We don't enforce a path — the extension uses /meet but accepting
        # any path makes the server forgiving for tests.
        with self._lock:
            self._client_count += 1
            count = self._client_count
        log.debug("MeetBridge client connected (count=%d)", count)
        self._safe_callback(self.on_client_change, count)
        try:
            async for raw in websocket:
                if isinstance(raw, bytes):
                    try:
                        raw = raw.decode("utf-8")
                    except UnicodeDecodeError:
                        continue
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                self._on_message(msg)
        except Exception:
            log.exception("MeetBridge handler error")
        finally:
            with self._lock:
                self._client_count -= 1
                count = self._client_count
            # Don't flush per-disconnect — the extension reconnects on
            # transient drops and we'd lose continuity. Active intervals
            # only flush on full server shutdown.
            log.debug("MeetBridge client disconnected (count=%d)", count)
            self._safe_callback(self.on_client_change, count)

    def _on_message(self, msg: dict) -> None:
        t = msg.get("type")
        now_ms = time.time() * 1000.0
        ts = float(msg.get("t") or now_ms)
        # Cap forward skew: if the extension's clock is ahead of ours, clamp
        # to "now" so attribute() lookups don't sit waiting for a future
        # window. Backward skew (extension behind) is harmless.
        ts = min(ts, now_ms + 1000.0)

        if t == "speak":
            sid = str(msg.get("id") or "")
            name = str(msg.get("name") or "").strip()
            speaking = bool(msg.get("speaking"))
            if not sid or not name:
                return
            with self._lock:
                self._last_event_at = now_ms
                if speaking:
                    self._active[sid] = {"name": name, "since_ms": ts}
                else:
                    prev = self._active.pop(sid, None)
                    if prev and ts > prev["since_ms"]:
                        self._intervals.append(
                            _SpeakInterval(
                                name=prev["name"],
                                start_ms=prev["since_ms"],
                                end_ms=ts,
                            )
                        )
        elif t == "leave":
            sid = str(msg.get("id") or "")
            with self._lock:
                prev = self._active.pop(sid, None)
                if prev:
                    self._intervals.append(
                        _SpeakInterval(
                            name=prev["name"],
                            start_ms=prev["since_ms"],
                            end_ms=ts,
                        )
                    )
        # "hello" and unknown types: ignore (no state to update)

    def _flush_active(self, reason: str) -> None:
        now = time.time() * 1000.0
        with self._lock:
            if not self._active:
                return
            for s in self._active.values():
                self._intervals.append(
                    _SpeakInterval(
                        name=s["name"], start_ms=s["since_ms"], end_ms=now
                    )
                )
            self._active.clear()
        log.debug("MeetBridge flushed active speakers: %s", reason)

    # ---------------------------------------------------------------------
    # Query API (called from worker thread)
    # ---------------------------------------------------------------------

    def attribute(self, t_start_ms: float, t_end_ms: float) -> Optional[str]:
        """Return the Meet speaker with the most overlap in the given
        wall-clock range, or None if no speaker covers at least
        MIN_OVERLAP_FRACTION of the range.

        t_start_ms / t_end_ms must be in the same epoch as time.time()*1000
        (matches the extension's Date.now() values).
        """
        if t_end_ms <= t_start_ms:
            return None
        seg_dur = t_end_ms - t_start_ms
        scores: dict[str, float] = {}

        with self._lock:
            for iv in self._intervals:
                ov = min(iv.end_ms, t_end_ms) - max(iv.start_ms, t_start_ms)
                if ov > 0:
                    scores[iv.name] = scores.get(iv.name, 0.0) + ov
            # Active intervals extend to "now" — clamp to t_end_ms so an
            # in-progress speaker doesn't dominate ranges entirely in
            # their past.
            now_ms = time.time() * 1000.0
            for s in self._active.values():
                end = min(now_ms, t_end_ms)
                ov = end - max(s["since_ms"], t_start_ms)
                if ov > 0:
                    scores[s["name"]] = scores.get(s["name"], 0.0) + ov

        if not scores:
            return None
        best_name, best_ov = max(scores.items(), key=lambda kv: kv[1])
        if best_ov < seg_dur * self.MIN_OVERLAP_FRACTION:
            return None
        return best_name

    # ---------------------------------------------------------------------
    # Diagnostic / status
    # ---------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        with self._lock:
            return self._client_count > 0

    @staticmethod
    def _safe_callback(cb, *args):
        if cb is None:
            return
        try:
            cb(*args)
        except Exception:
            log.exception("MeetBridge callback raised")

    def status(self) -> dict:
        with self._lock:
            return {
                "clients": self._client_count,
                "active": len(self._active),
                "intervals": len(self._intervals),
                "last_event_at": self._last_event_at,
            }
