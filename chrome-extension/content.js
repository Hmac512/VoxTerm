// VoxTerm Meet Bridge — content script
//
// Watches the Google Meet participant grid for active-speaker indicators
// and streams {name, speaking, t} events to ws://localhost:<port>/meet.
//
// The DOM selectors here target Meet's current UI. Meet ships UI changes
// frequently — when this breaks, run with VOXTERM_DEBUG=1 in the popup
// and re-derive the selectors from console output.

(() => {
  const DEFAULTS = {
    port: 8765,
    path: "/meet",
    debug: false,
    // How long a tile must remain in the "speaking" state before we emit
    // a start event. Filters out the rapid flicker Meet shows on attack.
    speakStartMs: 120,
    // How long a tile must remain quiet before we emit an end event.
    // Bridges short pauses inside a continuous utterance.
    speakEndMs: 350,
  };

  // Selectors for current Meet. Centralized so they're easy to update
  // when Meet's DOM changes. We try several in order and union the
  // results — Meet's class names are obfuscated and rotate, but at
  // least one of these signals is usually present.
  const SEL = {
    // Tile candidates. Listed in preference order: stable data attrs
    // first, then video-element ancestor walk as a structural fallback.
    tileSelectors: [
      "[data-participant-id]",
      "[data-requested-participant-id]",
      "[data-allocation-index]",
      "[data-self-name]",
    ],
    // The tile's display name. Meet renders names inside a span with
    // class .notranslate (Google's "don't auto-translate this" marker,
    // applied to user-supplied strings) under .OFfHfd. The text-node
    // fallback is dangerous here because tooltip strings like "Pin X to
    // your main screen" are longer than the name itself.
    nameInTile: ".OFfHfd .notranslate, [data-self-name], [jsname='YS01Ge'], .zWGUib, .KV1GEc, .NnTWjc",
  };

  let ws = null;
  let wsReady = false;
  let reconnectTimer = null;
  let reconnectDelay = 500;
  let config = { ...DEFAULTS };

  // Audio-level threshold for "speaking". audioLevel from getStats() is
  // a normalized peak level (0..1). Background-quiet rooms sit near 0;
  // typical speech sits ~0.05–0.4. 0.01 is well above floor and below
  // any plausible idle level.
  const AUDIO_LEVEL_THRESHOLD = 0.01;

  // tileId -> { name, speaking, lastChangeAt, pendingTimer }
  const state = new Map();

  // SSRC observability state: did the page-world hook ever post a
  // sample? When true, we trust WebRTC and skip the visual fallback.
  let webrtcSeenAt = 0;

  const log = (...args) => {
    if (config.debug) console.log("[voxterm-meet]", ...args);
  };

  const warn = (...args) => console.warn("[voxterm-meet]", ...args);

  // ---------------------------------------------------------------------
  // Transport
  // ---------------------------------------------------------------------

  function connect() {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
      return;
    }
    const url = `ws://localhost:${config.port}${config.path}`;
    log("connecting to", url);
    try {
      ws = new WebSocket(url);
    } catch (e) {
      warn("ws construct failed", e);
      scheduleReconnect();
      return;
    }
    ws.addEventListener("open", () => {
      wsReady = true;
      reconnectDelay = 500;
      log("ws open");
      send({
        type: "hello",
        client: "voxterm-meet-bridge",
        version: "0.1.0",
        // Browser monotonic clock anchor for time-sync handshake on the
        // VoxTerm side. Pair with Date.now() to convert to wall clock.
        perfNow: performance.now(),
        wallNow: Date.now(),
        meetUrl: location.href,
      });
      // Re-emit current state so a reconnecting VoxTerm sees who's
      // speaking right now without waiting for the next transition.
      for (const [id, s] of state) {
        if (s.speaking) {
          send({ type: "speak", id, name: s.name, speaking: true, t: Date.now(), resync: true });
        }
      }
    });
    ws.addEventListener("close", () => {
      wsReady = false;
      log("ws closed");
      scheduleReconnect();
    });
    ws.addEventListener("error", (e) => {
      log("ws error", e);
      // close handler will schedule reconnect
    });
  }

  function scheduleReconnect() {
    if (reconnectTimer) return;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      reconnectDelay = Math.min(reconnectDelay * 2, 10000);
      connect();
    }, reconnectDelay);
  }

  function send(obj) {
    if (!wsReady) return;
    try {
      ws.send(JSON.stringify(obj));
    } catch (e) {
      warn("ws send failed", e);
    }
  }

  // ---------------------------------------------------------------------
  // DOM observation
  // ---------------------------------------------------------------------

  function getName(tile) {
    const el = tile.querySelector(SEL.nameInTile);
    if (el) {
      const txt = (el.getAttribute("data-self-name") || el.textContent || "").trim();
      if (txt) return txt;
    }
    // Last-resort fallback: longest text node within the tile, but
    // explicitly skip text that looks like a Meet button tooltip
    // ("Pin X to your main screen", "More options for X", etc.).
    let best = "";
    const walker = document.createTreeWalker(tile, NodeFilter.SHOW_TEXT);
    let node;
    while ((node = walker.nextNode())) {
      const t = node.textContent.trim();
      if (!t || t.length >= 80) continue;
      if (/\b(Pin|Mute|More options|Reframe|Backgrounds)\b/i.test(t)) continue;
      if (t.length > best.length) best = t;
    }
    return best || "(unknown)";
  }

  // Speaker detection via mutation rate.
  //
  // We deliberately avoid naming any specific element — every Meet
  // identifier we've tried (.IisKdb, jscontroller="tae9tc",
  // jsname="QgSmzd", state class gjg47c↔wEsLMd) rotates on a months-
  // long cadence. Instead we attach a MutationObserver to each tile
  // and count `class`-attribute mutations anywhere in its subtree
  // over a short sliding window. The mic-level indicator animates by
  // class-swapping ~5–10×/sec while a participant is talking; idle
  // tiles see ≤1–2 mutations/sec from layout / hover / tooltip noise.
  // A threshold between those two regimes catches speech reliably
  // without needing to know what Meet's current class soup looks
  // like.
  //
  // The observer is attached lazily on first scan and held in a Map
  // keyed by tileId so it survives tile re-renders within the same
  // session. detachMutationObserver() is called on tile-leave to
  // stop the observer and free the reference.
  const SPEAK_WINDOW_MS = 1000;
  // Empirical: the gjg47c↔wEsLMd flip + the per-bar transform classes
  // produce ~5–8 mutations/sec under continuous speech. Hover/focus
  // animations on a single button burst to ~3 once and stop. Threshold
  // sits comfortably between.
  const SPEAK_MUTATION_THRESHOLD = 4;

  const tileObservers = new Map();   // tileId -> { observer, tile, hits: number[] }

  function attachMutationObserver(tile, idStr) {
    const existing = tileObservers.get(idStr);
    if (existing && existing.tile === tile) return existing;
    if (existing) existing.observer.disconnect();
    const entry = { tile, hits: [], observer: null };
    entry.observer = new MutationObserver((muts) => {
      const now = Date.now();
      // Each mutation record represents one observed attribute write,
      // even if multiple attributes changed at once. Pushing per-record
      // (not per-batch) keeps the count proportional to actual UI
      // activity — the bars animation produces multiple discrete writes
      // per animation frame.
      for (let i = 0; i < muts.length; i++) entry.hits.push(now);
      // Bound memory: trim aggressively past 4× the window so a
      // long-running tile doesn't accumulate.
      const cutoff = now - SPEAK_WINDOW_MS * 4;
      let drop = 0;
      while (drop < entry.hits.length && entry.hits[drop] < cutoff) drop++;
      if (drop > 0) entry.hits.splice(0, drop);
    });
    entry.observer.observe(tile, {
      attributes: true,
      attributeFilter: ["class"],
      subtree: true,
    });
    tileObservers.set(idStr, entry);
    return entry;
  }

  function detachMutationObserver(idStr) {
    const entry = tileObservers.get(idStr);
    if (!entry) return;
    entry.observer.disconnect();
    tileObservers.delete(idStr);
  }

  function tileMutationRate(idStr) {
    const entry = tileObservers.get(idStr);
    if (!entry) return 0;
    const cutoff = Date.now() - SPEAK_WINDOW_MS;
    while (entry.hits.length && entry.hits[0] < cutoff) entry.hits.shift();
    return entry.hits.length;
  }

  function detectSpeakers(tiles) {
    const result = new Map();
    for (const tile of tiles) {
      const id = tileId(tile);
      if (!id) continue;
      attachMutationObserver(tile, id);
      const rate = tileMutationRate(id);
      result.set(tile, rate >= SPEAK_MUTATION_THRESHOLD);
    }
    return result;
  }

  function tileId(tile) {
    return (
      tile.getAttribute("data-participant-id") ||
      tile.getAttribute("data-requested-participant-id") ||
      tile.getAttribute("data-allocation-index") ||
      tile.getAttribute("data-self-name") ||
      // Fallback: hash the tile's position in the DOM. Stable enough
      // within a single session even if all data attrs are missing.
      `dom:${[...document.querySelectorAll("video")].indexOf(tile.querySelector("video"))}`
    );
  }

  // Find candidate participant tiles. Returns an array of unique tile
  // elements, deduped by closest common ancestor so we don't double-count
  // when nested elements both match.
  function findTiles() {
    const set = new Set();
    for (const sel of SEL.tileSelectors) {
      for (const el of document.querySelectorAll(sel)) set.add(el);
    }
    // Structural fallback: every participant tile contains a <video>.
    // Walk up to find the tile container — heuristically, the first
    // ancestor whose bounding rect width/height is "tile-sized"
    // (>200px in the smaller dimension).
    if (set.size === 0) {
      for (const v of document.querySelectorAll("video")) {
        let p = v;
        for (let i = 0; i < 8 && p.parentElement; i++) {
          p = p.parentElement;
          const r = p.getBoundingClientRect();
          if (Math.min(r.width, r.height) >= 160) {
            set.add(p);
            break;
          }
        }
      }
    }
    // De-nest: if A contains B and both are in the set, keep only A.
    const arr = [...set];
    return arr.filter((a) => !arr.some((b) => b !== a && b.contains(a)));
  }

  // Diagnostic dump for when nothing matches. Always logs (not gated
  // on debug) — this is the failure mode users most need help with.
  let lastDiscoverAt = 0;
  function discover(reason) {
    const now = Date.now();
    if (now - lastDiscoverAt < 3000) return;
    lastDiscoverAt = now;
    const counts = {};
    for (const sel of [
      "[data-participant-id]",
      "[data-requested-participant-id]",
      "[data-allocation-index]",
      "[data-self-name]",
      "[data-second-screen]",
      "[data-ssrc]",
      "video",
    ]) {
      counts[sel] = document.querySelectorAll(sel).length;
    }
    console.log("[voxterm-meet:discover]", reason, "selector counts:", counts);
    // Sample the ancestors of the first <video> so we can see what
    // attributes/classes the actual tile container carries.
    const v = document.querySelector("video");
    if (v) {
      let p = v;
      for (let i = 0; i < 6 && p.parentElement; i++) {
        p = p.parentElement;
        const r = p.getBoundingClientRect();
        const attrs = [...p.attributes].map((a) => `${a.name}="${(a.value || "").slice(0, 60)}"`).join(" ");
        console.log(
          `[voxterm-meet:discover] video ancestor ${i}`,
          `<${p.tagName.toLowerCase()}>`,
          `${Math.round(r.width)}x${Math.round(r.height)}`,
          attrs.slice(0, 200)
        );
      }
    } else {
      console.log("[voxterm-meet:discover] no <video> elements found yet");
    }
  }

  function transition(id, nextSpeaking, name) {
    const now = Date.now();
    let s = state.get(id);
    if (!s) {
      s = { name, speaking: false, lastChangeAt: 0, pendingTimer: null };
      state.set(id, s);
    }
    s.name = name;
    if (s.speaking === nextSpeaking) return;

    // Debounce attack/release transitions to suppress flicker.
    if (s.pendingTimer) {
      clearTimeout(s.pendingTimer);
      s.pendingTimer = null;
    }
    const delay = nextSpeaking ? config.speakStartMs : config.speakEndMs;
    s.pendingTimer = setTimeout(() => {
      s.pendingTimer = null;
      if (s.speaking === nextSpeaking) return;
      s.speaking = nextSpeaking;
      s.lastChangeAt = now;
      log(nextSpeaking ? "START" : "END", name, id);
      send({
        type: "speak",
        id,
        name,
        speaking: nextSpeaking,
        t: now,
        // perf clock for high-precision alignment on the recv side
        perfNow: performance.now(),
      });
    }, delay);
  }

  // ---------------------------------------------------------------------
  // WebRTC audio-level path (primary)
  // ---------------------------------------------------------------------
  //
  // page.js runs in Meet's JS world, hooks RTCPeerConnection, and posts
  // {ssrc, level} samples here every 200ms. We map ssrc → participant
  // by looking up the tile that carries data-ssrc="<ssrc>" and walking
  // up to the [data-participant-id] container.

  // ssrc(string) -> { tile, name, lastSeenAt } cache so we don't run a
  // selector lookup per-sample when the mapping is stable.
  const ssrcCache = new Map();

  function tileForSsrc(ssrc) {
    const cached = ssrcCache.get(ssrc);
    if (cached && document.contains(cached.tile)) return cached;
    const ssrcEl = document.querySelector(`[data-ssrc="${ssrc}"]`);
    if (!ssrcEl) return null;
    const tile =
      ssrcEl.closest("[data-participant-id]") ||
      ssrcEl.closest("[data-requested-participant-id]") ||
      ssrcEl.closest("[jsname='E2KThb']");
    if (!tile) return null;
    const entry = { tile, name: getName(tile), lastSeenAt: Date.now() };
    ssrcCache.set(ssrc, entry);
    return entry;
  }

  // Per-ssrc speaking state, separate from the tile-id keyed `state`
  // map: an ssrc's speaking flag depends on its own audioLevel, but we
  // emit transitions keyed by participant-id so a single tile sees a
  // coherent start/end stream.
  const ssrcSpeaking = new Map();

  function handleAudioLevel(ssrc, level) {
    webrtcSeenAt = Date.now();
    const ssrcKey = String(ssrc);
    const speaking = level > AUDIO_LEVEL_THRESHOLD;
    const prev = ssrcSpeaking.get(ssrcKey);
    if (prev === speaking) return;
    ssrcSpeaking.set(ssrcKey, speaking);

    const mapping = tileForSsrc(ssrcKey);
    if (!mapping) {
      // No tile yet; defer — Meet may attach data-ssrc shortly. Keep
      // the speaking flag so the next sample re-evaluates.
      return;
    }
    const id = tileId(mapping.tile);
    transition(id, speaking, mapping.name);
  }

  // Last diagnostic from page.js so we can surface "audioLevel missing"
  // explicitly in the console when nothing's arriving.
  let lastDiag = null;

  window.addEventListener("message", (e) => {
    if (e.source !== window) return;
    const data = e.data;
    if (!data || data.source !== "voxterm-meet") return;
    if (data.type === "hook-ready") {
      log("page hook ready");
      return;
    }
    if (data.type === "diag") {
      lastDiag = data;
      // Always log this — it's the fastest way to diagnose "0 speakers"
      // (either no inbound audio reports, or reports lack audioLevel).
      console.log(
        `[voxterm-meet] webrtc diag: pcs=${data.pcs} polls=${data.polls} ` +
          `inbound-audio=${data.inboundAudio} with-audioLevel=${data.withLevel}`
      );
      return;
    }
    if (data.type === "audio-levels" && Array.isArray(data.levels)) {
      for (const { ssrc, level } of data.levels) {
        handleAudioLevel(ssrc, level);
      }
      if (config.debug) {
        const top = [...data.levels]
          .sort((a, b) => b.level - a.level)
          .slice(0, 4)
          .map((l) => `${l.ssrc}=${l.level.toFixed(3)}`)
          .join(" ");
        log(`audio-levels n=${data.levels.length} top: ${top}`);
      }
    }
  });

  // When debug is on, log the class-diff result once per second so we
  // can see the signature distribution and which tile (if any) is being
  // flagged as the speaker.
  let lastDebugAt = 0;
  function debugDetect(tiles, speakerByTile) {
    if (!config.debug) return;
    const now = Date.now();
    if (now - lastDebugAt < 1000) return;
    lastDebugAt = now;
    const speakers = [];
    for (const tile of tiles) {
      const id = tileId(tile);
      const name = getName(tile);
      const rate = tileMutationRate(id);
      const isSpeaker = !!speakerByTile.get(tile);
      const flag = isSpeaker ? "🔊" : "  ";
      console.log(`[voxterm-meet:detect] ${flag} ${name}: ${rate} mutations/s`);
      if (isSpeaker) speakers.push(name);
    }
    console.log(
      `[voxterm-meet:detect] → speakers: ${speakers.length ? speakers.join(", ") : "(none)"}`
    );
  }

  let emptyScanCount = 0;
  function scanOnce() {
    const tiles = findTiles();
    if (tiles.length === 0) {
      emptyScanCount++;
      // After ~3s of empty scans, dump diagnostics so the user can see
      // what's actually in the DOM.
      if (emptyScanCount === 30 || emptyScanCount % 100 === 0) {
        discover(`no tiles after ${emptyScanCount} scans`);
      }
    } else {
      if (emptyScanCount > 0) {
        log(`tiles found: ${tiles.length} (after ${emptyScanCount} empty scans)`);
      }
      emptyScanCount = 0;
    }
    const seen = new Set();
    // Mutation-rate detection drives speaker attribution. WebRTC audio
    // levels remain useful as a global "is anyone actually talking"
    // diagnostic in the console (Meet's SFU multiplexes audio into a
    // single mixed SSRC, so per-participant levels aren't available),
    // but they don't drive transitions.
    const speakerByTile = detectSpeakers(tiles);
    debugDetect(tiles, speakerByTile);
    for (const tile of tiles) {
      const id = tileId(tile);
      if (!id) continue;
      seen.add(id);
      const name = getName(tile);
      let s = state.get(id);
      if (!s) {
        s = { name, speaking: false, lastChangeAt: 0, pendingTimer: null };
        state.set(id, s);
      } else {
        s.name = name;
      }
      transition(id, !!speakerByTile.get(tile), name);
    }
    // Tiles that disappeared (participant left) — flush them as not-speaking
    // and drop from state.
    for (const id of [...state.keys()]) {
      if (!seen.has(id)) {
        const s = state.get(id);
        if (s.speaking) {
          send({ type: "speak", id, name: s.name, speaking: false, t: Date.now() });
        }
        if (s.pendingTimer) clearTimeout(s.pendingTimer);
        state.delete(id);
        detachMutationObserver(id);
        send({ type: "leave", id, name: s.name, t: Date.now() });
      }
    }
  }

  // ---------------------------------------------------------------------
  // Driver
  // ---------------------------------------------------------------------

  // Poll at 10Hz. MutationObserver alone doesn't catch the audio-level
  // animations (those are pure style changes that don't fire mutations
  // for attribute or child changes), so we sample. 10Hz comfortably
  // beats Meet's ~5Hz indicator update rate.
  let pollTimer = null;
  function startPolling() {
    if (pollTimer) return;
    pollTimer = setInterval(scanOnce, 100);
  }
  function stopPolling() {
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  }

  // Once we see any signal that the call is live (a video element or
  // any of our tile selectors), start polling and connect. We don't
  // require tiles specifically because the user might be in the
  // pre-join screen briefly — once polling starts, scanOnce() runs the
  // fuller findTiles() logic and the discover() diagnostic fires if
  // tiles still can't be found.
  const liveSignal = () =>
    document.querySelector("video") ||
    SEL.tileSelectors.some((s) => document.querySelector(s));

  const bootObserver = new MutationObserver(() => {
    if (liveSignal()) {
      log("call signal detected, starting poll loop");
      bootObserver.disconnect();
      startPolling();
      connect();
    }
  });
  bootObserver.observe(document.body, { childList: true, subtree: true });

  if (liveSignal()) {
    startPolling();
    connect();
  }

  // First-pass discovery so we get diagnostics in the console without
  // having to wait for the empty-scan threshold.
  setTimeout(() => discover("boot"), 2000);

  // ---------------------------------------------------------------------
  // Config from popup
  // ---------------------------------------------------------------------

  chrome.storage.local.get(["port", "debug"], (v) => {
    if (typeof v.port === "number") config.port = v.port;
    if (typeof v.debug === "boolean") config.debug = v.debug;
    log("config loaded", config);
  });

  chrome.storage.onChanged.addListener((changes) => {
    let needsReconnect = false;
    if (changes.port) {
      config.port = changes.port.newValue || DEFAULTS.port;
      needsReconnect = true;
    }
    if (changes.debug) {
      config.debug = !!changes.debug.newValue;
    }
    if (needsReconnect && ws) {
      try { ws.close(); } catch {}
    }
  });

  // Expose minimal status for the popup.
  chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
    if (msg && msg.type === "status") {
      sendResponse({
        connected: wsReady,
        tiles: state.size,
        // Speaker identification is class-diff on the per-tile mic
        // indicator. The webrtc flag separately reports whether the
        // page hook is producing audio samples — useful for diagnostics
        // (it tells us audio is flowing) but not used to pick speakers.
        detection: "class-diff",
        webrtc: Date.now() - webrtcSeenAt < 3000,
        speakers: [...state.entries()]
          .filter(([, s]) => s.speaking)
          .map(([id, s]) => ({ id, name: s.name })),
      });
      return true;
    }
  });

  window.addEventListener("beforeunload", () => {
    stopPolling();
    bootObserver.disconnect();
    try { ws && ws.close(); } catch {}
  });
})();
