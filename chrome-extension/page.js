// VoxTerm Meet Bridge — page-world hook.
//
// Runs in Meet's own JS world (manifest "world": "MAIN") so it can see
// the page's RTCPeerConnection instances. Wraps the constructor, then
// periodically calls getStats() on every live PC and forwards the
// inbound audio levels to the content script via window.postMessage.
//
// Why this exists: Meet's per-tile mic-bar animation has been the
// classic CSS-scraping target, but the class names rotate, the bars
// animate via mechanisms that aren't always visible to getComputedStyle,
// and the visual indicator can lag the audio. getStats() returns the
// authoritative decoded audio level keyed by SSRC, which we map to a
// tile via the [data-ssrc] attribute Meet renders on each participant.

(() => {
  const TAG = "voxterm-meet-page";
  const NS = "voxterm-meet";
  const POLL_MS = 200;

  if (typeof RTCPeerConnection === "undefined") return;
  if (window.__voxtermMeetHooked) return;
  window.__voxtermMeetHooked = true;

  const pcs = new Set();
  const _RTCPeerConnection = window.RTCPeerConnection;

  // Subclass so prototype, statics, and instanceof all stay correct.
  // Meet's code does `new RTCPeerConnection(...)`, so the constructor
  // wrap is sufficient — we don't need to intercept any methods.
  class HookedPC extends _RTCPeerConnection {
    constructor(...args) {
      super(...args);
      pcs.add(this);
      this.addEventListener("connectionstatechange", () => {
        if (this.connectionState === "closed") pcs.delete(this);
      });
    }
  }
  window.RTCPeerConnection = HookedPC;

  function post(payload) {
    window.postMessage({ source: NS, ...payload }, "*");
  }

  // Diagnostic counters so we can tell from the popup/console whether
  // the hook is producing useful samples or whether stats simply lack
  // audioLevel on this build.
  let totalPolls = 0;
  let totalInboundAudio = 0;
  let totalWithLevel = 0;
  let lastDiagAt = 0;

  async function pollOnce() {
    if (pcs.size === 0) return;
    totalPolls++;
    const levels = [];
    let inboundAudio = 0;
    let withLevel = 0;
    for (const pc of pcs) {
      let stats;
      try {
        stats = await pc.getStats();
      } catch {
        continue;
      }
      for (const r of stats.values()) {
        if (r.type === "inbound-rtp" && r.kind === "audio") {
          inboundAudio++;
          // audioLevel (0..1) is the recent peak audio level of the
          // received track per the WebRTC stats spec. Some Chromium
          // builds also expose totalAudioEnergy which we could
          // differentiate over time, but audioLevel is simpler when
          // present.
          if (typeof r.audioLevel === "number") {
            withLevel++;
            levels.push({ ssrc: r.ssrc, level: r.audioLevel });
          }
        }
      }
    }
    totalInboundAudio += inboundAudio;
    totalWithLevel += withLevel;

    if (levels.length) post({ type: "audio-levels", levels, t: Date.now() });

    // Heartbeat every ~3s so the content script can detect stalls and
    // surface "audioLevel missing" vs "no inbound streams" to the user.
    const now = Date.now();
    if (now - lastDiagAt > 3000) {
      lastDiagAt = now;
      post({
        type: "diag",
        pcs: pcs.size,
        polls: totalPolls,
        inboundAudio,
        withLevel,
        t: now,
      });
    }
  }

  setInterval(pollOnce, POLL_MS);

  // Heartbeat so the content script can show "WebRTC hook active"
  // without waiting for the first audio sample.
  post({ type: "hook-ready", t: Date.now() });
  console.log(`[${TAG}] RTCPeerConnection hooked`);
})();
