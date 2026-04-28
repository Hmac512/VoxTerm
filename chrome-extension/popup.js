const portInput = document.getElementById("port");
const debugInput = document.getElementById("debug");
const statusEl = document.getElementById("status");
const dotEl = document.getElementById("dot");
const tilesEl = document.getElementById("tiles");
const speakersEl = document.getElementById("speakers");
const reloadBtn = document.getElementById("reload");

const DEFAULT_PORT = 8765;

chrome.storage.local.get(["port", "debug"], (v) => {
  portInput.value = v.port ?? DEFAULT_PORT;
  debugInput.checked = !!v.debug;
});

portInput.addEventListener("change", () => {
  const n = parseInt(portInput.value, 10);
  if (Number.isFinite(n) && n >= 1024 && n <= 65535) {
    chrome.storage.local.set({ port: n });
  }
});

debugInput.addEventListener("change", () => {
  chrome.storage.local.set({ debug: debugInput.checked });
});

reloadBtn.addEventListener("click", async () => {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (tab?.id) chrome.tabs.reload(tab.id);
});

async function refreshStatus() {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tab?.id || !/^https:\/\/meet\.google\.com\//.test(tab.url || "")) {
    statusEl.textContent = "open a meet.google.com tab";
    dotEl.className = "dot bad";
    return;
  }
  try {
    const resp = await chrome.tabs.sendMessage(tab.id, { type: "status" });
    if (!resp) throw new Error("no response");
    if (resp.tiles === 0) {
      statusEl.textContent = "no tiles — check console";
      dotEl.className = "dot bad";
    } else {
      statusEl.textContent = resp.connected ? "connected" : "waiting for VoxTerm…";
      dotEl.className = "dot " + (resp.connected ? "ok" : "bad");
    }
    const audio = resp.webrtc ? " · audio ✓" : "";
    tilesEl.textContent = (resp.tiles ? `${resp.tiles} tile${resp.tiles === 1 ? "" : "s"}` : "0 tiles") + audio;
    if (resp.speakers && resp.speakers.length) {
      speakersEl.innerHTML = resp.speakers
        .map((s) => `<div><span class="name">${escapeHtml(s.name)}</span></div>`)
        .join("");
    } else {
      speakersEl.innerHTML = '<span class="empty">no active speakers</span>';
    }
  } catch {
    statusEl.textContent = "content script not loaded";
    dotEl.className = "dot bad";
  }
}

function escapeHtml(s) {
  return s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

refreshStatus();
setInterval(refreshStatus, 500);
