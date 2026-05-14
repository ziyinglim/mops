"""Streamlit wrapper for the MOPSOV HTML report with shared review-state sync.

Architecture
------------
A tiny background HTTP server runs on API_PORT (default 8502) alongside
Streamlit.  It handles two routes:

    GET  /api/checks          → return all check states as JSON
    POST /api/checks          → merge a {key: {state,name,ts}} patch into
                                storage/state/check_states.json

The embedded HTML report has its localStorage-based chkGet / chkSet
functions overridden by an injected <script> that reads from / writes to
this API.  All clients poll every 30 s so states stay in sync.

Requirements
------------
Port API_PORT must be reachable by every team member at the same hostname
they use to access Streamlit (port 8501).  On a VPS, open port 8502 in
the same firewall / security-group rule set as 8501.

Usage
-----
    streamlit run app.py
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import streamlit as st

# ── Config ─────────────────────────────────────────────────────────────────────
API_PORT         = 8502
STATE_DIR        = Path("storage/state")
CHECK_STATE_PATH = STATE_DIR / "check_states.json"
REPORT_PATH      = Path("output/report.html")

# ── Thread-safe JSON state store ───────────────────────────────────────────────
_file_lock = threading.Lock()


def _read_states() -> dict:
    if not CHECK_STATE_PATH.exists():
        return {}
    with _file_lock:
        try:
            return json.loads(CHECK_STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}


def _write_states(patch: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with _file_lock:
        existing: dict = {}
        if CHECK_STATE_PATH.exists():
            try:
                existing = json.loads(CHECK_STATE_PATH.read_text(encoding="utf-8"))
            except Exception:
                pass
        existing.update(patch)
        CHECK_STATE_PATH.write_text(
            json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8"
        )


# ── HTTP request handler ───────────────────────────────────────────────────────
class _Handler(BaseHTTPRequestHandler):
    def _cors(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self) -> None:
        if self.path.rstrip("/") not in ("/api/checks", ""):
            self.send_response(404)
            self.end_headers()
            return
        body = json.dumps(_read_states(), ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        try:
            payload = json.loads(self.rfile.read(length))
            _write_states(payload)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self._cors()
            self.end_headers()
            self.wfile.write(b'{"ok":true}')
        except Exception as exc:
            self.send_response(500)
            self._cors()
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(exc)}).encode())

    def log_message(self, *args) -> None:
        pass  # suppress console noise


@st.cache_resource
def _start_api_server() -> None:
    """Start the check-state HTTP server once per Streamlit process."""
    try:
        srv = HTTPServer(("0.0.0.0", API_PORT), _Handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
    except OSError:
        pass  # already bound on hot-reload


# ── JS injection ───────────────────────────────────────────────────────────────
# Injected right before </body>.  Overrides chkGet / chkSet with API versions,
# loads all states after tables render, and polls every 30 s for updates.
_INJECT_TEMPLATE = """<script>
(function(){{
  /* Derive API URL from parent window — same server, different port. */
  var _h = '';
  try {{ _h = window.parent.location.hostname; }} catch(e) {{}}
  if (!_h) try {{ _h = window.location.hostname; }} catch(e) {{}}
  if (!_h) _h = 'localhost';
  var API = window.location.protocol + '//' + _h + ':{port}/api/checks';

  /* In-memory mirror keeps reads synchronous (no async delay on first render). */
  var _mem = {{}};

  /* Override the localStorage helpers defined earlier in the page. */
  window.chkGet = function(key) {{ return _mem[key] || null; }};
  window.chkSet = function(key, state, name) {{
    var d = {{ state: state, name: name, ts: new Date().toISOString() }};
    _mem[key] = d;
    fetch(API, {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{ [key]: d }})
    }}).catch(function() {{}});
  }};

  /* Walk every rendered chk-wrap and apply latest state from _mem. */
  function _applyAll() {{
    document.querySelectorAll('.chk-wrap').forEach(function(wrap) {{
      var btn = wrap.querySelector('.chk-btn');
      if (!btn) return;
      var m = (btn.getAttribute('onclick') || '').match(/toggleCheck\(this,'([^']+)'\)/);
      if (!m) return;
      var key = m[1];
      var d   = _mem[key] || {{}};
      var st  = d.state || 'pending';
      var lbl = {{ pending: 'Pending', checked: 'Checked ✓', irrelevant: 'Irrelevant' }};
      var cls = {{ pending: 'chk-pend', checked: 'chk-done', irrelevant: 'chk-irrel' }};
      btn.textContent = lbl[st] || 'Pending';
      btn.className   = 'chk-btn ' + (cls[st] || 'chk-pend');
      var inp = wrap.querySelector('.chk-name');
      if (inp && d.name) inp.value = d.name;
      var tsEl = wrap.querySelector('.chk-ts');
      if (tsEl && d.ts && typeof fmtTs === 'function') tsEl.textContent = fmtTs(d.ts);
    }});
  }}

  /* Fetch latest states, merge into _mem, update DOM. */
  function _sync() {{
    fetch(API)
      .then(function(r) {{ return r.json(); }})
      .then(function(data) {{ Object.assign(_mem, data); _applyAll(); }})
      .catch(function() {{}});
  }}

  /* First sync after tables are rendered; then poll every 30 s. */
  document.addEventListener('DOMContentLoaded', function() {{ setTimeout(_sync, 250); }});
  setInterval(_sync, 30000);
}})();
</script>"""


def _patched_html() -> str:
    html = REPORT_PATH.read_text(encoding="utf-8")
    inject = _INJECT_TEMPLATE.format(port=API_PORT)
    return html.replace("</body>", inject + "\n</body>", 1)


# ── Streamlit page ─────────────────────────────────────────────────────────────
def main() -> None:
    st.set_page_config(page_title="MOPSOV Monitor", layout="wide")
    _start_api_server()

    if not REPORT_PATH.exists():
        st.error(
            "No report found. "
            "Run `python mopsov.py --mode report-only` (or `--mode full`) to generate one."
        )
        return

    import datetime
    mtime = datetime.datetime.fromtimestamp(REPORT_PATH.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
    n_checks = len(_read_states())
    st.caption(
        f"Report generated: **{mtime}** · "
        f"**{n_checks}** review states saved · "
        f"Syncs every 30 s across all sessions  "
        f"*(requires port {API_PORT} open on this server alongside port 8501)*"
    )

    st.components.v1.html(_patched_html(), height=960, scrolling=True)


main()
