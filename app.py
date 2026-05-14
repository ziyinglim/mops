"""Streamlit wrapper for the MOPSOV HTML report.

Setup
-----
1. Place this file in the same directory as mopsov.py.
2. Generate the report:
       python mopsov.py
3. Run the app:
       streamlit run app.py

Requirements
------------
- mopsov.py must be present in the same directory.
- Port 8502 must be open on the host server (alongside Streamlit's port 8501)
  for review-state changes (Checked / Pending / Irrelevant buttons) to sync
  across all users in real time. If port 8502 is blocked, buttons still work
  locally per browser using localStorage fallback.
"""

import os
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as v1

import mopsov

# ── Start shared review-state API server (port 8502) ──────────────────────────
mopsov.start_api_server()


# ── Locate report.html ─────────────────────────────────────────────────────────
def find_report_html() -> Path:
    """Search for report.html relative to this file and the working directory."""
    candidates = []
    try:
        app_dir = Path(__file__).resolve().parent
        candidates.append(app_dir / "output" / "report.html")
        candidates.append(app_dir.parent / "output" / "report.html")
    except NameError:
        pass
    cwd = Path.cwd().resolve()
    candidates.append(cwd / "output" / "report.html")
    candidates.append(cwd.parent / "output" / "report.html")
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(
        "report.html not found. Run `python mopsov.py` first to generate it.\n"
        "Searched:\n" + "\n".join(str(p) for p in candidates)
    )


# ── Streamlit page ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="MOPSOV Monitor", layout="wide")

try:
    html_path = find_report_html()
    html_data = html_path.read_text(encoding="utf-8")
    v1.html(html_data, height=1500, scrolling=True)
except FileNotFoundError as e:
    st.error(str(e))
