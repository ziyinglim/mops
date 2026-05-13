"""
MOPS Monitor — Streamlit Dashboard
Reads from mopsov.py archive/state files. Review state stored in SQLite.

Launch (local only):
    streamlit run app.py

Share on local network:
    streamlit run app.py --server.address 0.0.0.0 --server.port 8501
    → share http://<your-ip>:8501 with teammates
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from mopsov import (
    WATCHLIST,
    _build_fc_internal_notes,
    _build_pm_internal_notes,
)

# ── Config ────────────────────────────────────────────────────────────────────

STATE_DIR   = Path("storage/state")
ARCHIVE_DIR = Path("storage/archive")
REVIEW_DB   = Path("storage/state/review_state.db")

ALL_CODES     = [e["stock_code"] for e in WATCHLIST]
COMPANY_NAMES = {e["stock_code"]: e["name_en"] for e in WATCHLIST}
STATUS_OPTS   = ["Pending", "Checked", "Irrelevant"]
STATUS_ICON   = {"Pending": "🟡", "Checked": "🟢", "Irrelevant": "⚫"}

# ── SQLite review state ───────────────────────────────────────────────────────

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(REVIEW_DB), check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            key         TEXT PRIMARY KEY,
            status      TEXT DEFAULT 'Pending',
            reviewer    TEXT DEFAULT '',
            updated_at  TEXT DEFAULT ''
        )
    """)
    conn.commit()
    return conn

def load_reviews() -> dict[str, dict]:
    with _db() as conn:
        rows = conn.execute(
            "SELECT key, status, reviewer, updated_at FROM reviews"
        ).fetchall()
    return {r[0]: {"status": r[1], "reviewer": r[2], "updated_at": r[3]} for r in rows}

def save_reviews(changes: list[tuple[str, str, str]]) -> None:
    """Bulk-save a list of (key, status, reviewer) tuples."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    with _db() as conn:
        conn.executemany(
            "INSERT OR REPLACE INTO reviews (key, status, reviewer, updated_at) VALUES (?,?,?,?)",
            [(k, s, r, ts) for k, s, r in changes],
        )

# ── Review key helpers ────────────────────────────────────────────────────────

def rk_fc(r: dict) -> str:
    fund = (r.get("fund_name") or "")[:40].replace("|", "")
    return f"fc|{r['stock_code']}|{r.get('commitment_date','')}|{fund}"

def rk_pm(r: dict) -> str:
    person = (r.get("new_holder") or r.get("previous_holder") or "")[:20]
    return f"pm|{r['stock_code']}|{r.get('announcement_date','')}|{person}"

def rk_fs(r: dict) -> str:
    return f"fs|{r.get('stock_code','')}|{r.get('subsidiary_code','')}|{r.get('period','')}"

# ── Data loading ──────────────────────────────────────────────────────────────

def _latest_archive(pattern: str) -> list[dict]:
    records = []
    for code in ALL_CODES:
        files = sorted(ARCHIVE_DIR.glob(pattern.format(code=code)), reverse=True)
        if files:
            try:
                d = json.loads(files[0].read_text(encoding="utf-8"))
                recs = d.get("records", d) if isinstance(d, dict) else d
                if isinstance(recs, list):
                    records.extend(recs)
            except Exception:
                pass
    return records

@st.cache_data(ttl=60)
def load_funds() -> list[dict]:
    recs = _latest_archive("{code}_fund_commitments_*.json")
    for r in recs:
        r["_company"] = COMPANY_NAMES.get(r.get("stock_code", ""), r.get("stock_code", ""))
        if "internal_notes" not in r:
            r["internal_notes"] = _build_fc_internal_notes(r) if r.get("status") == "NEW" else ""
    return sorted(recs, key=lambda r: r.get("announcement_date", ""), reverse=True)

@st.cache_data(ttl=60)
def load_people() -> list[dict]:
    recs = _latest_archive("{code}_people_moves_*.json")
    for r in recs:
        r["_company"] = COMPANY_NAMES.get(r.get("stock_code", ""), r.get("stock_code", ""))
        if "internal_notes" not in r:
            r["internal_notes"] = _build_pm_internal_notes(r) if r.get("status") == "NEW" else ""
    return sorted(recs, key=lambda r: r.get("announcement_date", ""), reverse=True)

@st.cache_data(ttl=60)
def load_fs() -> list[dict]:
    records = []
    for code in ALL_CODES:
        p = STATE_DIR / f"{code}_balance_history.json"
        if p.exists():
            try:
                records.extend(json.loads(p.read_text(encoding="utf-8")))
            except Exception:
                pass
    for r in records:
        r["_company"] = COMPANY_NAMES.get(r.get("stock_code", ""), r.get("stock_code", ""))
    return sorted(records, key=lambda r: (r.get("roc_year", 0), r.get("season", 0)), reverse=True)

@st.cache_data(ttl=60)
def load_em() -> list[dict]:
    records = []
    for code in ALL_CODES:
        pfiles = sorted(ARCHIVE_DIR.glob(f"{code}_profile_*.json"), reverse=True)
        bfiles = sorted(ARCHIVE_DIR.glob(f"{code}_balance_sheet_*.json"), reverse=True)
        profile, bs = {}, {}
        if pfiles:
            d = json.loads(pfiles[0].read_text(encoding="utf-8"))
            recs = d.get("records", d) if isinstance(d, dict) else d
            if isinstance(recs, list) and recs:
                profile = recs[0]
        if bfiles:
            d = json.loads(bfiles[0].read_text(encoding="utf-8"))
            recs = d.get("records", d) if isinstance(d, dict) else d
            if isinstance(recs, list) and recs:
                bs = recs[0]
        if profile:
            merged = {**profile, **{k: v for k, v in bs.items() if k not in profile}}
            merged["_company"] = COMPANY_NAMES.get(code, code)
            records.append(merged)
    return records

# ── Shared filter helpers ─────────────────────────────────────────────────────

def _company_filter(key: str) -> str:
    return st.selectbox(
        "Company", ["All"] + sorted(COMPANY_NAMES.values()), key=key
    )

def _status_filter(key: str) -> str:
    return st.selectbox("Status", ["All"] + STATUS_OPTS, key=key)

# ── FC tab ────────────────────────────────────────────────────────────────────

def tab_fc() -> None:
    records = load_funds()
    reviews = load_reviews()

    c1, c2, c3, c4 = st.columns([2, 1, 1, 2])
    co  = c1.selectbox("Company", ["All"] + sorted(COMPANY_NAMES.values()), key="fc_co")
    st_ = c2.selectbox("Status",  ["All"] + STATUS_OPTS, key="fc_st")
    yr  = c3.text_input("Year", placeholder="2025", key="fc_yr")
    q   = c4.text_input("Search", placeholder="fund name / type…", key="fc_q")

    filtered = []
    for r in records:
        if co  != "All" and r["_company"] != co: continue
        rv = reviews.get(rk_fc(r), {})
        if st_ != "All" and rv.get("status", "Pending") != st_: continue
        if yr  and not (r.get("announcement_date") or "").startswith(yr): continue
        if q   and q.lower() not in (
            (r.get("fund_name","") + r.get("fund_type","") + r.get("headline","")).lower()
        ): continue
        filtered.append(r)

    st.caption(f"{len(filtered)} records")
    if not filtered:
        st.info("No records match the current filters.")
        return

    # Build editable dataframe
    rows = []
    for r in filtered:
        key = rk_fc(r)
        rv  = reviews.get(key, {})
        rows.append({
            "_key":      key,
            "Code":      r.get("stock_code", ""),
            "Company":   r["_company"],
            "Ann. Date": r.get("announcement_date", ""),
            "Fund":      r.get("fund_name", ""),
            "Type":      r.get("fund_type", ""),
            "Amount":    r.get("commitment_amount_raw", ""),
            "Commit Date": r.get("commitment_date", ""),
            "Status":    rv.get("status", "Pending"),
            "Reviewer":  rv.get("reviewer", ""),
            "Updated":   rv.get("updated_at", ""),
        })

    original_df = pd.DataFrame(rows)
    edited_df = st.data_editor(
        original_df.drop(columns=["_key"]),
        column_config={
            "Status":   st.column_config.SelectboxColumn("Status", options=STATUS_OPTS, width="small"),
            "Reviewer": st.column_config.TextColumn("Reviewer", width="small"),
            "Updated":  st.column_config.TextColumn("Updated", disabled=True, width="small"),
        },
        disabled=["Code", "Company", "Ann. Date", "Fund", "Type", "Amount", "Commit Date", "Updated"],
        hide_index=True,
        use_container_width=True,
        height=min(400, 45 + len(rows) * 35),
        key="fc_editor",
    )

    # Detect and save changes
    changes = []
    for i in range(len(original_df)):
        old_st = original_df.at[i, "Status"]
        old_rv = original_df.at[i, "Reviewer"]
        new_st = edited_df.at[i, "Status"]
        new_rv = edited_df.at[i, "Reviewer"]
        if old_st != new_st or old_rv != new_rv:
            changes.append((original_df.at[i, "_key"], new_st, new_rv))
    if changes:
        save_reviews(changes)
        st.toast(f"Saved {len(changes)} change(s)", icon="✅")
        st.rerun()

    # Internal notes (expandable, grouped by fund)
    notes_records = [r for r in filtered if r.get("internal_notes")]
    if notes_records:
        with st.expander(f"📋 Internal notes ({len(notes_records)} records)"):
            for r in notes_records:
                st.markdown(f"**{r['_company']} — {r.get('fund_name','—')}**")
                st.code(r["internal_notes"], language=None)

# ── PM tab ────────────────────────────────────────────────────────────────────

def tab_pm() -> None:
    records = load_people()
    reviews = load_reviews()

    c1, c2, c3, c4 = st.columns([2, 1, 1, 2])
    co  = c1.selectbox("Company", ["All"] + sorted(COMPANY_NAMES.values()), key="pm_co")
    st_ = c2.selectbox("Status",  ["All"] + STATUS_OPTS, key="pm_st")
    yr  = c3.text_input("Year", placeholder="2025", key="pm_yr")
    q   = c4.text_input("Search", placeholder="name / role…", key="pm_q")

    filtered = []
    for r in records:
        if co  != "All" and r["_company"] != co: continue
        rv = reviews.get(rk_pm(r), {})
        if st_ != "All" and rv.get("status", "Pending") != st_: continue
        if yr  and not (r.get("announcement_date") or "").startswith(yr): continue
        if q   and q.lower() not in (
            (r.get("new_holder","") + r.get("previous_holder","")
             + r.get("role_title","") + r.get("narrative_en","")).lower()
        ): continue
        filtered.append(r)

    st.caption(f"{len(filtered)} records")
    if not filtered:
        st.info("No records match the current filters.")
        return

    rows = []
    for r in filtered:
        key = rk_pm(r)
        rv  = reviews.get(key, {})
        headline = (r.get("narrative_en") or "").split("\n\n")[0]
        rows.append({
            "_key":       key,
            "Code":       r.get("stock_code", ""),
            "Company":    r["_company"],
            "Ann. Date":  r.get("announcement_date", ""),
            "Role":       r.get("role_title", ""),
            "New Holder": r.get("new_holder", ""),
            "Prev Holder": r.get("previous_holder", ""),
            "Effective":  r.get("effective_date", ""),
            "Headline":   headline,
            "Status":     rv.get("status", "Pending"),
            "Reviewer":   rv.get("reviewer", ""),
            "Updated":    rv.get("updated_at", ""),
        })

    original_df = pd.DataFrame(rows)
    edited_df = st.data_editor(
        original_df.drop(columns=["_key"]),
        column_config={
            "Status":   st.column_config.SelectboxColumn("Status", options=STATUS_OPTS, width="small"),
            "Reviewer": st.column_config.TextColumn("Reviewer", width="small"),
            "Headline": st.column_config.TextColumn("Headline", width="large"),
            "Updated":  st.column_config.TextColumn("Updated", disabled=True, width="small"),
        },
        disabled=["Code", "Company", "Ann. Date", "Role", "New Holder", "Prev Holder",
                  "Effective", "Headline", "Updated"],
        hide_index=True,
        use_container_width=True,
        height=min(400, 45 + len(rows) * 35),
        key="pm_editor",
    )

    changes = []
    for i in range(len(original_df)):
        old_st = original_df.at[i, "Status"]
        old_rv = original_df.at[i, "Reviewer"]
        new_st = edited_df.at[i, "Status"]
        new_rv = edited_df.at[i, "Reviewer"]
        if old_st != new_st or old_rv != new_rv:
            changes.append((original_df.at[i, "_key"], new_st, new_rv))
    if changes:
        save_reviews(changes)
        st.toast(f"Saved {len(changes)} change(s)", icon="✅")
        st.rerun()

    notes_records = [r for r in filtered if r.get("internal_notes")]
    if notes_records:
        with st.expander(f"📋 Internal notes ({len(notes_records)} records)"):
            for r in notes_records:
                label = f"{r['_company']} — {r.get('role_title','—')} ({r.get('announcement_date','')})"
                st.markdown(f"**{label}**")
                st.code(r["internal_notes"], language=None)

# ── FS tab ────────────────────────────────────────────────────────────────────

def tab_fs() -> None:
    records = load_fs()
    reviews = load_reviews()

    c1, c2, c3 = st.columns([2, 2, 2])
    co  = c1.selectbox("Company", ["All"] + sorted(COMPANY_NAMES.values()), key="fs_co")
    per = c2.text_input("Period", placeholder="2025/Q3", key="fs_per")
    st_ = c3.selectbox("Status", ["All"] + STATUS_OPTS, key="fs_st")

    filtered = []
    for r in records:
        if co  != "All" and r["_company"] != co: continue
        if per and per.lower() not in (r.get("period") or "").lower(): continue
        rv = reviews.get(rk_fs(r), {})
        if st_ != "All" and rv.get("status", "Pending") != st_: continue
        filtered.append(r)

    st.caption(f"{len(filtered)} records")
    if not filtered:
        st.info("No records match the current filters.")
        return

    rows = []
    for r in filtered:
        key = rk_fs(r)
        rv  = reviews.get(key, {})
        source = ("PDFs - Unconsolidated" if r.get("is_consolidated") is False
                  else "PDFs - Consolidated" if r.get("is_consolidated") else "Balance Sheet")
        rows.append({
            "_key":         key,
            "Code":         r.get("stock_code", ""),
            "Company":      r["_company"],
            "Subsidiary":   r.get("subsidiary_name_en", ""),
            "Period":       r.get("period", ""),
            "Source":       source,
            "Total Assets": r.get("total_assets_raw", "—"),
            "Inv. Property": r.get("investment_property_raw", "—"),
            "Scraped":      (r.get("scraped_at") or "")[:10],
            "Status":       rv.get("status", "Pending"),
            "Reviewer":     rv.get("reviewer", ""),
            "Updated":      rv.get("updated_at", ""),
        })

    original_df = pd.DataFrame(rows)
    edited_df = st.data_editor(
        original_df.drop(columns=["_key"]),
        column_config={
            "Status":   st.column_config.SelectboxColumn("Status", options=STATUS_OPTS, width="small"),
            "Reviewer": st.column_config.TextColumn("Reviewer", width="small"),
            "Updated":  st.column_config.TextColumn("Updated", disabled=True, width="small"),
        },
        disabled=["Code", "Company", "Subsidiary", "Period", "Source",
                  "Total Assets", "Inv. Property", "Scraped", "Updated"],
        hide_index=True,
        use_container_width=True,
        height=min(450, 45 + len(rows) * 35),
        key="fs_editor",
    )

    changes = []
    for i in range(len(original_df)):
        if (original_df.at[i, "Status"]   != edited_df.at[i, "Status"] or
                original_df.at[i, "Reviewer"] != edited_df.at[i, "Reviewer"]):
            changes.append((original_df.at[i, "_key"],
                            edited_df.at[i, "Status"],
                            edited_df.at[i, "Reviewer"]))
    if changes:
        save_reviews(changes)
        st.toast(f"Saved {len(changes)} change(s)", icon="✅")
        st.rerun()

# ── EM tab ────────────────────────────────────────────────────────────────────

def tab_em() -> None:
    records = load_em()

    co = st.selectbox("Company", ["All"] + sorted(COMPANY_NAMES.values()), key="em_co")
    filtered = [r for r in records if co == "All" or r["_company"] == co]

    if not filtered:
        st.info("No records found.")
        return

    rows = [{
        "Code":            r.get("stock_code", ""),
        "Company":         r.get("company_name_en", r["_company"]),
        "Chairman":        r.get("chairman", "—"),
        "General Manager": r.get("general_manager", "—"),
        "Tel":             r.get("telephone", "—"),
        "Web":             r.get("web_address", "—"),
        "Address":         r.get("address", "—"),
        "Total Assets":    r.get("total_assets_raw", "—"),
        "Period":          r.get("period", "—"),
        "Scraped":         (r.get("scraped_at") or "")[:10],
    } for r in filtered]

    st.dataframe(
        pd.DataFrame(rows),
        use_container_width=True,
        hide_index=True,
        height=min(500, 45 + len(rows) * 35),
    )

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(
        page_title="MOPS Monitor",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.title("📊 MOPS Monitor")

    hdr_l, hdr_r = st.columns([5, 1])
    hdr_l.caption(
        "Review states save automatically when you edit Status / Reviewer. "
        "Data refreshes every 60s — or click Refresh to force it."
    )
    if hdr_r.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    tab_labels = ["📦 Fund Commitments", "👤 People Moves",
                  "📄 Financial Statements", "🏢 Company Profiles"]
    t_fc, t_pm, t_fs, t_em = st.tabs(tab_labels)

    with t_fc:
        tab_fc()
    with t_pm:
        tab_pm()
    with t_fs:
        tab_fs()
    with t_em:
        tab_em()


if __name__ == "__main__":
    main()
