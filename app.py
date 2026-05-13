"""
MOPS Monitor — Streamlit Dashboard
Tables rendered via st.components.v1.html() — complete iframe, no CSS conflicts.
Review state stored in browser localStorage (matches HTML report behaviour).
Launch: streamlit run app.py --server.address 0.0.0.0 --server.port 8501 --server.headless true
"""

import base64
import html as html_lib
import json
import re
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as v1

from mopsov import (
    WATCHLIST, _SUBSIDIARY_STUBS,
    _build_fc_internal_notes, _build_pm_internal_notes,
    _clean_company_name, _clean_address, _format_tw_phone, _clean_web_address,
    _build_fs_data,
)

# ── Constants ─────────────────────────────────────────────────────────────────

STATE_DIR   = Path("storage/state")
ARCHIVE_DIR = Path("storage/archive")

ALL_CODES     = [e["stock_code"] for e in WATCHLIST]
COMPANY_NAMES = {e["stock_code"]: e["name_en"] for e in WATCHLIST}
COMPANY_NAMES.update({s["stock_code"]: s["company_name_en"] for s in _SUBSIDIARY_STUBS})

_FIRM_TYPE_DISPLAY = {
    "financial holding company": "Financial Holding",
    "insurance company":         "Insurance",
    "commercial bank":           "Commercial Bank",
    "state-owned bank":          "State-owned Bank",
    "technology company":        "Technology",
    "semiconductor company":     "Semiconductor",
}
FIRM_TYPE_BY_CODE = {
    e["stock_code"]: _FIRM_TYPE_DISPLAY.get(e.get("company_type", ""), "")
    for e in WATCHLIST
}
# Infer subsidiary firm types from name keywords
def _infer_firm_type(name: str) -> str:
    n = (name or "").lower()
    if "life insurance" in n or "insurance" in n:
        return "Insurance"
    if "bank" in n:
        return "Commercial Bank"
    return ""

for _s in _SUBSIDIARY_STUBS:
    if _s["stock_code"] not in FIRM_TYPE_BY_CODE:
        FIRM_TYPE_BY_CODE[_s["stock_code"]] = _infer_firm_type(_s["company_name_en"])

# ── Formatting helpers ────────────────────────────────────────────────────────

def _fmt_date(d: str) -> str:
    if not d:
        return "—"
    d = str(d).strip().replace("-", "/")
    return d if re.match(r'^\d{4}/\d{2}/\d{2}$', d) else (d or "—")

def _scraped(s: str) -> str:
    return _fmt_date((s or "")[:10])

def _fmt_tel(tel: str) -> str:
    """Ensure consistent +886-X-XXXX-XXXX format with 3 dashes."""
    t = _format_tw_phone(tel or "")
    if not t:
        return "—"
    # +886-area-XXXXXXXX → split local number 4+4 (or 4+3 for 7-digit)
    m = re.match(r'^(\+886-\d{1,2}-)(\d{7,8})$', t)
    if m:
        prefix, digits = m.group(1), m.group(2)
        if len(digits) == 8:
            return f'{prefix}{digits[:4]}-{digits[4:]}'
        if len(digits) == 7:
            return f'{prefix}{digits[:3]}-{digits[3:]}'
    return t

def _fmt_addr(addr: str) -> str:
    """Clean address: strip mojibake, add spaces after commas, fix merged city names."""
    a = _clean_address(addr or "")
    if not a:
        return "—"
    # Space after every comma where missing
    a = re.sub(r',(\S)', r', \1', a)
    # Comma + space before "Taipei" when directly appended (e.g. RoadTaipei → Road, Taipei)
    a = re.sub(r'([^\s,])(Taipei\b)', r'\1, \2', a)
    # Space between a letter and a 3-or-more-digit run (e.g. City104 → City 104)
    a = re.sub(r'([A-Za-z])(\d{3,})', r'\1 \2', a)
    # Remove space(s) immediately before a comma
    a = re.sub(r'\s+,', ',', a)
    # Collapse double commas and multiple spaces
    a = re.sub(r',\s*,', ', ', a)
    a = re.sub(r'\s{2,}', ' ', a).strip().rstrip(',').strip()
    return a

# ── Table CSS (embedded in every iframe document) ─────────────────────────────

_TABLE_CSS = """
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Segoe UI', Arial, sans-serif; font-size: .81rem;
       color: #1a1a2e; background: #fff; }
.tbl-wrap { overflow-x: auto; width: 100%; }
table { border-collapse: collapse; white-space: nowrap; background: #fff; width: 100%; }
th { background: #FF6633; color: #fff; padding: 9px 12px; text-align: left;
     font-weight: 600; cursor: pointer; user-select: none; white-space: nowrap; }
th:hover { background: #e85520; }
th::after { content: ' ⇅'; opacity: .35; font-size: .7em; }
th[data-sort=asc]::after  { content: ' ▲'; opacity: 1; }
th[data-sort=desc]::after { content: ' ▼'; opacity: 1; }
td { padding: 8px 12px; border-bottom: 1px solid #f0f0f0; vertical-align: top;
     color: #1a1a2e; }
td.w  { white-space: normal; min-width: 140px; max-width: 280px; word-break: break-word; }
td.amt{ white-space: normal; min-width: 120px; max-width: 200px; word-break: break-word; }
td.hl { white-space: normal; max-width: 340px; }
td.ft { font-size: .75rem; color: #666; white-space: nowrap; }
tr:last-child td { border-bottom: none; }
tr:hover td { background: rgba(0,0,0,.02); }
tr.row-new td { background: #FFE0B2; }
tr.row-changed td { background: #fffbf0; }
.hl-txt strong { font-weight: 700; color: #1a1a2e; }
.hl-txt em { color: #555; font-style: italic; }
a { color: #FF6633; text-decoration: none; }
a:hover { text-decoration: underline; }
details > summary { cursor: pointer; color: #FF6633; font-size: .73rem;
                    font-weight: 600; list-style: none; }
details > summary::before { content: '+ '; }
details[open] > summary::before { content: '- '; }
details > pre { font-size: .71rem; color: #1a1a2e; white-space: pre-wrap;
                margin-top: 4px; max-width: 300px; background: #f9f9f9;
                padding: 6px; border-radius: 4px; }
.copy-btn { padding: 2px 7px; border: 1px solid #ccc; border-radius: 3px;
            font-size: .68rem; cursor: pointer; background: #f5f5f5; color: #555;
            display: block; margin-bottom: 3px; }
.copy-btn:hover { background: #e8e8e8; }
.copy-btn.copied { background: #C6EFCE; color: #276221; }
.chk-wrap { display: flex; flex-direction: column; gap: 3px; min-width: 108px; }
.chk-btn  { padding: 4px 8px; border: none; border-radius: 4px; font-size: .74rem;
            font-weight: 700; cursor: pointer; width: 100%; text-align: center; }
.chk-pend  { background: #FFD700; color: #5a4000; }
.chk-done  { background: #C6EFCE; color: #276221; }
.chk-irrel { background: #D3D3D3; color: #555; }
.chk-ts   { font-size: .64rem; color: #888; }
.chk-name { width: 100%; padding: 3px 5px; border: 1px solid #ddd; border-radius: 3px;
            font-size: .71rem; color: #1a1a2e; background: #fff; }
.chk-cell { display: inline-block; }
.mcheck { color: #e67e22; font-weight: 600; cursor: help;
          border-bottom: 1px dotted #e67e22; position: relative; }
.mcheck::after {
  content: attr(data-tip); position: absolute; bottom: 130%; left: 50%;
  transform: translateX(-50%); background: #333; color: #fff;
  padding: 5px 10px; border-radius: 4px; font-size: .71rem;
  white-space: nowrap; pointer-events: none; opacity: 0;
  transition: opacity .15s; z-index: 999;
}
.mcheck:hover::after { opacity: 1; }
</style>
"""

# localStorage-based review JS — exact match of HTML report behaviour
_TABLE_JS = r"""
<script>
function chkGet(k){try{return JSON.parse(localStorage.getItem(k)||'null');}catch(e){return null;}}
function chkSet(k,st,nm){localStorage.setItem(k,JSON.stringify({state:st,name:nm,ts:new Date().toISOString()}));}
function fmtTs(iso){
  if(!iso)return '';
  try{var d=new Date(iso);
    return d.toLocaleDateString('en-GB',{day:'2-digit',month:'short',year:'numeric'})+
           ' '+d.toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit'});}
  catch(e){return '';}
}
function copyNotes(btn){
  try{
    var text=decodeURIComponent(escape(atob(btn.dataset.n||'')));
    (navigator.clipboard?navigator.clipboard.writeText(text):Promise.reject())
      .catch(function(){
        var ta=document.createElement('textarea');ta.value=text;
        ta.style.cssText='position:fixed;opacity:0';
        document.body.appendChild(ta);ta.select();
        document.execCommand('copy');document.body.removeChild(ta);
      });
    btn.textContent='Copied!';btn.classList.add('copied');
    setTimeout(function(){btn.textContent='Copy notes';btn.classList.remove('copied');},2000);
  }catch(e){}
}
function toggleCheck(btn,key){
  var inp=btn.closest('.chk-wrap').querySelector('.chk-name');
  if(!inp.value.trim()){inp.style.outline='2px solid #e74c3c';inp.focus();return;}
  inp.style.outline='';
  var d=chkGet(key)||{};
  var cycle=['pending','checked','irrelevant'];
  var next=cycle[(cycle.indexOf(d.state||'pending')+1)%3];
  chkSet(key,next,inp.value.trim());
  var lbl={pending:'Pending',checked:'Checked ✓',irrelevant:'Irrelevant'};
  var cls={pending:'chk-pend',checked:'chk-done',irrelevant:'chk-irrel'};
  btn.textContent=lbl[next];
  btn.className='chk-btn '+(cls[next]||'chk-pend');
  btn.nextElementSibling.textContent=fmtTs(new Date().toISOString());
}
function saveName(inp,key){var d=chkGet(key)||{state:'pending'};chkSet(key,d.state,inp.value);}
function buildChkBtn(key){
  var d=chkGet(key)||{};var st=d.state||'pending';
  var lbl={pending:'Pending',checked:'Checked ✓',irrelevant:'Irrelevant'};
  var cls={pending:'chk-pend',checked:'chk-done',irrelevant:'chk-irrel'};
  var safe=key.replace(/\\/g,'\\\\').replace(/'/g,"\\'");
  var wrap=document.createElement('span');wrap.className='chk-wrap';
  var btn=document.createElement('button');
  btn.className='chk-btn '+(cls[st]||'chk-pend');
  btn.textContent=lbl[st]||'Pending';
  btn.setAttribute('onclick',"toggleCheck(this,'"+safe+"')");
  var ts=document.createElement('div');ts.className='chk-ts';ts.textContent=fmtTs(d.ts||'');
  var inp=document.createElement('input');
  inp.className='chk-name';inp.placeholder='Reviewer';inp.value=d.name||'';
  inp.setAttribute('onchange',"saveName(this,'"+safe+"')");
  inp.setAttribute('onclick','event.stopPropagation()');
  wrap.appendChild(btn);wrap.appendChild(ts);wrap.appendChild(inp);
  return wrap;
}
document.addEventListener('DOMContentLoaded',function(){
  document.querySelectorAll('.chk-cell').forEach(function(span){
    span.parentNode.replaceChild(buildChkBtn(span.dataset.key),span);
  });
  var tbl=document.querySelector('table');
  if(!tbl)return;
  var ths=Array.from(tbl.querySelectorAll('thead th'));
  ths.forEach(function(th,idx){
    th.addEventListener('click',function(){
      var asc=th.dataset.sort!=='asc';
      ths.forEach(function(h){h.dataset.sort='';});
      th.dataset.sort=asc?'asc':'desc';
      var rows=Array.from(tbl.tBodies[0].rows);
      rows.sort(function(a,b){
        var av=(a.cells[idx]?a.cells[idx].textContent:'').trim();
        var bv=(b.cells[idx]?b.cells[idx].textContent:'').trim();
        var n1=parseFloat(av.replace(/[^0-9.-]/g,''));
        var n2=parseFloat(bv.replace(/[^0-9.-]/g,''));
        if(!isNaN(n1)&&!isNaN(n2))return asc?n1-n2:n2-n1;
        return asc?av.localeCompare(bv):bv.localeCompare(av);
      });
      rows.forEach(function(r){tbl.tBodies[0].appendChild(r);});
    });
  });
});
</script>
"""

def _make_iframe_html(table_html: str) -> str:
    return (
        "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
        f"{_TABLE_CSS}</head><body>"
        f"<div class='tbl-wrap'>{table_html}</div>"
        f"{_TABLE_JS}</body></html>"
    )

def _render_table(table_html: str, n_rows: int) -> None:
    height = min(max(250, 80 + n_rows * 58), 820)
    v1.html(_make_iframe_html(table_html), height=height, scrolling=True)

# ── Review key helpers ────────────────────────────────────────────────────────

def rk_fc(r):  return f"fc|{r['stock_code']}|{r.get('commitment_date','')}|{(r.get('fund_name','') or '')[:40]}"
def rk_pm(r):  return f"pm|{r['stock_code']}|{r.get('announcement_date','')}|{(r.get('new_holder','') or r.get('previous_holder','') or '')[:20]}"
def rk_fs(r):  return f"fs|{r.get('stock_code','')}|{r.get('name_en','')}|{r.get('period','')}"
def rk_em(r):  return f"em|{r.get('stock_code','')}"

# ── Data loading ──────────────────────────────────────────────────────────────

def _latest_archive(pattern: str) -> list[dict]:
    records = []
    for code in ALL_CODES:
        files = sorted(ARCHIVE_DIR.glob(pattern.format(code=code)), reverse=True)
        if files:
            try:
                d    = json.loads(files[0].read_text(encoding="utf-8"))
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
        r["_company"] = _clean_company_name(COMPANY_NAMES.get(r.get("stock_code", ""), r.get("stock_code", "")))
        if "internal_notes" not in r:
            r["internal_notes"] = _build_fc_internal_notes(r) if r.get("status") == "NEW" else ""
    return sorted(recs, key=lambda r: r.get("announcement_date", ""), reverse=True)

@st.cache_data(ttl=60)
def load_people() -> list[dict]:
    recs = _latest_archive("{code}_people_moves_*.json")
    for r in recs:
        r["_company"] = _clean_company_name(COMPANY_NAMES.get(r.get("stock_code", ""), r.get("stock_code", "")))
        if "internal_notes" not in r:
            r["internal_notes"] = _build_pm_internal_notes(r) if r.get("status") == "NEW" else ""
    return sorted(recs, key=lambda r: r.get("announcement_date", ""), reverse=True)

@st.cache_data(ttl=60)
def load_fs() -> list[dict]:
    emops_data = []
    for entry in WATCHLIST:
        code = entry["stock_code"]
        files = sorted(ARCHIVE_DIR.glob(f"{code}_balance_sheet_*.json"), reverse=True)[:1]
        for f in files:
            try:
                d    = json.loads(f.read_text(encoding="utf-8"))
                recs = d.get("records", d) if isinstance(d, dict) else d
                for r in (recs or []):
                    r["name_en"] = r.get("name_en") or entry["name_en"]
                    if "investment_property_numeric" in r and "inv_property_numeric" not in r:
                        r["inv_property_numeric"] = r["investment_property_numeric"]
                    emops_data.append(r)
            except Exception:
                pass
    emops_data += list(_SUBSIDIARY_STUBS)

    balance_history = []
    for code in ALL_CODES:
        p = STATE_DIR / f"{code}_balance_history.json"
        if p.exists():
            try:
                balance_history.extend(json.loads(p.read_text(encoding="utf-8")))
            except Exception:
                pass

    return _build_fs_data(emops_data, balance_history)

@st.cache_data(ttl=60)
def load_em() -> list[dict]:
    records = []
    for code in ALL_CODES:
        pfiles = sorted(ARCHIVE_DIR.glob(f"{code}_profile_*.json"),       reverse=True)[:1]
        bfiles = sorted(ARCHIVE_DIR.glob(f"{code}_balance_sheet_*.json"), reverse=True)[:1]
        profile, bs = {}, {}
        if pfiles:
            d    = json.loads(pfiles[0].read_text(encoding="utf-8"))
            recs = d.get("records", d) if isinstance(d, dict) else d
            if isinstance(recs, list) and recs:
                profile = recs[0]
        if bfiles:
            d    = json.loads(bfiles[0].read_text(encoding="utf-8"))
            recs = d.get("records", d) if isinstance(d, dict) else d
            if isinstance(recs, list) and recs:
                bs = recs[0]
        if profile:
            merged = {**profile, **{k: v for k, v in bs.items() if k not in profile}}
            merged["_company"] = _clean_company_name(COMPANY_NAMES.get(code, code))
            records.append(merged)
    for s in _SUBSIDIARY_STUBS:
        records.append({
            "stock_code":     s["stock_code"],
            "company_name_en": _clean_company_name(s["company_name_en"]),
            "no_filing_data": True,
            "_company":       _clean_company_name(s["company_name_en"]),
        })
    return records

# ── HTML helpers ──────────────────────────────────────────────────────────────

def _h(v) -> str:
    return html_lib.escape(str(v or ""))

def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")

def _headline(hl: str) -> str:
    if not hl:
        return ""
    parts = hl.split("\n\n")
    if len(parts) < 2:
        return f'<span class="hl-txt">{_h(hl)}</span>'
    return (f'<span class="hl-txt"><strong>{_h(parts[0])}</strong>'
            f'<br><em>{_h(" ".join(parts[1:]))}</em></span>')

def _notes_cell(text: str) -> str:
    if not text:
        return "—"
    encoded = _b64(text)
    return (
        f'<button class="copy-btn" data-n="{encoded}" onclick="copyNotes(this)">Copy notes</button>'
        f'<details><summary>Internal notes</summary><pre>{_h(text)}</pre></details>'
    )

def _chk_btn(key: str) -> str:
    return f'<span class="chk-cell" data-key="{_h(key)}"></span>'

def _firm_td(stock_code: str) -> str:
    """Returns Firm Type <td> for a given stock code."""
    return f'<td class="ft">{_h(FIRM_TYPE_BY_CODE.get(stock_code, ""))}</td>'

# ── Table builders ────────────────────────────────────────────────────────────

def _fc_table(records: list) -> str:
    head = ("<tr>"
            "<th>Stock Code</th><th>Firm ID</th><th>Firm Name</th><th>Firm Type</th>"
            "<th>Published Date</th><th>Fund Name</th><th>Fund Type</th>"
            "<th>Commit Date</th><th>Amount</th>"
            "<th>Headlines</th><th>Key Events</th>"
            "<th>AUM as of</th><th>Scraped At</th><th>Internal Notes</th><th>Action</th>"
            "</tr>")
    rows = []
    for r in records:
        rc   = {"NEW": "row-new", "CHANGED": "row-changed"}.get(r.get("status", ""), "")
        fund = (f'<a href="{_h(r["url"])}" target="_blank">{_h(r.get("fund_name",""))}</a>'
                if r.get("url") else _h(r.get("fund_name", "") or "—"))
        amt  = (f'<a href="{_h(r["fx_url"])}" target="_blank">{_h(r.get("commitment_amount_raw",""))}</a>'
                if r.get("fx_url") else _h(r.get("commitment_amount_raw", "") or "—"))
        sc   = r.get("stock_code", "")
        rows.append(
            f'<tr class="{rc}">'
            f'<td>{_h(sc)}</td>'
            f'<td></td>'
            f'<td class="w">{_h(r.get("_company",""))}</td>'
            f'{_firm_td(sc)}'
            f'<td>{_fmt_date(r.get("announcement_date",""))}</td>'
            f'<td class="w">{fund}</td>'
            f'<td>{_h(r.get("fund_type","") or "—")}</td>'
            f'<td>{_fmt_date(r.get("commitment_date",""))}</td>'
            f'<td class="amt">{amt}</td>'
            f'<td class="hl">{_headline(r.get("headline","") or "")}</td>'
            f'<td class="w">{_h(r.get("key_events","") or "—")}</td>'
            f'<td>{_fmt_date(r.get("bs_date",""))}</td>'
            f'<td>{_scraped(r.get("scraped_at",""))}</td>'
            f'<td>{_notes_cell(r.get("internal_notes",""))}</td>'
            f'<td>{_chk_btn(rk_fc(r))}</td>'
            f'</tr>'
        )
    return f'<table><thead>{head}</thead><tbody>{"".join(rows)}</tbody></table>'

def _pm_table(records: list) -> str:
    # Link is embedded in the Role column as a hyperlink (no separate Link column)
    head = ("<tr>"
            "<th>Stock Code</th><th>Firm ID</th><th>Firm Name</th><th>Firm Type</th>"
            "<th>Published Date</th><th>Role</th><th>New Holder</th><th>Prev Holder</th>"
            "<th>Effective Date</th><th>Headlines</th><th>Key Events</th>"
            "<th>AUM as of</th><th>Scraped At</th>"
            "<th>Internal Notes</th><th>Action</th>"
            "</tr>")
    rows = []
    for r in records:
        rc       = {"NEW": "row-new", "CHANGED": "row-changed"}.get(r.get("status", ""), "")
        role_raw = r.get("role_title","") or r.get("role_type","") or "—"
        role     = (f'<a href="{_h(r["url"])}" target="_blank">{_h(role_raw)}</a>'
                    if r.get("url") else _h(role_raw))
        sc = r.get("stock_code", "")
        rows.append(
            f'<tr class="{rc}">'
            f'<td>{_h(sc)}</td>'
            f'<td></td>'
            f'<td class="w">{_h(r.get("_company",""))}</td>'
            f'{_firm_td(sc)}'
            f'<td>{_fmt_date(r.get("announcement_date",""))}</td>'
            f'<td>{role}</td>'
            f'<td class="w">{_h(r.get("new_holder","") or "—")}</td>'
            f'<td class="w">{_h(r.get("previous_holder","") or "—")}</td>'
            f'<td>{_fmt_date(r.get("effective_date",""))}</td>'
            f'<td class="hl">{_headline(r.get("narrative_en","") or "")}</td>'
            f'<td class="w">{_h(r.get("key_events","") or "—")}</td>'
            f'<td>{_fmt_date(r.get("bs_date",""))}</td>'
            f'<td>{_scraped(r.get("scraped_at",""))}</td>'
            f'<td>{_notes_cell(r.get("internal_notes",""))}</td>'
            f'<td>{_chk_btn(rk_pm(r))}</td>'
            f'</tr>'
        )
    return f'<table><thead>{head}</thead><tbody>{"".join(rows)}</tbody></table>'

def _fs_filing_cell(r: dict) -> str:
    if r.get("source") in ("annual", "mops_ixbrl") and r.get("filing_url"):
        return f'<a href="{_h(r["filing_url"])}" target="_blank">View ↗</a>'
    fname = r.get("pdf_filename", "") or ""
    return _h(fname) if fname else "—"

def _fs_table(records: list) -> str:
    head = ("<tr>"
            "<th>Stock Code</th><th>Firm ID</th><th>Firm Name</th><th>Firm Type</th>"
            "<th>BS Date</th><th>Source</th>"
            "<th>Total Assets (TWD mn)</th><th>Inv. Property (TWD mn)</th>"
            "<th>Filing</th><th>Scraped At</th><th>Internal Notes</th><th>Action</th>"
            "</tr>")
    rows = []
    for r in records:
        sc   = r.get("stock_code", "")
        name = _clean_company_name(r.get("name_en", "") or "")
        src  = ("PDFs - Unconsolidated FS" if r.get("source") == "quarterly"
                else "PDFs - Consolidated FS" if r.get("source") == "quarterly_consolidated"
                else "Balance Sheet"          if r.get("source") == "annual"
                else "MOPS iXBRL"             if r.get("source") == "mops_ixbrl"
                else "—")
        if r.get("stub_only"):
            rows.append(
                f'<tr><td>{_h(sc)}</td><td></td>'
                f'<td class="w">{_h(name)}</td>'
                f'{_firm_td(sc)}'
                f'<td colspan="8" style="color:#888;font-style:italic">'
                f'No financial statement data available</td>'
                f'</tr>'
            )
            continue

        rbg = ' style="background:#fff8f0"' if r.get("extraction_failed") else ""
        if r.get("extraction_failed"):
            ta = '<span class="mcheck" data-tip="PDF might be image-based and is unreadable">Manual check needed</span>'
        else:
            ta = _h(r.get("total_assets_raw", "") or "—")
        ip = _h(r.get("investment_property_raw", "") or "—")

        name_cell = _h(name)
        if r.get("delisted"):
            name_cell += ' <span style="color:#c0392b;font-weight:600">(Delisted)</span>'

        rows.append(
            f'<tr{rbg}>'
            f'<td>{_h(sc)}</td>'
            f'<td></td>'
            f'<td class="w">{name_cell}</td>'
            f'{_firm_td(sc)}'
            f'<td>{_fmt_date(r.get("period",""))}</td>'
            f'<td>{_h(src)}</td>'
            f'<td style="text-align:right">{ta}</td>'
            f'<td style="text-align:right">{ip}</td>'
            f'<td>{_fs_filing_cell(r)}</td>'
            f'<td>{_scraped(r.get("scraped_at",""))}</td>'
            f'<td>{_notes_cell(r.get("internal_notes",""))}</td>'
            f'<td>{_chk_btn(rk_fs(r))}</td>'
            f'</tr>'
        )
    return f'<table><thead>{head}</thead><tbody>{"".join(rows)}</tbody></table>'

def _em_table(records: list) -> str:
    head = ("<tr>"
            "<th>Stock Code</th><th>Firm ID</th><th>Firm Name</th><th>Firm Type</th>"
            "<th>Telephone</th><th>Website</th><th>Address</th>"
            "<th>Scraped At</th><th>Action</th>"
            "</tr>")
    rows = []
    for r in records:
        sc = r.get("stock_code", "")
        if r.get("no_filing_data"):
            rows.append(
                f'<tr><td>{_h(sc)}</td><td></td>'
                f'<td class="w">{_h(r.get("company_name_en",""))}</td>'
                f'{_firm_td(sc)}'
                f'<td colspan="4" style="color:#888;font-style:italic">'
                f'Information not available on filings — please refer to firm website</td>'
                f'<td>{_chk_btn(rk_em(r))}</td></tr>'
            )
            continue
        # Prefer WATCHLIST canonical name for correct casing
        name    = _clean_company_name(
            COMPANY_NAMES.get(sc) or r.get("company_name_en", "") or r.get("_company", "")
        )
        raw_web = _clean_web_address(r.get("web_address", "") or "")
        web_url = raw_web if raw_web.startswith("http") else ("https://" + raw_web if raw_web else "")
        web     = (f'<a href="{_h(web_url)}" target="_blank">{_h(raw_web)}</a>' if web_url else "—")
        tel     = _fmt_tel(r.get("telephone", ""))
        addr    = _fmt_addr(r.get("address", ""))
        rows.append(
            f'<tr>'
            f'<td>{_h(sc)}</td>'
            f'<td></td>'
            f'<td class="w">{_h(name)}</td>'
            f'{_firm_td(sc)}'
            f'<td style="white-space:nowrap">{_h(tel)}</td>'
            f'<td>{web}</td>'
            f'<td class="w">{_h(addr)}</td>'
            f'<td>{_scraped(r.get("scraped_at",""))}</td>'
            f'<td>{_chk_btn(rk_em(r))}</td>'
            f'</tr>'
        )
    return f'<table><thead>{head}</thead><tbody>{"".join(rows)}</tbody></table>'

# ── Tab helpers ───────────────────────────────────────────────────────────────

def _co_opts(names: list[str] | None = None) -> list[str]:
    base = {_clean_company_name(v) for v in COMPANY_NAMES.values()}
    if names:
        base.update(names)
    return ["All"] + sorted(n for n in base if n)

def _date_filter(records: list, field: str, d_from, d_to) -> list[dict]:
    out = []
    for r in records:
        d = (r.get(field, "") or "")[:10]
        if d_from and d and d < str(d_from):
            continue
        if d_to   and d and d > str(d_to):
            continue
        out.append(r)
    return out

def _parse_dr(dr) -> tuple:
    """Parse Streamlit date_input range result → (from, to)."""
    if not dr:
        return None, None
    dates = list(dr)
    return (dates[0] if len(dates) > 0 else None,
            dates[1] if len(dates) > 1 else None)

# ── Tab renderers ─────────────────────────────────────────────────────────────

def tab_fc() -> None:
    records = load_funds()
    c1, c2, c3, c4, _ = st.columns([2, 1.5, 1, 2, 2])
    co = c1.selectbox("Company",   _co_opts(),                       key="fc_co")
    dr = c2.date_input("Date range", value=[],                       key="fc_dr")
    ft = c3.text_input("Fund Type",  placeholder="Filter type…",     key="fc_ft")
    q  = c4.text_input("Search",     placeholder="Fund name / headline…", key="fc_q")
    dfrm, dto = _parse_dr(dr)

    filtered = [
        r for r in records
        if (co == "All" or r["_company"] == co)
        and (not ft or ft.lower() in (r.get("fund_type", "") or "").lower())
        and (not q  or q.lower() in
             (r.get("fund_name","") + r.get("fund_type","") + r.get("headline","")).lower())
    ]
    filtered = _date_filter(filtered, "announcement_date", dfrm, dto)
    st.caption(f"{len(filtered)} records")
    _render_table(_fc_table(filtered), len(filtered))

def tab_pm() -> None:
    records = load_people()
    c1, c2, c3, _ = st.columns([2, 1.5, 2, 2])
    co = c1.selectbox("Company",    _co_opts(), key="pm_co")
    dr = c2.date_input("Date range", value=[],  key="pm_dr")
    q  = c3.text_input("Search",     placeholder="Name / role…", key="pm_q")
    dfrm, dto = _parse_dr(dr)

    filtered = [
        r for r in records
        if (co == "All" or r["_company"] == co)
        and (not q or q.lower() in
             (r.get("new_holder","") + r.get("previous_holder","")
              + r.get("role_title","") + r.get("narrative_en","")).lower())
    ]
    filtered = _date_filter(filtered, "announcement_date", dfrm, dto)
    st.caption(f"{len(filtered)} records")
    _render_table(_pm_table(filtered), len(filtered))

def tab_fs() -> None:
    records  = load_fs()
    fs_names = sorted({
        _clean_company_name(r.get("name_en", ""))
        for r in records if r.get("name_en")
    })
    c1, c2, c3, _ = st.columns([2, 1.5, 2, 2])
    co = c1.selectbox("Entity",      ["All"] + fs_names, key="fs_co")
    dr = c2.date_input("BS Date range", value=[],         key="fs_dr")
    q  = c3.text_input("Search",     placeholder="Entity / period…", key="fs_q")
    dfrm, dto = _parse_dr(dr)

    def _bs_ok(period, d_from, d_to) -> bool:
        pd = _fmt_date(period or "")
        if d_from and pd != "—" and pd < str(d_from):
            return False
        if d_to   and pd != "—" and pd > str(d_to):
            return False
        return True

    filtered = [
        r for r in records
        if (co == "All" or _clean_company_name(r.get("name_en", "")) == co)
        and _bs_ok(r.get("period", ""), dfrm, dto)
        and (not q or q.lower() in
             (_fmt_date(r.get("period","")) + (r.get("name_en","") or "")).lower())
    ]
    st.caption(f"{len(filtered)} records")
    _render_table(_fs_table(filtered), len(filtered))

def tab_em() -> None:
    records = load_em()
    c1, c2, c3, _ = st.columns([2, 1.5, 2, 2])
    co = c1.selectbox("Company",      _co_opts(),  key="em_co")
    dr = c2.date_input("Scraped range", value=[],  key="em_dr")
    q  = c3.text_input("Search",  placeholder="Name / address…", key="em_q")
    dfrm, dto = _parse_dr(dr)

    filtered = [
        r for r in records
        if (co == "All" or r.get("_company","") == co)
        and (not q or q.lower() in
             (r.get("company_name_en","") + r.get("address","") + r.get("telephone","")).lower())
    ]
    filtered = _date_filter(filtered, "scraped_at", dfrm, dto)
    st.caption(f"{len(filtered)} companies")
    _render_table(_em_table(filtered), len(filtered))

# ── Page CSS ──────────────────────────────────────────────────────────────────

def _page_css() -> None:
    st.markdown("""
<style>
[data-testid="stHeader"], [data-testid="stToolbar"], footer { display:none !important; }
[data-testid="stMainBlockContainer"] {
    padding: 0 0 24px 0 !important; max-width: 100% !important;
}
[data-testid="block-container"] {
    padding: 12px 28px !important; max-width: 100% !important;
}
.stApp { background: #f5f6fa !important; }

.mops-hdr { background: #FF6633; color: #fff; padding: 14px 0;
            font-family: 'Segoe UI', sans-serif; }
.mops-hdr h1 { font-size: 1.2rem; font-weight: 700; margin: 0; }

.mops-metrics { display: flex; gap: 14px; padding: 14px 0 10px;
                background: #f5f6fa; font-family: 'Segoe UI', sans-serif; }
.mops-card { background: #fff; border-radius: 8px; padding: 12px 18px; flex: 1;
             box-shadow: 0 1px 4px rgba(0,0,0,.08); }
.mops-card .num { font-size: 1.8rem; font-weight: 700; color: #FF6633; line-height: 1.1; }
.mops-card .lbl { font-size: .76rem; color: #666; margin-top: 3px; }

/* Tabs */
[data-testid="stTabs"] { padding-left: 28px !important; }
.stTabs [data-baseweb="tab-list"] { background: #fff !important; gap: 0 !important; }
.stTabs [data-baseweb="tab"] {
    padding: 9px 20px !important; font-weight: 600 !important;
    font-size: .86rem !important; color: #666 !important;
    font-family: 'Segoe UI', sans-serif !important; background: none !important;
}
.stTabs [aria-selected="true"] { color: #FF6633 !important; }
.stTabs [data-baseweb="tab-highlight"] { background-color: #FF6633 !important; }
.stTabs [data-baseweb="tab-border"]    { background-color: #ddd !important; }

/* Compact filter widgets */
[data-testid="stSelectbox"] > div > div,
[data-baseweb="select"] > div {
    background: #fff !important; color: #1a1a2e !important;
    min-height: 34px !important; font-size: .8rem !important;
}
[data-testid="stTextInput"] input {
    background: #fff !important; color: #1a1a2e !important;
    padding: 6px 10px !important; font-size: .8rem !important;
}
[data-testid="stDateInput"] input {
    background: #fff !important; color: #1a1a2e !important;
    padding: 5px 8px !important; font-size: .8rem !important;
}
[data-baseweb="popover"], [data-baseweb="menu"] { background: #fff !important; }
[data-baseweb="menu"] li { color: #1a1a2e !important; font-size: .8rem !important; }
[data-baseweb="menu"] li:hover { background: #fff3ef !important; }
[data-testid="stColumn"] label {
    font-size: .72rem !important; color: #666 !important;
    font-weight: 500 !important; margin-bottom: 2px !important;
}
.stButton > button { background: #FF6633 !important; color: #fff !important;
                     border: none !important; font-size: .8rem !important; }
.stButton > button:hover { background: #e05520 !important; }
[data-testid="stCaptionContainer"] {
    padding: 3px 0 5px; font-size: .76rem !important; color: #666 !important; }
</style>
""", unsafe_allow_html=True)

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(page_title="MOPs", layout="wide",
                       initial_sidebar_state="collapsed")
    _page_css()

    st.markdown('<div class="mops-hdr"><h1>MOPs</h1></div>', unsafe_allow_html=True)

    funds  = load_funds()
    people = load_people()
    n_new_fc  = sum(1 for r in funds  if r.get("status") == "NEW")
    n_his_fc  = sum(1 for r in funds  if r.get("status") == "HISTORICAL")
    n_new_pm  = sum(1 for r in people if r.get("status") == "NEW")
    st.markdown(f"""
<div class="mops-metrics">
  <div class="mops-card"><div class="num">{n_new_fc}</div><div class="lbl">New Fund Commitments</div></div>
  <div class="mops-card"><div class="num">{n_his_fc}</div><div class="lbl">Historical Commitments</div></div>
  <div class="mops-card"><div class="num">{n_new_pm}</div><div class="lbl">New People Moves</div></div>
</div>""", unsafe_allow_html=True)

    _, rc = st.columns([9, 1])
    if rc.button("Refresh", key="refresh"):
        st.cache_data.clear()
        st.rerun()

    t_em, t_fs, t_fc, t_pm = st.tabs([
        "Company Profiles", "Financial Statements",
        "Fund Commitments", "People Moves",
    ])
    with t_em:
        tab_em()
    with t_fs:
        tab_fs()
    with t_fc:
        tab_fc()
    with t_pm:
        tab_pm()


if __name__ == "__main__":
    main()
