"""
MOPSOV Scraper — Fund Commitments + People Moves
Direct HTTP requests — no browser needed.
POST to: https://mopsov.twse.com.tw/mops/web/ezsearch_query
Detail pages from: https://emops.twse.com.tw/server-java/t05sr01_1_e
"""

import asyncio
import hashlib
import json
import logging
import re
import sys
import warnings

sys.stdout.reconfigure(encoding="utf-8")
from datetime import datetime, timezone, timedelta
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("mopsov")

# ── Config ────────────────────────────────────────────────────────────────────

MOPSOV_SEARCH_URL = "https://mopsov.twse.com.tw/mops/web/ezsearch_query"
EMOPS_DETAIL_HOST = "https://emops.twse.com.tw"

OUTPUT_DIR  = Path("output")
ARCHIVE_DIR = Path("storage/archive")
STATE_DIR   = Path("storage/state")

# Default date range — 2 years back to today
SDATE = (datetime.now() - timedelta(days=730)).strftime("%Y/%m/%d")
EDATE = datetime.now().strftime("%Y/%m/%d")

WATCHLIST = [
    {"stock_code": "2881", "name_en": "Fubon Financial Holding"},
    {"stock_code": "2882", "name_en": "Cathay Life Insurance"},
    {"stock_code": "2891", "name_en": "CBC Financial Holding"},
    {"stock_code": "2330", "name_en": "Taiwan Semiconductor Manufacturing Company"},
    {"stock_code": "5874", "name_en": "Nan Shan Life Insurance"},
    {"stock_code": "2317", "name_en": "Foxconn Technology Group"},
    {"stock_code": "2886", "name_en": "Mega International Commercial Bank"},
    {"stock_code": "2880", "name_en": "Hua Nan Commercial Bank"},
    {"stock_code": "5857", "name_en": "Land Bank of Taiwan"},
    {"stock_code": "2888", "name_en": "Shin Kong Life Insurance"},
    {"stock_code": "2801", "name_en": "Chang Hwa Bank"},
    {"stock_code": "2890", "name_en": "Bank SinoPac"},
    {"stock_code": "5876", "name_en": "Shanghai Commercial & Savings Bank"},
    {"stock_code": "2885", "name_en": "Yuanta Commercial Bank"},
    {"stock_code": "2833", "name_en": "Taiwan Life Insurance"},
    {"stock_code": "2867", "name_en": "Mercuries Life Insurance"},
    {"stock_code": "5873", "name_en": "TransGlobe Life Insurance"},
    {"stock_code": "2382", "name_en": "Quanta Computer"},
    {"stock_code": "3231", "name_en": "Wistron Corporation"},
    {"stock_code": "3711", "name_en": "ASE Technology Holding"},
    {"stock_code": "5859", "name_en": "Farglory Life Insurance"},
    {"stock_code": "2454", "name_en": "MediaTek"},
    {"stock_code": "2897", "name_en": "O-Bank"},
    {"stock_code": "2002", "name_en": "China Steel Corporation"},
]

FUND_KEYWORDS = [
    "fund", "acquisition", "LP", "buyout", "infrastructure",
    "private equity", "venture", "commitment",
]

PEOPLE_KEYWORDS = [
    "Chief Executive Officer", "Chief Investment Officer", "Chief Financial Officer",
    "Chief Risk Officer", "Chief Operating Officer", "Chief Information Officer",
    "General Manager", "President",
    "CEO", "CIO", "CFO", "CRO", "COO", "CMO", "CSO",
    "執行長", "投資長", "財務長", "風控長", "總經理",
]

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://mopsov.twse.com.tw/mops/web/ezsearch",
    "Origin": "https://mopsov.twse.com.tw",
    "Content-Type": "application/x-www-form-urlencoded",
}

# ── HTTP Search ───────────────────────────────────────────────────────────────

async def search_mopsov(stock_code: str, pro_item: str, sdate: str = None) -> list[dict]:
    """POST to ezsearch_query and return list of announcement rows."""
    data = {
        "step": "00",
        "RADIO_CM": "2",
        "TYPEK": "CO_MARKET",
        "CO_ID": stock_code,
        "PRO_ITEM": pro_item,
        "SUBJECT": "",
        "SDATE": sdate or SDATE,
        "EDATE": EDATE,
        "lang": "EN",
    }
    async with httpx.AsyncClient(headers=_HEADERS, timeout=30, verify=False) as client:
        try:
            resp = await client.post(MOPSOV_SEARCH_URL, data=data)
            resp.raise_for_status()
            logger.info("Search [%s/%s] → %d chars", stock_code, pro_item, len(resp.text))
            return _parse_results(resp.text, stock_code)
        except Exception as exc:
            logger.error("Search failed [%s/%s]: %s", stock_code, pro_item, exc)
            return []


def _parse_results(response_text: str, stock_code: str) -> list[dict]:
    try:
        # Strip UTF-8 BOM and leading whitespace before the JSON object
        clean = response_text.lstrip("\ufeff \r\n\t")
        data = json.loads(clean)
        records = data.get("data", [])
    except Exception:
        logger.error("Failed to parse JSON response for %s", stock_code)
        return []

    rows = []
    for item in records:
        subject = item.get("SUBJECT", "").replace("\r\n", " ").replace("\n", " ").strip()
        rows.append({
            "date": item.get("CDATE", ""),
            "time": item.get("CTIME", ""),
            "stock_code": item.get("COMPANY_ID", stock_code),
            "company_name": item.get("COMPANY_NAME", ""),
            "announcement_item": item.get("AN_CODE", ""),
            "subject": subject,
            "url": item.get("HYPERLINK", ""),
        })
    logger.info("Parsed %d records for %s", len(rows), stock_code)
    return rows


# ── Detail Page Parser ────────────────────────────────────────────────────────

async def fetch_detail(url: str) -> str:
    """Fetch announcement detail page HTML."""
    if not url:
        return ""
    async with httpx.AsyncClient(timeout=30, verify=False) as client:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            logger.error("Detail fetch failed [%s]: %s", url, exc)
            return ""


def extract_statement(html: str) -> str:
    """Extract the Statement column content from detail page HTML."""
    soup = BeautifulSoup(html, "lxml")
    for selector in [
        lambda s: s.find("td", string=re.compile(r"Statement", re.I)),
    ]:
        header = selector(soup)
        if header:
            sibling = header.find_next_sibling("td")
            if sibling:
                return sibling.get_text("\n", strip=True)
    # Fallback: find the largest td containing numbered fields
    best = ""
    for td in soup.find_all("td"):
        text = td.get_text("\n", strip=True)
        if re.search(r"^\d+\.", text, re.MULTILINE) and len(text) > len(best):
            best = text
    return best


def parse_statement_fields(text: str) -> dict[int, str]:
    """Split numbered field blocks e.g. '1. Fund name...' → {1: 'Fund name...'}"""
    text = re.sub(r"\r\n|\r", "\n", text)
    parts = re.split(r"(?:^|\n)(\d{1,2})\.\s*", text, flags=re.MULTILINE)
    fields: dict[int, str] = {}
    i = 1
    while i < len(parts) - 1:
        try:
            fields[int(parts[i])] = parts[i + 1].strip()
        except (ValueError, IndexError):
            pass
        i += 2
    return fields


# ── Fund Commitments ──────────────────────────────────────────────────────────

async def scrape_fund_commitments(stock_code: str, sdate: str = None) -> list[dict]:
    rows = await search_mopsov(stock_code, "M20", sdate=sdate)
    results = []
    for row in rows:
        html = await fetch_detail(row["url"])
        if not html:
            continue
        statement = extract_statement(html)
        if not statement:
            logger.warning("Empty statement: %s", row["url"])
            continue
        fields = parse_statement_fields(statement)
        f1, f2, f5 = fields.get(1, ""), fields.get(2, ""), fields.get(5, "")

        parts = [p.strip() for p in f1.split(";")]
        fund_name = parts[0] if parts else f1
        fund_type = parts[1] if len(parts) > 1 else ""

        date_match = re.search(r"\d{4}/\d{2}/\d{2}", f2)
        amount_parts = [p.strip() for p in f5.split(";")]
        amount_raw = next((p for p in reversed(amount_parts)
                           if p and p.upper() not in ("NA", "N/A", "")), f5)
        currency_match = re.search(r"\b(USD|EUR|GBP|JPY|TWD|HKD|SGD|AUD|CAD)\b", amount_raw)

        results.append({
            "stock_code": stock_code,
            "announcement_date": row["date"],
            "subject": row["subject"],
            "fund_name": fund_name,
            "fund_type": fund_type,
            "commitment_date": date_match.group(0) if date_match else "",
            "commitment_amount_raw": amount_raw,
            "commitment_currency": currency_match.group(1) if currency_match else "",
            "commitment_amount_numeric": _parse_amount(amount_raw),
            "url": row["url"],
        })
    logger.info("Fund commitments [%s]: %d found", stock_code, len(results))
    return results


# ── People Moves ──────────────────────────────────────────────────────────────

async def scrape_people_moves(stock_code: str, sdate: str = None) -> list[dict]:
    results = []
    for pro_item in ["B02", "M08"]:
        rows = await search_mopsov(stock_code, pro_item, sdate=sdate)
        for row in rows:
            if not _matches(row["subject"], PEOPLE_KEYWORDS):
                continue
            html = await fetch_detail(row["url"])
            if not html:
                continue
            statement = extract_statement(html)
            if not statement:
                continue
            fields = parse_statement_fields(statement)

            def clean(text):
                if text and ":" in text and len(text.split(":", 1)[0]) > 10:
                    return text.split(":", 1)[1].strip()
                return (text or "").strip()

            role_type      = clean(fields.get(1, ""))
            change_date    = _extract_date(fields.get(2, ""))
            prev_holder    = clean(fields.get(3, ""))
            new_holder     = clean(fields.get(4, ""))
            change_type    = clean(fields.get(5, "")).lower()
            reason         = clean(fields.get(6, ""))
            effective_date = _extract_date(fields.get(7, ""))

            if not _matches(role_type, PEOPLE_KEYWORDS):
                continue

            narrative = _build_narrative(stock_code, role_type, new_holder,
                                         prev_holder, change_type, effective_date)
            results.append({
                "stock_code": stock_code,
                "announcement_date": row["date"],
                "subject": row["subject"],
                "role_type": role_type,
                "change_date": change_date,
                "previous_holder": prev_holder,
                "new_holder": new_holder,
                "change_type": change_type,
                "reason": reason,
                "effective_date": effective_date,
                "narrative_en": narrative,
                "url": row["url"],
            })
    logger.info("People moves [%s]: %d found", stock_code, len(results))
    return results


# ── Helpers ───────────────────────────────────────────────────────────────────

def _matches(text: str, keywords: list[str]) -> bool:
    t = text.lower()
    return any(k.lower() in t for k in keywords)

def _extract_date(text: str) -> str:
    m = re.search(r"\d{4}/\d{2}/\d{2}|\d{4}-\d{2}-\d{2}", text or "")
    return m.group(0) if m else ""

def _parse_amount(text: str) -> float | None:
    m = re.search(r"[\d,]+(?:\.\d+)?", text.replace(" ", ""))
    if m:
        try:
            return float(m.group().replace(",", ""))
        except ValueError:
            pass
    return None

def _build_narrative(stock_code, role, new_holder, prev_holder, change_type, effective_date):
    entry = next((w for w in WATCHLIST if w["stock_code"] == stock_code), {})
    company = entry.get("name_en", "The investor")
    aum = _get_latest_aum(stock_code)
    ref = f"The {aum} {company}" if aum else company
    date_str = _format_date(effective_date)
    if "new replacement" in change_type or "appointment" in change_type:
        action = f"has appointed {new_holder} as its new {role}"
    elif "resignation" in change_type:
        action = f"'s {role}, {prev_holder}, has resigned"
    elif "retirement" in change_type:
        action = f"'s {role}, {prev_holder}, has retired"
    else:
        action = f"has announced a change in its {role}"
    return f"{ref} {action}{', effective ' + date_str if date_str else ''}."

def _get_latest_aum(stock_code: str) -> str:
    files = sorted(ARCHIVE_DIR.glob(f"{stock_code}_balance_sheet_*.json"), reverse=True)
    if not files:
        return ""
    try:
        records = json.loads(files[0].read_text(encoding="utf-8")).get("records", [])
        if not records:
            return ""
        total = records[0].get("total_assets_numeric")
        currency = records[0].get("currency", "TWD").replace(" (thousands)", "")
        if total is None:
            return ""
        if "thousands" in records[0].get("currency", ""):
            total *= 1000
        if total >= 1e9:
            return f"{currency} {total/1e9:.0f}bn"
        if total >= 1e6:
            return f"{currency} {total/1e6:.0f}mn"
    except Exception:
        pass
    return ""

def _format_date(date_str: str) -> str:
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return f"{dt.day} {dt.strftime('%B')} {dt.year}"
        except (ValueError, AttributeError):
            pass
    return date_str

# ── Change Detection ──────────────────────────────────────────────────────────

def detect_changes(records, state_path, key_fields):
    stored = _load_json(state_path) or {}
    updated = dict(stored)
    now = datetime.now(timezone.utc).isoformat()
    for record in records:
        key = "|".join(str(record.get(f, "")) for f in key_fields)
        h = hashlib.sha256(json.dumps(
            {k: v for k, v in record.items() if k not in {"scraped_at", "status", "hash"}},
            sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        record["hash"] = h
        record["scraped_at"] = now
        if key not in stored:           record["status"] = "NEW"
        elif stored[key]["hash"] != h:  record["status"] = "CHANGED"
        else:                           record["status"] = "UNCHANGED"
        updated[key] = {"hash": h, "last_seen": now}
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(updated, indent=2, ensure_ascii=False), encoding="utf-8")
    return records

def archive(stock_code, category, records):
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = ARCHIVE_DIR / f"{stock_code}_{category}_{ts}.json"
    path.write_text(json.dumps({"stock_code": stock_code, "category": category,
                                "records": records}, indent=2, ensure_ascii=False), encoding="utf-8")

def _load_json(path):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return None

# ── Excel Output ──────────────────────────────────────────────────────────────

_HDR_FILL = PatternFill("solid", fgColor="1F3864")
_NEW_FILL  = PatternFill("solid", fgColor="C6EFCE")   # green
_HIS_FILL  = PatternFill("solid", fgColor="FFC7CE")   # red
_CHG_FILL  = PatternFill("solid", fgColor="FFEB9C")   # yellow
_HDR_FONT  = Font(bold=True, color="FFFFFF", name="Calibri", size=10)

def write_excel(fund_commitments, people_moves, since=None, new_since=None):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    path = OUTPUT_DIR / f"MOPSOV_{ts}.xlsx"
    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    ws = wb.create_sheet("Summary")
    new_label = f"New Fund Commitments (from {new_since})" if new_since else "New Fund Commitments"
    his_label = f"Historical Fund Commitments (before {new_since})" if new_since else "Historical Fund Commitments"
    _header(ws, ["Run Date", "Data Extracted From", new_label, his_label, "New People Moves", "Changed People Moves"])
    ws.append([datetime.now().strftime("%Y-%m-%d %H:%M"),
               since or "2 years",
               sum(1 for r in fund_commitments if r.get("status") == "NEW"),
               sum(1 for r in fund_commitments if r.get("status") == "HISTORICAL"),
               sum(1 for r in people_moves if r.get("status") == "NEW"),
               sum(1 for r in people_moves if r.get("status") == "CHANGED")])
    _autofit(ws)

    ws = wb.create_sheet("FundCommitments")
    _header(ws, ["Stock Code", "Announcement Date", "Fund Name", "Fund Type",
                 "Commitment Date", "Amount (Raw)", "Amount (Numeric)", "Currency",
                 "Status", "Subject", "URL", "Scraped At"])
    for i, r in enumerate(fund_commitments, 2):
        ws.append([r.get("stock_code"), r.get("announcement_date"), r.get("fund_name"),
                   r.get("fund_type"), r.get("commitment_date"), r.get("commitment_amount_raw"),
                   r.get("commitment_amount_numeric"), r.get("commitment_currency"),
                   r.get("status"), r.get("subject"), r.get("url"), r.get("scraped_at")])
        _status_fill(ws, i, r.get("status"))
    _autofit(ws)

    ws = wb.create_sheet("PeopleMoves")
    _header(ws, ["Stock Code", "Announcement Date", "Role", "New Holder", "Previous Holder",
                 "Change Type", "Change Date", "Effective Date", "Reason",
                 "Narrative (EN)", "Status", "URL", "Scraped At"])
    for i, r in enumerate(people_moves, 2):
        ws.append([r.get("stock_code"), r.get("announcement_date"), r.get("role_type"),
                   r.get("new_holder"), r.get("previous_holder"), r.get("change_type"),
                   r.get("change_date"), r.get("effective_date"), r.get("reason"),
                   r.get("narrative_en"), r.get("status"), r.get("url"), r.get("scraped_at")])
        _status_fill(ws, i, r.get("status"))
    _autofit(ws)

    wb.save(path)
    logger.info("Excel saved: %s", path)
    return path

def _header(ws, cols):
    for c, h in enumerate(cols, 1):
        cell = ws.cell(1, c, h)
        cell.font = _HDR_FONT
        cell.fill = _HDR_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center")

def _status_fill(ws, row, status):
    if status == "NEW":
        for cell in ws[row]: cell.fill = _NEW_FILL
    elif status == "HISTORICAL":
        for cell in ws[row]: cell.fill = _HIS_FILL
    elif status == "CHANGED":
        for cell in ws[row]: cell.fill = _CHG_FILL

def _autofit(ws):
    for col in ws.columns:
        letter = get_column_letter(col[0].column)
        width = max((len(str(c.value or "")) for c in col), default=10)
        ws.column_dimensions[letter].width = min(max(width + 2, 10), 50)

# ── Terminal Preview ──────────────────────────────────────────────────────────

def print_results(fund_commitments, people_moves):
    print("\n" + "═" * 80)
    print(f"  FUND COMMITMENTS ({len(fund_commitments)} records)")
    print("═" * 80)
    for r in fund_commitments:
        print(f"\n  [{r.get('stock_code')}] {r.get('fund_name')} [{r.get('status')}]")
        print(f"    Type     : {r.get('fund_type')}")
        print(f"    Date     : {r.get('commitment_date')}")
        print(f"    Amount   : {r.get('commitment_currency')} {r.get('commitment_amount_raw')}")

    print("\n" + "═" * 80)
    print(f"  PEOPLE MOVES ({len(people_moves)} records)")
    print("═" * 80)
    for r in people_moves:
        print(f"\n  [{r.get('stock_code')}] {r.get('role_type')} [{r.get('status')}]")
        print(f"    New      : {r.get('new_holder')}")
        print(f"    Previous : {r.get('previous_holder')}")
        print(f"    Effective: {r.get('effective_date')}")
        print(f"    Narrative: {r.get('narrative_en')}")
    print("\n" + "═" * 80 + "\n")

# ── Main ──────────────────────────────────────────────────────────────────────

def _apply_date_filter(funds: list[dict], new_since: str) -> list[dict]:
    """Mark fund commitments NEW (green) if announcement_date >= new_since, else HISTORICAL (red)."""
    try:
        cutoff = datetime.strptime(new_since.strip(), "%Y/%m/%d")
    except ValueError:
        logger.warning("Invalid --new-since date '%s', expected YYYY/MM/DD — skipping filter", new_since)
        return funds
    for r in funds:
        raw = r.get("announcement_date", "")
        try:
            ann_dt = datetime.strptime(raw.strip(), "%Y/%m/%d")
            r["status"] = "NEW" if ann_dt >= cutoff else "HISTORICAL"
        except ValueError:
            r["status"] = "UNKNOWN"
    return funds


async def run(companies=None, export_excel=True, funds_only=False, people_only=False,
              since=None, new_since=None):
    watchlist = WATCHLIST if not companies else [w for w in WATCHLIST if w["stock_code"] in companies]
    logger.info("Running MOPSOV for %d companies", len(watchlist))
    if since:
        logger.info("Extracting fund commitments from %s", since)
    if new_since:
        logger.info("Flagging NEW (green) if announcement_date >= %s, else HISTORICAL (red)", new_since)

    all_funds, all_people = [], []

    for entry in watchlist:
        code = entry["stock_code"]
        logger.info("── %s %s", code, entry["name_en"])

        if not people_only:
            funds = await scrape_fund_commitments(code, sdate=since)
            if new_since:
                funds = _apply_date_filter(funds, new_since)
            else:
                funds = detect_changes(funds, STATE_DIR / f"{code}_funds.json",
                                       ["stock_code", "fund_name", "commitment_date"])
            all_funds.extend(funds)
            archive(code, "fund_commitments", funds)

        if not funds_only:
            moves = await scrape_people_moves(code, sdate=since)
            moves = detect_changes(moves, STATE_DIR / f"{code}_people.json",
                                   ["stock_code", "role_type", "change_date"])
            all_people.extend(moves)
            archive(code, "people_moves", moves)

    print_results(all_funds, all_people)
    if export_excel:
        write_excel(all_funds, all_people, since=since, new_since=new_since)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="MOPSOV Scraper — Fund Commitments & People Moves")
    parser.add_argument("--companies", nargs="+", help="Limit to specific stock codes e.g. 2882 2330")
    parser.add_argument("--no-excel", action="store_true", help="Print only, skip Excel export")
    parser.add_argument("--funds-only", action="store_true", help="Fund commitments only")
    parser.add_argument("--people-only", action="store_true", help="People moves only")
    parser.add_argument("--since", help="Extract data from this date e.g. 2025/01/01 (YYYY/MM/DD)")
    parser.add_argument("--new-since", dest="new_since", help="Flag as NEW (green) if on/after this date, else HISTORICAL (red) e.g. 2025/09/30")
    args = parser.parse_args()

    asyncio.run(run(companies=args.companies, export_excel=not args.no_excel,
                    funds_only=args.funds_only, people_only=args.people_only,
                    since=args.since, new_since=args.new_since))
