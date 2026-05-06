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
    {"stock_code": "2881", "name_en": "Fubon Financial Holding",                "company_type": "financial holding company"},
    {"stock_code": "2882", "name_en": "Cathay Life Insurance",                   "company_type": "insurance company"},
    {"stock_code": "2891", "name_en": "CBC Financial Holding",                   "company_type": "financial holding company"},
    {"stock_code": "2330", "name_en": "Taiwan Semiconductor Manufacturing Company", "company_type": "semiconductor company"},
    {"stock_code": "5874", "name_en": "Nan Shan Life Insurance",                 "company_type": "insurance company"},
    {"stock_code": "2317", "name_en": "Foxconn Technology Group",                "company_type": "technology company"},
    {"stock_code": "2886", "name_en": "Mega International Commercial Bank",      "company_type": "commercial bank"},
    {"stock_code": "2880", "name_en": "Hua Nan Commercial Bank",                 "company_type": "commercial bank"},
    {"stock_code": "5857", "name_en": "Land Bank of Taiwan",                     "company_type": "state-owned bank"},
    {"stock_code": "2888", "name_en": "Shin Kong Life Insurance",                "company_type": "insurance company"},
    {"stock_code": "2801", "name_en": "Chang Hwa Bank",                          "company_type": "commercial bank"},
    {"stock_code": "2890", "name_en": "Bank SinoPac",                            "company_type": "commercial bank"},
    {"stock_code": "5876", "name_en": "Shanghai Commercial & Savings Bank",      "company_type": "commercial bank"},
    {"stock_code": "2885", "name_en": "Yuanta Commercial Bank",                  "company_type": "commercial bank"},
    {"stock_code": "2833", "name_en": "Taiwan Life Insurance",                   "company_type": "insurance company"},
    {"stock_code": "2867", "name_en": "Mercuries Life Insurance",                "company_type": "insurance company"},
    {"stock_code": "5873", "name_en": "TransGlobe Life Insurance",               "company_type": "insurance company"},
    {"stock_code": "2382", "name_en": "Quanta Computer",                         "company_type": "technology company"},
    {"stock_code": "3231", "name_en": "Wistron Corporation",                     "company_type": "technology company"},
    {"stock_code": "3711", "name_en": "ASE Technology Holding",                  "company_type": "semiconductor company"},
    {"stock_code": "5859", "name_en": "Farglory Life Insurance",                 "company_type": "insurance company"},
    {"stock_code": "2454", "name_en": "MediaTek",                                "company_type": "semiconductor company"},
    {"stock_code": "2897", "name_en": "O-Bank",                                  "company_type": "digital bank"},
    {"stock_code": "2002", "name_en": "China Steel Corporation",                 "company_type": "steel company"},
]

# Allowlist: fund commitment must match at least one of these in name+type+statement
_FUND_ALLOWLIST_RE = re.compile(
    r"\bfund\b|private equity|\bP\.?E\b|venture capital|"
    r"real estate|\bREIT\b|infrastructure|hedge fund|"
    r"alternative (?:asset|investment)|secondar(?:y|ies)|"
    r"private (?:credit|debt|market)|mezzanine|"
    r"growth (?:equity|capital)|buyout|\bLBO\b|"
    r"co.?invest|special situation|distressed|"
    r"natural resource|commodit(?:y|ies)|gold|precious metal|"
    r"private fund|real asset",
    re.IGNORECASE
)

# Fund name looks like a date or date-range (not a real fund name)
_DATE_NAME_RE = re.compile(r"^\d{4}/\d{2}/\d{2}(\s*~\s*\d{4}/\d{2}/\d{2})?$")

# Roles we actively track — sorted longest-first so the regex prefers specific matches
TRACKED_ROLES = sorted([
    "Chief Compliance Officer", "Chief Executive Officer",
    "Chief Finance Officer", "Chief Financial Officer",
    "Chief Information Officer", "Chief Investment Officer",
    "Chief Operating Officer", "Chief Operations Officer",
    "Chief Risk Officer",
    "Chief Sustainability Officer", "Chief Technology Officer",
    "Chief Strategy Officer", "Chief Accountant",
    "Chief Business Development Officer",
    "Global Head of Alternative Assets", "Global Head of Alternative Investments",
    "Global Head of Alternatives", "Global Head of Hedge Funds",
    "Global Head of Infrastructure", "Global Head of Investor Relations",
    "Global Head of Private Debt", "Global Head of Private Equity",
    "Global Head of Real Assets", "Global Head of Real Estate",
    "Global Head of Secondaries",
    "Head of Alternative Assets", "Head of Alternative Investment",
    "Head of Alternative Investments", "Head of Alternatives",
    "Head of Asset Allocation", "Head of Asset Management",
], key=len, reverse=True)

_ROLE_ABBREV = {
    "CEO": "Chief Executive Officer", "CFO": "Chief Financial Officer",
    "CIO": "Chief Investment Officer", "COO": "Chief Operating Officer",
    "CTO": "Chief Technology Officer", "CSO": "Chief Strategy Officer",
}

_TRACKED_ROLES_RE = re.compile(
    "|".join(re.escape(r) for r in TRACKED_ROLES) + r"|\b(CEO|CFO|CIO|COO|CTO|CSO)\b",
    re.IGNORECASE,
)

# Broad keywords used to decide which MOPS announcements to fetch
PEOPLE_KEYWORDS = (
    TRACKED_ROLES
    + list(_ROLE_ABBREV.keys())
    + ["執行長", "投資長", "財務長", "風控長", "總經理", "General Manager", "President"]
)

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

async def fetch_detail(url: str, retries: int = 3) -> str:
    """Fetch announcement detail page HTML with retries and rate-limit handling."""
    if not url:
        return ""
    for attempt in range(1, retries + 1):
        try:
            async with httpx.AsyncClient(timeout=30, verify=False) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                html = ""
                for enc in ("utf-8", "cp950", "big5"):
                    try:
                        html = resp.content.decode(enc)
                        break
                    except UnicodeDecodeError:
                        continue
                if not html:
                    html = resp.content.decode("utf-8", errors="replace")
                # Detect server rate-limit page (查詢過量 = "Too many queries")
                if "查詢過量" in html:
                    wait = 10 * attempt
                    logger.warning("Rate limited by server — waiting %ds before retry %d/%d [%s]", wait, attempt, retries, url)
                    await asyncio.sleep(wait)
                    continue
                return html
        except Exception as exc:
            if attempt < retries:
                logger.warning("Detail fetch attempt %d/%d failed [%s]: %s — retrying", attempt, retries, url, exc)
                await asyncio.sleep(2 ** attempt)
            else:
                logger.error("Detail fetch failed after %d attempts [%s]: %s", retries, url, exc)
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
    rows = []
    for pro_item in ["M20", "M24"]:
        rows.extend(await search_mopsov(stock_code, pro_item, sdate=sdate))
    results = []
    for row in rows:
        await asyncio.sleep(0.8)   # throttle to avoid server rate limit
        html = await fetch_detail(row["url"])
        if not html:
            continue
        statement = extract_statement(html)
        if not statement:
            logger.warning("Empty statement: %s", row["url"])
            continue
        fields = parse_statement_fields(statement)
        f1, f2 = fields.get(1, ""), fields.get(2, "")

        # Strip the standard field label prefix, keep only fund name and type
        f1_clean = f1.rsplit(":", 1)[-1].strip() if ":" in f1 else f1
        parts = [p.strip() for p in f1_clean.split(";")]
        fund_name = _clean_fund_name(parts[0] if parts else f1_clean)
        fund_type = parts[1] if len(parts) > 1 else ""

        # Skip disposal/secondary transactions and unparseable names
        if re.search(r"^The partnership interests? of\b", fund_name, re.I):
            continue
        if _DATE_NAME_RE.match(fund_name):   # field 1 parsed as a date, not a fund name
            continue

        date_match = re.search(r"\d{4}/\d{2}/\d{2}", f2)

        # Find the amount field — field number varies across M20/M24 formats
        amount_field = next(
            (v for v in fields.values()
             if re.search(r"total monetary amount of the transaction", v, re.I)),
            None
        )
        # Skip if no standard fund commitment amount field found (e.g. property/lease)
        if not amount_field:
            continue

        total_match = re.search(r"\nTotal monetary amount[^:]*:\s*([^\n]+)", amount_field, re.I)
        if total_match:
            amount_raw = total_match.group(1).strip()
        else:
            amount_parts = [p.strip() for p in amount_field.split(";")]
            amount_raw = next((p for p in reversed(amount_parts)
                               if p and not re.match(r"^N/?A\b", p, re.I)), "")
        # Strip update/revision annotations from raw amount text
        amount_raw = re.sub(r"\s*\(updated\)", "", amount_raw, flags=re.IGNORECASE).strip()
        # Deduplicate leading currency ticker (e.g. "EUR EUR 50,000,000" → "EUR 50,000,000")
        amount_raw = re.sub(r"^([A-Z]{3})\s+\1\b", r"\1", amount_raw)
        currency_match = re.search(r"\b(USD|EUR|GBP|JPY|TWD|HKD|SGD|AUD|CAD)\b", amount_raw)

        searchable = fund_name + " " + fund_type + " " + row["subject"] + " " + statement
        if not _FUND_ALLOWLIST_RE.search(searchable):
            continue

        _, bs_date = _get_latest_aum(stock_code)
        formatted_amount, fx_url = await _format_commitment_amount(amount_raw, currency_match.group(1) if currency_match else "")
        headline = _build_fund_headline(stock_code, fund_name, formatted_amount)

        results.append({
            "stock_code": stock_code,
            "announcement_date": row["date"],
            "subject": row["subject"],
            "headline": headline,
            "fund_name": fund_name,
            "fund_type": fund_type,
            "commitment_date": date_match.group(0) if date_match else "",
            "commitment_amount_raw": amount_raw,
            "commitment_currency": currency_match.group(1) if currency_match else "",
            "commitment_amount_numeric": _parse_amount(amount_raw),
            "fx_url": fx_url,
            "bs_date": bs_date,
            "url": row["url"],
        })
    logger.info("Fund commitments [%s]: %d found", stock_code, len(results))
    return results


# ── People Moves ──────────────────────────────────────────────────────────────

async def scrape_people_moves(stock_code: str, sdate: str = None) -> list[dict]:
    results = []
    seen_urls: set[str] = set()
    for pro_item in ["B02", "M08"]:
        rows = await search_mopsov(stock_code, pro_item, sdate=sdate)
        for row in rows:
            if row.get("url") in seen_urls:
                continue
            seen_urls.add(row.get("url", ""))
            if not _matches(row["subject"], PEOPLE_KEYWORDS):
                continue
            await asyncio.sleep(0.8)
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

            # Primary: extract role from field 1 suffix (text after last ':')
            # e.g. "...representatives):COO" → "COO" → "Chief Operating Officer"
            role_raw = role_type.rsplit(":", 1)[-1].strip() if ":" in role_type else ""
            role_title = (_extract_tracked_role(role_raw)
                          or _extract_tracked_role(new_holder)
                          or _extract_tracked_role(prev_holder))
            if not role_title:
                continue

            new_holder_clean  = _clean_holder_name(new_holder)
            prev_holder_clean = _clean_holder_name(prev_holder)

            narrative = _build_narrative(stock_code, role_title,
                                         new_holder_clean, prev_holder_clean,
                                         change_type, effective_date,
                                         reason=reason)
            results.append({
                "stock_code": stock_code,
                "announcement_date": row["date"],
                "subject": row["subject"],
                "role_type": role_type,
                "role_title": role_title,
                "change_date": change_date,
                "previous_holder": prev_holder_clean,
                "new_holder": new_holder_clean,
                "change_type": change_type,
                "reason": reason,
                "effective_date": effective_date,
                "narrative_en": narrative,
                "url": row["url"],
            })
    logger.info("People moves [%s]: %d found", stock_code, len(results))
    return results


# ── Helpers ───────────────────────────────────────────────────────────────────

_LEGAL_SUFFIX_RE = re.compile(
    r"[,\s]*\b("
    r"S\.C\.S\.,?\s*SICAV-RAIF|SICAV-RAIF|S\.C\.S\.|SCSp|S\.C\.A\.|RAIF|SICAV|SCA|"
    r"L\.P\.|L\.P|(?<!\w)LP(?!\w)|"
    r"Ltd\.|Ltd|Limited|"
    r"LLC|L\.L\.C\.|"
    r"Pte\.?\s*Ltd\.?|(?<!\w)Pte(?!\w)|"
    r"Inc\.|(?<!\w)Inc(?!\w)|"
    r"Corp\.|(?<!\w)Corp(?!\w)|"
    r"(?<!\w)Co\.(?!\w)|"
    r"GmbH|S\.A\.|(?<!\w)SA(?!\w)|B\.V\.|(?<!\w)BV(?!\w)|N\.V\.|(?<!\w)NV(?!\w)|"
    r"Sàrl|SARL"
    r")\s*$",
    re.IGNORECASE,
)

def _clean_fund_name(name: str) -> str:
    """Strip trailing legal entity suffixes (L.P., Ltd., SCSp, SICAV-RAIF, etc.)."""
    prev = None
    while prev != name:
        prev = name
        name = _LEGAL_SUFFIX_RE.sub("", name).strip().rstrip(",").strip()
    return name

def _extract_tracked_role(text: str) -> str:
    """Return the best-matching tracked role title from holder text (searches after '/')."""
    if not text:
        return ""
    # Prefer the section after '/' where the role description lives
    parts = re.split(r"\s*/\s*", text, maxsplit=1)
    search_in = parts[1] if len(parts) > 1 else text
    best_raw_len = 0
    best = ""
    for m in _TRACKED_ROLES_RE.finditer(search_in):
        matched = m.group(0)
        # Skip abbreviations that appear inside "to/of the CEO" constructions
        if len(matched) <= 3 and re.search(
            r'\b(?:to|of)\s+the\s+' + re.escape(matched), search_in, re.I
        ):
            continue
        expanded = _ROLE_ABBREV.get(matched.upper(), matched)
        # Score by raw match length — full titles beat abbreviations
        if len(matched) > best_raw_len:
            best_raw_len = len(matched)
            best = expanded
    return best.strip()

def _clean_holder_name(text: str) -> str:
    """Return person's name from holder text, title-cased (strips '/ role...' suffix)."""
    if not text or text.lower().strip() in ("none", "nil", "n/a", ""):
        return ""
    name = re.split(r"\s*/\s*", text.strip(), maxsplit=1)[0].strip()
    name = name.rstrip(".,、，").strip()
    if name.lower() in ("none", "nil", "n/a"):
        return ""
    # Comma-separated format is SURNAME,GIVEN-NAME → convert to "Surname Given-Name"
    name = name.replace(",", " ").strip()
    # Title-case each word, preserving hyphens (Python's str.title handles hyphens)
    name = name.title()
    return name

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

_FX_CACHE: dict[str, dict] = {}

async def _get_fx_rates(base: str) -> dict[str, float]:
    if base in _FX_CACHE:
        return _FX_CACHE[base]
    try:
        async with httpx.AsyncClient(timeout=10, verify=False) as client:
            resp = await client.get(f"https://open.er-api.com/v6/latest/{base}")
            data = resp.json()
            if data.get("result") == "success":
                _FX_CACHE[base] = data.get("rates", {})
                return _FX_CACHE[base]
    except Exception as exc:
        logger.warning("FX fetch failed [%s]: %s", base, exc)
    return {}

async def _format_commitment_amount(amount_raw: str, orig_currency: str) -> tuple[str, str]:
    """Returns (formatted_amount_str, fx_url).
    formatted_amount like 'TWD 1,846 million (EUR 50 million)'.
    fx_url is an XE.com link for the conversion, empty if no conversion needed."""
    if not amount_raw or re.match(r"^N/?A\b", amount_raw, re.I):
        return "", ""
    num_match = re.search(r"\d[\d,]*(?:\.\d+)?", amount_raw.replace(" ", ""))
    if not num_match:
        return amount_raw, ""
    amount = float(num_match.group().replace(",", ""))
    orig_millions = amount / 1e6

    if not orig_currency or orig_currency == "TWD":
        return f"TWD {orig_millions:,.0f} million", ""

    orig_str = f"{orig_currency} {orig_millions:,.0f} million"
    fx_url = f"https://www.xe.com/currencyconverter/convert/?Amount={amount:,.0f}&From={orig_currency}&To=TWD"
    rates = await _get_fx_rates(orig_currency)
    twd_rate = rates.get("TWD", 0)
    if twd_rate:
        twd_millions = (amount * twd_rate) / 1e6
        return f"TWD {twd_millions:,.0f} million ({orig_str})", fx_url
    return orig_str, fx_url

def _build_fund_headline(stock_code, fund_name, formatted_amount):
    entry = next((w for w in WATCHLIST if w["stock_code"] == stock_code), {})
    company_type = entry.get("company_type", "investor")
    aum, _ = _get_latest_aum(stock_code)
    ref = f"The {aum} {company_type}" if aum else f"The {company_type}"
    committed = f"has committed {formatted_amount} to" if formatted_amount else "has committed to"
    return f"{ref} {committed} {fund_name}."

def _build_narrative(stock_code, role, new_holder, prev_holder, change_type,
                     effective_date, reason=""):
    entry = next((w for w in WATCHLIST if w["stock_code"] == stock_code), {})
    company_type = entry.get("company_type", "company")
    aum, _ = _get_latest_aum(stock_code)
    company_ref = f"The {aum} {company_type}" if aum else f"The {company_type}"
    date_str = _format_date(effective_date)
    eff = f", effective {date_str}" if date_str else ""

    has_new  = bool(new_holder  and new_holder.lower()  not in ("none", "nil", "n/a", ""))
    has_prev = bool(prev_holder and prev_holder.lower() not in ("none", "nil", "n/a", ""))
    ct = change_type or ""

    reason_clean = reason.strip().rstrip(".") if reason else ""
    # Capitalise and append as a closing sentence
    reason_s = (f" {reason_clean[0].upper() + reason_clean[1:]}."
                if reason_clean and reason_clean.lower() not in ("nil", "n/a", "none")
                else "")

    if has_new:
        s1 = f"{company_ref} has appointed {new_holder} as {role}{eff}."
        if has_prev:
            surname = new_holder.split()[-1]
            s2 = f" {surname} will succeed {prev_holder}."
        else:
            s2 = ""
    elif "position adjustment" in ct and not has_new:
        s1 = f"{company_ref}'s {role} position has been eliminated{eff}."
        s2 = f" {prev_holder} vacated the role." if has_prev else ""
    elif has_prev:
        verb = ("has resigned" if "resignation" in ct
                else "has retired" if "retirement" in ct
                else "has stepped down")
        s1 = f"{company_ref}'s {role}, {prev_holder}, {verb}{eff}."
        s2 = ""
    else:
        s1 = f"{company_ref} has announced a change in its {role}{eff}."
        s2 = ""

    return s1 + s2 + reason_s

def _get_latest_aum(stock_code: str) -> tuple[str, str]:
    """Returns (aum_string, balance_sheet_period). Both empty string if unavailable."""
    files = sorted(ARCHIVE_DIR.glob(f"{stock_code}_balance_sheet_*.json"), reverse=True)
    if not files:
        return "", ""
    try:
        data = json.loads(files[0].read_text(encoding="utf-8"))
        records = data.get("records", [])
        if not records:
            return "", ""
        rec = records[0]
        total = rec.get("total_assets_numeric")
        currency = rec.get("currency", "TWD").replace(" (thousands)", "")
        period = rec.get("period", "")
        if total is None:
            return "", period
        if "thousands" in rec.get("currency", ""):
            total *= 1000
        if total >= 1e9:
            return f"{currency} {total/1e9:,.0f} billion", period
        if total >= 1e6:
            return f"{currency} {total/1e6:,.0f} million", period
    except Exception:
        pass
    return "", ""

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
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    for record in records:
        key = "|".join(str(record.get(f, "")) for f in key_fields)
        h = hashlib.sha256(json.dumps(
            {k: v for k, v in record.items() if k not in {"scraped_at", "status", "hash"}},
            sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        record["hash"] = h
        record["scraped_at"] = now
        if key not in stored:           record["status"] = "NEW"
        elif stored[key]["hash"] != h:  record["status"] = "CHANGED"
        else:                           record["status"] = "HISTORICAL"
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

_MOJIBAKE_RE = re.compile(chr(0xfffd) + '[@AB]')

def _clean_address(addr: str) -> str:
    if not addr:
        return addr
    # Big5 0xA1 bytes decode as U+FFFD + next ASCII byte; replace each pair with ', '
    addr = _MOJIBAKE_RE.sub(', ', addr)
    addr = addr.replace('、', ', ').replace('，', ', ')
    addr = re.sub(r',\s*,', ', ', addr)
    addr = re.sub(r'\s+', ' ', addr).strip()
    return addr

def _clean_web_address(url: str) -> str:
    if not url:
        return url
    # Strip mojibake artifacts entirely from URLs
    url = _MOJIBAKE_RE.sub('', url).rstrip('@').strip()
    return url

_CO_SUFFIX_RE = re.compile(
    r'(?:[,.\s]|\s)*\b('
    r'co\.,?\s*ltd\.?|co\.,?\s*limited|company\s+limited|company\s+ltd\.?|'
    r'corp\.,?\s*ltd\.?|holdings?\s+company\s+limited|'
    r'incorporated|\blimited\b|ltd\.?|corp\.?|\bcorporation\b|\binc\.?|co\.?'
    r')\s*$',
    re.IGNORECASE
)

def _clean_company_name(name: str) -> str:
    if not name:
        return name
    name = name.strip()
    prev = None
    while prev != name:
        prev = name
        name = _CO_SUFFIX_RE.sub('', name).strip().rstrip(',').strip()
    return name

def _format_tw_phone(phone: str) -> str:
    if not phone:
        return phone
    if phone.startswith('+'):
        return phone
    clean = re.sub(r'[()（）\s]', '', phone)
    if clean.startswith('886'):
        return '+' + clean
    if clean.startswith('0'):
        if clean.startswith('09'):
            return '+886-' + clean[1:]
        m = re.match(r'^0(\d+?)[-]?(.*)', clean)
        if m:
            return f'+886-{m.group(1)}-{m.group(2)}'
        return '+886-' + clean[1:]
    if len(re.sub(r'\D', '', clean)) == 8:
        return f'+886-2-{clean}'
    return phone

# ── Excel Output ──────────────────────────────────────────────────────────────

_HDR_FILL = PatternFill("solid", fgColor="1F3864")
_NEW_FILL  = PatternFill("solid", fgColor="C6EFCE")   # green
_HIS_FILL  = PatternFill("solid", fgColor="FFC7CE")   # red
_CHG_FILL  = PatternFill("solid", fgColor="FFEB9C")   # yellow
_HDR_FONT  = Font(bold=True, color="FFFFFF", name="Calibri", size=10)

def _save_run_to_history(fund_commitments, people_moves, emops_data, since, new_since) -> list:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    history_path = OUTPUT_DIR / "run_history.json"
    history = []
    if history_path.exists():
        try:
            history = json.loads(history_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    def slim(r, keys):
        return {k: (r.get(k) or "") for k in keys}

    fc_keys = ["stock_code","announcement_date","fund_name","fund_type","commitment_date",
               "commitment_amount_raw","headline","bs_date","status","scraped_at","url","fx_url"]
    pm_keys = ["stock_code","announcement_date","role_type","role_title","new_holder","previous_holder",
               "change_type","reason","effective_date","narrative_en","status","scraped_at","url"]
    em_keys = ["stock_code","name_en","company_type","company_name_en","address","telephone",
               "web_address","period","currency","total_assets_raw","inv_property_raw",
               "profile_status","changed_fields","scraped_at"]

    def slim_em(r):
        d = {k: (r.get(k) or "") for k in em_keys}
        d["changed_fields"] = r.get("changed_fields") or []
        return d

    run = {
        "run_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "since": since or "",
        "new_since": new_since or "",
        "n_new_fc":  sum(1 for r in fund_commitments if r.get("status") == "NEW"),
        "n_his_fc":  sum(1 for r in fund_commitments if r.get("status") == "HISTORICAL"),
        "n_new_pm":  sum(1 for r in people_moves    if r.get("status") == "NEW"),
        "n_chg_pm":  sum(1 for r in people_moves    if r.get("status") == "CHANGED"),
        "n_chg_em":  sum(1 for r in (emops_data or []) if r.get("profile_status") == "CHANGED"),
        "funds":  [slim(r, fc_keys) for r in fund_commitments],
        "people": [slim(r, pm_keys) for r in people_moves],
        "emops":  [slim_em(r) for r in (emops_data or [])],
    }
    history.insert(0, run)
    history_path.write_text(json.dumps(history, indent=2, ensure_ascii=False), encoding="utf-8")
    return history


def write_html_report(fund_commitments, people_moves, emops_data=None, since=None, new_since=None):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    history = _save_run_to_history(fund_commitments, people_moves, emops_data, since, new_since)
    history_json = json.dumps(history, ensure_ascii=False)
    run_ts = history[0]["run_date"]

    run_options = "\n".join(
        f'<option value="{i}">{r["run_date"].split(" ")[0]}  —  {r["n_new_fc"]} new FC · {r["n_new_pm"]} new PM · {r.get("n_chg_em",0)} EMOPS changes</option>'
        for i, r in enumerate(history)
    )

    companies_js = "{" + ",".join(
        f'"{w["stock_code"]}":"{w["name_en"]}"' for w in WATCHLIST
    ) + "}"

    # JS is a plain string (not f-string) so template literals work without escaping
    js = (
        f"<script>\nconst COMPANIES={companies_js};\nconst HISTORY = "
        + history_json
        + r"""
;
function badge(st){const m={NEW:'new',HISTORICAL:'his',CHANGED:'chg',UNCHANGED:''};const c=m[st]||'';return c?`<span class="badge ${c}">${st}</span>`:(st||'');}
function fv(val,changed){return changed?`<span class="field-chg" title="Changed since last run">${val||'—'}</span>`:(val||'—');}
function chkGet(key){try{return JSON.parse(localStorage.getItem(key)||'null');}catch{return null;}}
function chkSet(key,state,name){localStorage.setItem(key,JSON.stringify({state,name,ts:new Date().toISOString()}));}
function fmtTs(iso){if(!iso)return '';try{const d=new Date(iso);return d.toLocaleDateString('en-GB',{day:'2-digit',month:'short',year:'numeric'})+' '+d.toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit'});}catch{return '';}}
function chkBtn(key){
  const d=chkGet(key)||{};const done=d.state==='checked';
  return `<span class="chk-wrap"><button class="chk-btn ${done?'chk-done':'chk-pend'}" onclick="toggleCheck(this,'${key}')">${done?'Checked ✓':'Pending'}</button><div class="chk-ts">${fmtTs(d.ts)}</div><input class="chk-name" placeholder="Reviewer" value="${d.name||''}" onchange="saveName(this,'${key}')" onclick="event.stopPropagation()"></span>`;
}
function toggleCheck(btn,key){
  const d=chkGet(key)||{};const next=d.state!=='checked';
  chkSet(key,next?'checked':'pending',d.name||'');
  btn.textContent=next?'Checked ✓':'Pending';btn.className='chk-btn '+(next?'chk-done':'chk-pend');
  btn.nextElementSibling.textContent=fmtTs(new Date().toISOString());
}
function saveName(inp,key){const d=chkGet(key)||{state:'pending'};chkSet(key,d.state,inp.value);}

function populateSel(sel,opts){
  const cur=sel.value;sel.innerHTML='<option value="">All</option>';
  opts.forEach(([v,l])=>{const o=document.createElement('option');o.value=v;o.textContent=l;sel.appendChild(o);});
  if([...sel.options].some(o=>o.value===cur))sel.value=cur;
}

function renderRun(idx){
  const run=HISTORY[idx];
  document.querySelectorAll('.filters input[type=text]').forEach(i=>i.value='');
  document.getElementById('run-info').textContent=`Data from: ${run.since||'2 years'} · baseline: ${run.new_since||'state file'}`;
  document.getElementById('m-new-fc').textContent=run.n_new_fc;
  document.getElementById('m-his-fc').textContent=run.n_his_fc;
  document.getElementById('m-new-pm').textContent=run.n_new_pm;
  document.getElementById('m-chg-pm').textContent=run.n_chg_pm;
  document.getElementById('m-chg-em').textContent=run.n_chg_em||0;
  // Populate company dropdowns from run data
  const allCodes=[...new Set([...run.funds.map(r=>r.stock_code),...(run.people||[]).map(r=>r.stock_code),...(run.emops||[]).map(r=>r.stock_code)])].sort();
  const codeOpts=allCodes.map(c=>[c,COMPANIES[c]?`${c} — ${COMPANIES[c]}`:c]);
  document.querySelectorAll('.co-filter').forEach(s=>populateSel(s,codeOpts));
  // Populate fund type dropdown
  const ftypes=[...new Set(run.funds.map(r=>r.fund_type||'').filter(Boolean))].sort();
  populateSel(document.getElementById('fc-ft-sel'),ftypes.map(t=>[t.toLowerCase(),t]));
  renderEM(run.emops||[]);renderFC(run.funds||[]);renderPM(run.people||[]);
}
function renderEM(rows){
  document.querySelector('#em-table tbody').innerHTML=rows.map(r=>{
    const cf=r.changed_fields||[];
    const webUrl=r.web_address?(r.web_address.match(/^https?:\/\//)?r.web_address:'https://'+r.web_address):'';
    const webInner=webUrl?`<a href="${webUrl}" target="_blank">${r.web_address}</a>`:(r.web_address||'—');
    const webCell=cf.includes('web_address')?`<span class="field-chg">${webInner}</span>`:webInner;
    const rowCls=r.profile_status==='CHANGED'?'row-changed':'';
    const cfList=cf.length?`<span class="cf-list" title="${cf.join(', ')}">${cf.join(', ')}</span>`:'—';
    const ck=`chk_em_${r.stock_code}`;
    return `<tr class="${rowCls}" data-co="${r.stock_code}" data-st="${(r.profile_status||'').toLowerCase()}" data-date="${normDate(r.scraped_at||'')}"><td>${r.stock_code}</td><td>${fv(r.company_name_en||r.name_en,cf.includes('company_name_en'))}</td><td>${r.company_type}</td><td>${fv(r.period,cf.includes('period'))}</td><td>${fv(r.currency,cf.includes('currency'))}</td><td>${fv(r.total_assets_raw,cf.includes('total_assets_raw'))}</td><td>${fv(r.inv_property_raw,cf.includes('inv_property_raw'))}</td><td>${fv(r.telephone,cf.includes('telephone'))}</td><td>${webCell}</td><td>${fv(r.address,cf.includes('address'))}</td><td>${badge(r.profile_status)}</td><td>${cfList}</td><td>${r.scraped_at||'—'}</td><td>${chkBtn(ck)}</td></tr>`;
  }).join('');
  document.getElementById('em-count').textContent=rows.length+' companies';
}
function renderFC(rows){
  document.querySelector('#fc-table tbody').innerHTML=rows.map(r=>{
    const st=r.status||'';const yr=(r.announcement_date||'').slice(0,4);
    const nm=r.url?`<a href="${r.url}" target="_blank">${r.fund_name}</a>`:(r.fund_name||'');
    const amt=r.fx_url?`<a href="${r.fx_url}" target="_blank">${r.commitment_amount_raw}</a>`:(r.commitment_amount_raw||'—');
    const ck=`chk_fc_${r.stock_code}_${(r.fund_name||'').replace(/\W+/g,'_')}_${r.commitment_date}`;
    const firm=COMPANIES[r.stock_code]||r.stock_code;
    return `<tr class="row-${st.toLowerCase()}" data-co="${r.stock_code}" data-ft="${(r.fund_type||'').toLowerCase()}" data-st="${st.toLowerCase()}" data-yr="${yr}" data-date="${normDate(r.announcement_date||'')}"><td>${r.stock_code}</td><td>${firm}</td><td>${r.announcement_date}</td><td>${nm}</td><td>${r.fund_type||'—'}</td><td>${r.commitment_date}</td><td>${amt}</td><td class="headline">${r.headline}</td><td>${r.bs_date}</td><td>${badge(st)}</td><td>${r.scraped_at}</td><td>${chkBtn(ck)}</td></tr>`;
  }).join('');
  document.getElementById('fc-count').textContent=rows.length+' records';
}
function renderPM(rows){
  document.querySelector('#pm-table tbody').innerHTML=rows.map(r=>{
    const st=r.status||'';const yr=(r.announcement_date||'').slice(0,4);
    const role=r.role_title||r.role_type||'';
    const lnk=r.url?`<a href="${r.url}" target="_blank">View</a>`:'';
    const ck=`chk_pm_${r.stock_code}_${r.announcement_date}_${(r.new_holder||'').replace(/\W+/g,'_')}`;
    return `<tr class="row-${st.toLowerCase()}" data-co="${r.stock_code}" data-st="${st.toLowerCase()}" data-yr="${yr}" data-date="${normDate(r.announcement_date||'')}"><td>${r.stock_code}</td><td>${r.announcement_date}</td><td>${role||'—'}</td><td>${r.new_holder||'—'}</td><td>${r.previous_holder||'—'}</td><td>${r.effective_date}</td><td class="headline">${r.narrative_en}</td><td>${lnk}</td><td>${chkBtn(ck)}</td></tr>`;
  }).join('');
  document.getElementById('pm-count').textContent=rows.length+' records';
}
function showTab(id,el){document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));document.getElementById(id).classList.add('active');el.classList.add('active');}
function normDate(d){return d?d.replace(/\//g,'-'):'';}
function inDateRange(rowDate,from,to){const d=normDate(rowDate);return(!from||d>=from)&&(!to||d<=to);}
function filterEM(){
  const sel=[...document.querySelectorAll('#em .filters select')].map(e=>e.value.toLowerCase());
  const dates=[...document.querySelectorAll('#em .filters input[type=date]')].map(e=>e.value);
  const q=document.querySelector('#em .filters input[type=text]').value.toLowerCase();
  const [co,st]=[sel[0],sel[1]];const [df,dt]=dates;
  let v=0;document.querySelectorAll('#em-table tbody tr').forEach(r=>{
    const s=(!co||r.dataset.co===co)&&(!st||r.dataset.st===st)&&inDateRange(r.dataset.date,df,dt)&&(!q||r.textContent.toLowerCase().includes(q));
    r.style.display=s?'':'none';if(s)v++;});
  document.getElementById('em-count').textContent=v+' companies';
}
function filterFC(){
  const sel=[...document.querySelectorAll('#fc .filters select')].map(e=>e.value.toLowerCase());
  const dates=[...document.querySelectorAll('#fc .filters input[type=date]')].map(e=>e.value);
  const q=document.querySelector('#fc .filters input[type=text]').value.toLowerCase();
  const [co,ft,st]=[sel[0],sel[1],sel[2]];const [df,dt]=dates;
  let v=0;document.querySelectorAll('#fc-table tbody tr').forEach(r=>{
    const s=(!co||r.dataset.co===co)&&(!ft||r.dataset.ft.includes(ft))&&(!st||r.dataset.st===st)&&inDateRange(r.dataset.date,df,dt)&&(!q||r.textContent.toLowerCase().includes(q));
    r.style.display=s?'':'none';if(s)v++;});
  document.getElementById('fc-count').textContent=v+' records';
}
function filterPM(){
  const sel=[...document.querySelectorAll('#pm .filters select')].map(e=>e.value.toLowerCase());
  const dates=[...document.querySelectorAll('#pm .filters input[type=date]')].map(e=>e.value);
  const q=document.querySelector('#pm .filters input[type=text]').value.toLowerCase();
  const [co,st]=[sel[0],sel[1]];const [df,dt]=dates;
  let v=0;document.querySelectorAll('#pm-table tbody tr').forEach(r=>{
    const s=(!co||r.dataset.co===co)&&(!st||r.dataset.st===st)&&inDateRange(r.dataset.date,df,dt)&&(!q||r.textContent.toLowerCase().includes(q));
    r.style.display=s?'':'none';if(s)v++;});
  document.getElementById('pm-count').textContent=v+' records';
}
const _sortState={};
function sortTable(tid,col){
  const t=document.getElementById(tid);
  const st=_sortState[tid]||{col:-1,asc:true};
  const asc=st.col===col?!st.asc:true;
  _sortState[tid]={col,asc};
  t.querySelectorAll('thead th').forEach((th,i)=>{if(th.classList.contains('sortable'))th.dataset.sort=i===col?(asc?'asc':'desc'):'';});
  const rows=[...t.querySelectorAll('tbody tr')];
  rows.sort((a,b)=>{
    const av=(a.cells[col]?.textContent||'').trim();
    const bv=(b.cells[col]?.textContent||'').trim();
    const an=parseFloat(av.replace(/[^0-9.-]/g,''));
    const bn=parseFloat(bv.replace(/[^0-9.-]/g,''));
    if(!isNaN(an)&&!isNaN(bn))return asc?an-bn:bn-an;
    return asc?av.localeCompare(bv):bv.localeCompare(av);
  });
  const tb=t.querySelector('tbody');rows.forEach(r=>tb.appendChild(r));
}
renderRun(0);
</script>"""
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>MOPS Monitor</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0;}}
  body{{font-family:'Segoe UI',sans-serif;background:#f5f6fa;color:#1a1a2e;}}
  header{{background:#1F3864;color:#fff;padding:14px 32px;display:flex;align-items:center;gap:20px;flex-wrap:wrap;}}
  header h1{{font-size:1.25rem;font-weight:700;flex-shrink:0;}}
  .run-sel{{display:flex;align-items:center;gap:8px;}}
  .run-sel label{{font-size:.8rem;opacity:.75;white-space:nowrap;}}
  .run-sel select{{padding:4px 10px;border-radius:6px;border:none;font-size:.82rem;background:rgba(255,255,255,.15);color:#fff;cursor:pointer;max-width:360px;}}
  .run-sel select option{{background:#1F3864;}}
  #run-info{{font-size:.78rem;opacity:.65;margin-left:auto;}}
  .metrics{{display:flex;gap:16px;padding:20px 32px;}}
  .metric{{background:#fff;border-radius:8px;padding:14px 20px;flex:1;box-shadow:0 1px 4px rgba(0,0,0,.08);}}
  .metric .num{{font-size:1.9rem;font-weight:700;color:#1F3864;}}
  .metric .lbl{{font-size:.78rem;color:#666;margin-top:4px;}}
  .tabs{{display:flex;padding:0 32px;border-bottom:2px solid #ddd;margin-top:4px;}}
  .tab{{padding:10px 22px;cursor:pointer;font-weight:600;font-size:.88rem;color:#666;border-bottom:3px solid transparent;margin-bottom:-2px;}}
  .tab.active{{color:#1F3864;border-bottom-color:#1F3864;}}
  .panel{{display:none;padding:20px 32px;}}
  .panel.active{{display:block;}}
  .filters{{display:flex;gap:10px;margin-bottom:14px;flex-wrap:wrap;align-items:center;}}
  .filters select,.filters input{{padding:5px 10px;border:1px solid #ddd;border-radius:6px;font-size:.83rem;background:#fff;}}
  .filters label{{font-size:.78rem;color:#666;font-weight:600;}}
  table{{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.08);font-size:.81rem;}}
  th{{background:#1F3864;color:#fff;padding:9px 11px;text-align:left;font-weight:600;white-space:nowrap;}}
  th.sortable{{cursor:pointer;user-select:none;}}
  th.sortable:hover{{background:#2a4a8a;}}
  th.sortable::after{{content:' ⇅';opacity:.35;font-size:.7em;}}
  th.sortable[data-sort=asc]::after{{content:' ▲';opacity:1;}}
  th.sortable[data-sort=desc]::after{{content:' ▼';opacity:1;}}
  td{{padding:8px 11px;border-bottom:1px solid #f0f0f0;vertical-align:top;}}
  tr:last-child td{{border-bottom:none;}}
  tr:hover td{{background:rgba(0,0,0,.02);}}
  .row-new td{{background:#f0fff4;}}.row-historical td{{background:#fff5f5;}}.row-changed td{{background:#fffbf0;}}
  .headline{{max-width:380px;font-style:italic;color:#444;}}
  .badge{{padding:2px 7px;border-radius:4px;font-size:.73rem;font-weight:700;white-space:nowrap;}}
  .badge.new{{background:#C6EFCE;color:#276221;}}.badge.his{{background:#FFC7CE;color:#9C0006;}}.badge.chg{{background:#FFEB9C;color:#9C6500;}}
  .field-chg{{background:#FFEB9C;padding:1px 5px;border-radius:3px;font-weight:600;}}
  .cf-list{{font-size:.75rem;color:#9C6500;font-style:italic;}}
  .chk-wrap{{display:flex;flex-direction:column;gap:2px;min-width:88px;}}
  .chk-btn{{padding:3px 8px;border:none;border-radius:4px;font-size:.75rem;font-weight:700;cursor:pointer;white-space:nowrap;width:100%;}}
  .chk-pend{{background:#FFD700;color:#5a4000;}}.chk-done{{background:#C6EFCE;color:#276221;}}
  .chk-ts{{font-size:.65rem;color:#888;white-space:nowrap;}}
  .chk-name{{width:100%;padding:2px 4px;border:1px solid #ddd;border-radius:3px;font-size:.72rem;box-sizing:border-box;}}
  a{{color:#1F3864;}}.count{{font-size:.78rem;color:#666;margin-bottom:8px;}}
</style>
</head>
<body>
<header>
  <h1>📡 MOPS Monitor</h1>
  <div class="run-sel">
    <label>Run:</label>
    <select onchange="renderRun(+this.value)">
{run_options}
    </select>
  </div>
  <span id="run-info"></span>
</header>
<div class="metrics">
  <div class="metric"><div class="num" id="m-new-fc">—</div><div class="lbl">New Fund Commitments</div></div>
  <div class="metric"><div class="num" id="m-his-fc">—</div><div class="lbl">Historical Commitments</div></div>
  <div class="metric"><div class="num" id="m-new-pm">—</div><div class="lbl">New People Moves</div></div>
  <div class="metric"><div class="num" id="m-chg-pm">—</div><div class="lbl">Changed People Moves</div></div>
  <div class="metric"><div class="num" id="m-chg-em">—</div><div class="lbl">EMOPS Profile Changes</div></div>
</div>
<div class="tabs">
  <div class="tab active" onclick="showTab('em',this)">🏢 Company Profiles</div>
  <div class="tab"        onclick="showTab('fc',this)">💼 Fund Commitments</div>
  <div class="tab"        onclick="showTab('pm',this)">👤 People Moves</div>
</div>
<div id="em" class="panel active">
  <div class="filters">
    <label>Company</label><select class="co-filter" onchange="filterEM()"><option value="">All</option></select>
    <label>Status</label><select onchange="filterEM()"><option value="">All</option><option value="new">NEW</option><option value="changed">CHANGED</option><option value="unchanged">UNCHANGED</option></select>
    <label>Scraped from</label><input type="date" onchange="filterEM()" style="width:140px;">
    <label>to</label><input type="date" onchange="filterEM()" style="width:140px;">
    <input type="text" placeholder="Search…" oninput="filterEM()" style="width:180px;">
  </div>
  <div class="count" id="em-count"></div>
  <table id="em-table"><thead><tr><th class="sortable" onclick="sortTable('em-table',0)">Code</th><th class="sortable" onclick="sortTable('em-table',1)">Company Name</th><th class="sortable" onclick="sortTable('em-table',2)">Type</th><th class="sortable" onclick="sortTable('em-table',3)">BS Period</th><th class="sortable" onclick="sortTable('em-table',4)">Currency</th><th class="sortable" onclick="sortTable('em-table',5)">Total Assets</th><th class="sortable" onclick="sortTable('em-table',6)">Inv. Property</th><th>Telephone</th><th>Website</th><th>Address</th><th class="sortable" onclick="sortTable('em-table',10)">Status</th><th>Changed Fields</th><th class="sortable" onclick="sortTable('em-table',12)">Scraped At</th><th>Action</th></tr></thead><tbody></tbody></table>
</div>
<div id="fc" class="panel">
  <div class="filters">
    <label>Company</label><select class="co-filter" onchange="filterFC()"><option value="">All</option></select>
    <label>Fund Type</label><select id="fc-ft-sel" onchange="filterFC()"><option value="">All</option></select>
    <label>Status</label><select onchange="filterFC()"><option value="">All</option><option value="new">NEW</option><option value="historical">HISTORICAL</option><option value="changed">CHANGED</option></select>
    <label>From</label><input type="date" onchange="filterFC()" style="width:140px;">
    <label>To</label><input type="date" onchange="filterFC()" style="width:140px;">
    <input type="text" placeholder="Search fund name…" oninput="filterFC()" style="width:180px;">
  </div>
  <div class="count" id="fc-count"></div>
  <table id="fc-table"><thead><tr><th class="sortable" onclick="sortTable('fc-table',0)">Code</th><th class="sortable" onclick="sortTable('fc-table',1)">Firm Name</th><th class="sortable" onclick="sortTable('fc-table',2)">Ann. Date</th><th class="sortable" onclick="sortTable('fc-table',3)">Fund Name</th><th class="sortable" onclick="sortTable('fc-table',4)">Fund Type</th><th class="sortable" onclick="sortTable('fc-table',5)">Commit Date</th><th class="sortable" onclick="sortTable('fc-table',6)">Amount (Raw)</th><th>Headline</th><th class="sortable" onclick="sortTable('fc-table',8)">BS Date</th><th class="sortable" onclick="sortTable('fc-table',9)">Status</th><th class="sortable" onclick="sortTable('fc-table',10)">Scraped At</th><th>Action</th></tr></thead><tbody></tbody></table>
</div>
<div id="pm" class="panel">
  <div class="filters">
    <label>Company</label><select class="co-filter" onchange="filterPM()"><option value="">All</option></select>
    <label>Status</label><select onchange="filterPM()"><option value="">All</option><option value="new">NEW</option><option value="historical">HISTORICAL</option><option value="changed">CHANGED</option></select>
    <label>From</label><input type="date" onchange="filterPM()" style="width:140px;">
    <label>To</label><input type="date" onchange="filterPM()" style="width:140px;">
    <input type="text" placeholder="Search name or role…" oninput="filterPM()" style="width:180px;">
  </div>
  <div class="count" id="pm-count"></div>
  <table id="pm-table"><thead><tr><th class="sortable" onclick="sortTable('pm-table',0)">Code</th><th class="sortable" onclick="sortTable('pm-table',1)">Ann. Date</th><th class="sortable" onclick="sortTable('pm-table',2)">Role</th><th class="sortable" onclick="sortTable('pm-table',3)">New Holder</th><th class="sortable" onclick="sortTable('pm-table',4)">Previous Holder</th><th class="sortable" onclick="sortTable('pm-table',5)">Effective Date</th><th>Narrative</th><th>Link</th><th>Action</th></tr></thead><tbody></tbody></table>
</div>
""" + js + "\n</body></html>"

    report_path = OUTPUT_DIR / "report.html"
    report_path.write_text(html, encoding="utf-8")
    logger.info("HTML report saved: %s", report_path)
    return report_path


def write_excel(fund_commitments, people_moves, emops_data=None, since=None, new_since=None):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    path = OUTPUT_DIR / f"MOPSOV_{ts}.xlsx"
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    _link_font = Font(color="0563C1", underline="single", name="Calibri", size=10)

    # Summary
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

    # Fund Commitments — mirrors HTML FC table
    _co_map = {w["stock_code"]: w["name_en"] for w in WATCHLIST}
    ws = wb.create_sheet("FundCommitments")
    _FC_COLS = ["Stock Code", "Firm Name", "Ann. Date", "Fund Name", "Fund Type",
                "Commit Date", "Amount (Raw)", "Headline", "BS Date", "Status", "Scraped At"]
    _header(ws, _FC_COLS)
    _name_col = _FC_COLS.index("Fund Name") + 1
    _amt_col  = _FC_COLS.index("Amount (Raw)") + 1
    for i, r in enumerate(fund_commitments, 2):
        firm = _co_map.get(r.get("stock_code", ""), "")
        ws.append([r.get("stock_code"), firm, r.get("announcement_date"), r.get("fund_name"),
                   r.get("fund_type"), r.get("commitment_date"), r.get("commitment_amount_raw"),
                   r.get("headline"), r.get("bs_date"), r.get("status"), r.get("scraped_at")])
        if r.get("url"):
            cell = ws.cell(i, _name_col)
            cell.hyperlink = r["url"]
            cell.font = _link_font
        if r.get("fx_url"):
            cell = ws.cell(i, _amt_col)
            cell.hyperlink = r["fx_url"]
            cell.font = _link_font
        _status_fill(ws, i, r.get("status"))
    _autofit(ws)

    # People Moves — mirrors HTML PM table
    ws = wb.create_sheet("PeopleMoves")
    _PM_COLS = ["Stock Code", "Ann. Date", "Role", "New Holder", "Previous Holder",
                "Effective Date", "Narrative", "URL", "Status", "Scraped At"]
    _header(ws, _PM_COLS)
    _url_col = _PM_COLS.index("URL") + 1
    for i, r in enumerate(people_moves, 2):
        role = r.get("role_title") or r.get("role_type") or ""
        ws.append([r.get("stock_code"), r.get("announcement_date"), role,
                   r.get("new_holder"), r.get("previous_holder"),
                   r.get("effective_date"), r.get("narrative_en"),
                   r.get("url"), r.get("status"), r.get("scraped_at")])
        if r.get("url"):
            cell = ws.cell(i, _url_col)
            cell.hyperlink = r["url"]
            cell.font = _link_font
        _status_fill(ws, i, r.get("status"))
    _autofit(ws)

    # Company Profiles — mirrors HTML EMOPS table
    if emops_data:
        ws = wb.create_sheet("CompanyProfiles")
        _EM_COLS = ["Stock Code", "Company Name", "Type", "BS Period", "Currency",
                    "Total Assets", "Inv. Property", "Telephone", "Website",
                    "Address", "Status", "Changed Fields", "Scraped At"]
        _header(ws, _EM_COLS)
        _web_col = _EM_COLS.index("Website") + 1
        for i, r in enumerate(emops_data, 2):
            cf = ", ".join(r.get("changed_fields") or [])
            ws.append([r.get("stock_code"), r.get("company_name_en") or r.get("name_en"),
                       r.get("company_type"), r.get("period"), r.get("currency"),
                       r.get("total_assets_raw"), r.get("inv_property_raw"),
                       r.get("telephone"), r.get("web_address"),
                       r.get("address"), r.get("profile_status"), cf, r.get("scraped_at")])
            web = r.get("web_address", "")
            if web:
                url = web if web.startswith("http") else "https://" + web
                cell = ws.cell(i, _web_col)
                cell.hyperlink = url
                cell.font = _link_font
            _status_fill(ws, i, r.get("profile_status"))
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


_EMOPS_TRACK = ["company_name_en", "address", "telephone", "web_address",
                "period", "currency", "total_assets_raw", "inv_property_raw"]

def _load_emops_data() -> list[dict]:
    """Read latest profile + balance sheet archive per company, detect field-level changes."""
    results = []
    for entry in WATCHLIST:
        code = entry["stock_code"]
        profile_files = sorted(ARCHIVE_DIR.glob(f"{code}_profile_*.json"), reverse=True)
        bs_files      = sorted(ARCHIVE_DIR.glob(f"{code}_balance_sheet_*.json"), reverse=True)
        profile, bs = {}, {}
        if profile_files:
            d = _load_json(profile_files[0])
            if d and d.get("records"):
                profile = d["records"][0]
        if bs_files:
            d = _load_json(bs_files[0])
            if d and d.get("records"):
                bs = d["records"][0]

        record = {
            "stock_code":           code,
            "name_en":              entry["name_en"],
            "company_type":         entry.get("company_type", ""),
            "company_name_en":      _clean_company_name(profile.get("company_name_en", "")),
            "address":              _clean_address(profile.get("address", "")),
            "telephone":            _format_tw_phone(profile.get("telephone", "")),
            "web_address":          _clean_web_address(profile.get("web_address", "")),
            "period":               bs.get("period", ""),
            "currency":             bs.get("currency", "").replace(" (thousands)", ""),
            "total_assets_raw":     bs.get("total_assets_raw", ""),
            "total_assets_numeric": bs.get("total_assets_numeric"),
            "inv_property_raw":     bs.get("investment_property_raw", ""),
            "profile_status":       "",
            "changed_fields":       [],
            "scraped_at":           profile.get("scraped_at", ""),
        }

        # Compare with stored state to detect field-level changes
        state_path = STATE_DIR / f"{code}_emops.json"
        prev = _load_json(state_path) or {}
        if not prev:
            record["profile_status"] = "NEW"
        else:
            changed = [f for f in _EMOPS_TRACK
                       if record.get(f) and record.get(f) != prev.get(f)]
            record["profile_status"] = "CHANGED" if changed else "UNCHANGED"
            record["changed_fields"] = changed

        # Persist current values as new baseline
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(
            json.dumps({f: record.get(f, "") for f in _EMOPS_TRACK},
                       indent=2, ensure_ascii=False),
            encoding="utf-8")

        results.append(record)
    return results


async def run(companies=None, export_excel=True, funds_only=False, people_only=False,
              since=None, new_since=None):
    watchlist = WATCHLIST if not companies else [w for w in WATCHLIST if w["stock_code"] in companies]
    logger.info("Running MOPSOV for %d companies", len(watchlist))
    if since:
        logger.info("Extracting fund commitments from %s", since)
    if new_since:
        logger.info("Last-run baseline date: %s (records seen before this run will be HISTORICAL)", new_since)

    all_funds, all_people = [], []

    for entry in watchlist:
        code = entry["stock_code"]
        logger.info("── %s %s", code, entry["name_en"])

        if not people_only:
            funds = await scrape_fund_commitments(code, sdate=since)
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
        emops_data = _load_emops_data()
        write_excel(all_funds, all_people, emops_data, since=since, new_since=new_since)
        write_html_report(all_funds, all_people, emops_data, since=since, new_since=new_since)


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
