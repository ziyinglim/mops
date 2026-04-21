"""
Scrapes fund commitment announcements from mopsov ezsearch.
Category: G00 > M20 (Material Acquisition or Disposal of Assets)
Filters results by fund/acquisition keywords, then parses fields 1, 2, 5.
"""
import logging
import re
from playwright.async_api import Page
from .base import MOPSSession, with_retry, switch_to_english
from processors.statement_parser import parse_statement_fields

logger = logging.getLogger(__name__)

MOPSOV_SEARCH = "https://mopsov.twse.com.tw/mops/web/ezsearch"


async def scrape_fund_commitments(
    session: MOPSSession,
    stock_code: str,
    keywords: list[str],
) -> list[dict]:
    async def _run():
        page = await session.new_page()
        try:
            announcements = await _search_announcements(page, stock_code, "G00", "M20")
            results = []
            for ann in announcements:
                if not _matches_keywords(ann.get("summary", ""), keywords):
                    continue
                detail = await _scrape_detail(session, ann["url"], stock_code)
                if detail:
                    results.append(detail)
            logger.info("Found %d fund commitments for %s", len(results), stock_code)
            return results
        except Exception as exc:
            logger.error("Fund commitment scrape failed for %s: %s", stock_code, exc)
            raise
        finally:
            await page.close()

    return await with_retry(_run)


async def _search_announcements(
    page: Page, stock_code: str, category: str, subcategory: str
) -> list[dict]:
    """Submit search form and return list of {url, date, summary} for result rows."""
    await page.goto(MOPSOV_SEARCH)
    await page.wait_for_load_state("networkidle")
    await switch_to_english(page)

    # Fill the visible search input
    await page.locator('input#pro_co_id').fill(stock_code)

    # Use JS to set the hidden category fields and submit directly,
    # bypassing the dropdown tree which stays hidden after click
    submitted = await page.evaluate(f"""
        (function() {{
            // Find all hidden inputs that hold the declaration category
            var selects = document.querySelectorAll(
                'input[name="declare_item"], input[name="declaration"], '
                'input[name="category"], input[name="m_category"]'
            );
            selects.forEach(function(el) {{ el.value = '{subcategory}'; }});

            // Also try setting by known mopsov field names
            var trySet = function(name, val) {{
                var el = document.querySelector('input[name="' + name + '"]');
                if (el) el.value = val;
            }};
            trySet('co_id', '{stock_code}');
            trySet('declare_item', '{subcategory}');
            trySet('b_date', '');
            trySet('e_date', '');

            // Find and click the submit button or submit the form
            var btn = document.querySelector(
                'input[type="submit"], button[type="submit"], '
                'a[onclick*="search"], input[value="查詢"]'
            );
            if (btn) {{ btn.click(); return 'clicked:' + btn.tagName; }}

            // Fallback: submit the search form directly
            var form = document.querySelector('form[name="search_form"], form[name="fm1"], form');
            if (form) {{ form.submit(); return 'submitted form'; }}
            return 'no submit found';
        }})()
    """)
    logger.info("mopsov submit result: %s", submitted)
    await page.wait_for_load_state("networkidle")

    return await _parse_results_table(page)


async def _parse_results_table(page: Page) -> list[dict]:
    """Extract announcement rows from search results table."""
    rows = page.locator("table tr").filter(has=page.locator("td"))
    count = await rows.count()
    results = []

    for i in range(count):
        row = rows.nth(i)
        try:
            # Find the announcement link
            link = row.locator("a").first
            if await link.count() == 0:
                continue
            href = await link.get_attribute("href") or ""
            text = (await link.inner_text()).strip()

            # Get date from row (usually first or second td)
            tds = row.locator("td")
            td_count = await tds.count()
            date_str = ""
            for j in range(min(3, td_count)):
                candidate = (await tds.nth(j).inner_text()).strip()
                if re.match(r"\d{4}/\d{2}/\d{2}|\d{8}", candidate):
                    date_str = candidate
                    break

            # Full URL handling: relative paths need base prepended
            if href and not href.startswith("http"):
                href = "https://emops.twse.com.tw" + href

            results.append({"url": href, "date": date_str, "summary": text})
        except Exception:
            continue

    return results


def _matches_keywords(text: str, keywords: list[str]) -> bool:
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in keywords)


async def _scrape_detail(session: MOPSSession, url: str, stock_code: str) -> dict | None:
    """Open announcement detail page/popup and parse fund fields 1, 2, 5."""
    if not url:
        return None

    detail_page = await session.new_page()
    try:
        await detail_page.goto(url)
        await detail_page.wait_for_load_state("networkidle")

        # Extract the Statement column content
        statement_text = await _extract_statement(detail_page)
        if not statement_text:
            logger.warning("Empty statement at %s", url)
            return None

        fields = parse_statement_fields(statement_text)

        raw_field1 = fields.get(1, "")
        raw_field2 = fields.get(2, "")
        raw_field5 = fields.get(5, "")

        # Parse fund name and type from field 1
        # e.g. "Inflexion Buyout Fund VII; Private Equity Fund"
        parts = [p.strip() for p in raw_field1.split(";")]
        fund_name = parts[0] if parts else raw_field1
        fund_type = parts[1] if len(parts) > 1 else ""

        # Parse date from field 2
        date_match = re.search(r"(\d{4}/\d{2}/\d{2})", raw_field2)
        commitment_date = date_match.group(1) if date_match else ""

        # Parse amount from field 5 — keep raw, extract currency + number
        amount_raw = _extract_amount_section(raw_field5)

        return {
            "stock_code": stock_code,
            "announcement_url": url,
            "fund_name": fund_name,
            "fund_type": fund_type,
            "commitment_date": commitment_date,
            "commitment_amount_raw": amount_raw,
            "commitment_amount_numeric": _parse_commitment_amount(amount_raw),
            "commitment_currency": _extract_currency(amount_raw),
            "statement_raw": statement_text,
        }
    except Exception as exc:
        logger.error("Detail scrape failed for %s: %s", url, exc)
        return None
    finally:
        await detail_page.close()


async def _extract_statement(page: Page) -> str:
    """Extract the Statement column text from the announcement detail page."""
    # Try common layouts: table with 'Statement' header, or td.statement
    candidates = [
        'td:has-text("Statement") + td',
        'th:has-text("Statement") ~ td',
        'td.statement',
        # Sometimes content is in a specific table column position
        'table tr td:nth-child(2)',
    ]
    for selector in candidates:
        try:
            el = page.locator(selector).first
            if await el.count() > 0:
                text = (await el.inner_text()).strip()
                if len(text) > 20:
                    return text
        except Exception:
            continue

    # Fallback: grab all visible table cell text that looks like numbered fields
    try:
        all_text = await page.inner_text("body")
        if re.search(r"1\.\s*\w", all_text):
            return all_text
    except Exception:
        pass

    return ""


def _extract_amount_section(raw: str) -> str:
    """Extract the last meaningful segment of field 5 (the monetary amount)."""
    if not raw:
        return ""
    # Field 5 format: "description; unit price; total amount"
    parts = [p.strip() for p in raw.split(";")]
    # Return last non-NA part
    for part in reversed(parts):
        if part and part.upper() not in ("NA", "N/A", ""):
            return part
    return raw.strip()


def _extract_currency(text: str) -> str:
    match = re.search(r"\b(USD|EUR|GBP|JPY|TWD|HKD|SGD|AUD|CAD)\b", text)
    return match.group(1) if match else "Unknown"


def _parse_commitment_amount(text: str) -> float | None:
    """Extract numeric value from amount string like 'EUR 25,000,000'."""
    match = re.search(r"[\d,]+(?:\.\d+)?", text.replace(" ", ""))
    if match:
        try:
            return float(match.group().replace(",", ""))
        except ValueError:
            pass
    return None
