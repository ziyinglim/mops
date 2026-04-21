"""
Scrapes balance sheet from emops via direct POST to t164sb03_e.
No browser needed — mirrors the Balance Sheet button onclick exactly.
"""
import logging
import re
from bs4 import BeautifulSoup
from .emops_http import post_emops

logger = logging.getLogger(__name__)


async def scrape_balance_sheet(session, stock_code: str) -> dict:
    html = await post_emops(
        "/server-java/t164sb03_e",
        stock_code=stock_code,
        extra={"step": "current"},
    )
    if not html:
        return {"stock_code": stock_code, "error": "No response", "status": "ERROR"}

    soup = BeautifulSoup(html, "lxml")
    period = _extract_period(soup)
    currency = _extract_currency(soup)

    total_assets = _find_balance_value(soup, ["資產總計", "Total assets", "Total Assets", "總資產"])
    investment_property = _find_balance_value(soup, [
        "投資性不動產淨額", "Investment property, net",
        "Investment property", "投資性不動產",
    ])

    logger.info("Balance sheet for %s: assets=%s", stock_code, total_assets)
    return {
        "stock_code": stock_code,
        "period": period,
        "currency": currency,
        "total_assets_raw": total_assets,
        "total_assets_numeric": _parse_number(total_assets),
        "investment_property_raw": investment_property,
        "investment_property_numeric": _parse_number(investment_property),
    }


def _extract_period(soup: BeautifulSoup) -> str:
    for tag in soup.find_all(["h2", "h3", "caption", "title"]):
        text = tag.get_text(strip=True)
        if text:
            return text
    return ""


def _extract_currency(soup: BeautifulSoup) -> str:
    text = soup.get_text()
    if "千元" in text or "thousands" in text.lower():
        return "TWD (thousands)"
    if "USD" in text:
        return "USD"
    return "TWD"


def _find_balance_value(soup: BeautifulSoup, labels: list[str]) -> str:
    for label in labels:
        # Strip full-width spaces (U+3000) that prefix balance sheet labels
        for cell in soup.find_all("td", string=lambda t: t and label.lower() in t.replace("\u3000", "").strip().lower()):
            # Value is the next sibling td in the same row
            value_td = cell.find_next_sibling("td")
            if value_td:
                val = value_td.get_text(strip=True)
                if val:
                    return val
    return ""


def _parse_number(raw: str) -> float | None:
    if not raw:
        return None
    cleaned = raw.replace(",", "").replace(" ", "")
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = "-" + cleaned[1:-1]
    try:
        return float(cleaned)
    except ValueError:
        return None
