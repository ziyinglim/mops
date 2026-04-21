"""
Scrapes company profile from emops via direct POST to t146sb05_e.
No browser needed — mirrors the onclick form submission exactly.
"""
import logging
import httpx
from bs4 import BeautifulSoup
from .emops_http import post_emops, TYPEK_OPTIONS

logger = logging.getLogger(__name__)


async def scrape_profile(session, stock_code: str) -> dict:
    """POST directly to company profile endpoint, parse result with BeautifulSoup."""
    html = await post_emops(
        "/server-java/t146sb05_e",
        stock_code=stock_code,
        extra={"step": "0"},
    )
    if not html:
        return {"stock_code": stock_code, "error": "No response", "status": "ERROR"}

    soup = BeautifulSoup(html, "lxml")
    result = {
        "stock_code": stock_code,
        "company_name_zh": _find_field(soup, ["公司名稱", "Company Name"]),
        "company_name_en": _find_field(soup, ["英文簡稱", "English Name", "英文名稱"]),
        "address": _find_address(soup) or _find_field(soup, ["地址", "Address"]),
        "telephone": _find_field(soup, ["電話", "Telephone"]),
        "web_address": _find_field(soup, ["網址", "Web Address", "Website"]),
    }
    logger.info("Profile scraped for %s: %s", stock_code, result.get("company_name_zh") or result.get("company_name_en"))
    return result


def _find_field(soup: BeautifulSoup, labels: list[str]) -> str:
    """
    Handles two layouts:
    1. Column layout: header cell on top, value in next row same column position
    2. Row layout: header cell left, value cell right (fallback)
    """
    for label in labels:
        for cell in soup.find_all(["td", "th"], string=lambda t: t and label in t):
            row = cell.find_parent("tr")
            if not row:
                continue
            # Column layout — find position, get value from next row
            siblings = row.find_all(["td", "th"])
            try:
                col_idx = siblings.index(cell)
            except ValueError:
                continue
            next_row = row.find_next_sibling("tr")
            if next_row:
                next_cells = next_row.find_all(["td", "th"])
                if col_idx < len(next_cells):
                    val = next_cells[col_idx].get_text(strip=True)
                    if val and val != label:
                        return val
            # Row layout fallback
            tds = row.find_all("td")
            if len(tds) >= 2:
                val = tds[-1].get_text(strip=True)
                if val and val != label:
                    return val
    return ""


def _find_address(soup: BeautifulSoup) -> str:
    """Address is in a td[colspan='6'] inside tr.odd.center."""
    # Primary: colspan=6 cell (confirmed HTML structure)
    for td in soup.find_all("td", attrs={"colspan": True}):
        text = td.get_text(strip=True)
        if text and len(text) > 5:
            row = td.find_parent("tr")
            if row and ("odd" in row.get("class", []) or "center" in row.get("class", [])):
                return text
    # Fallback: any td with colspan >= 4 that looks like an address
    for td in soup.find_all("td", attrs={"colspan": True}):
        text = td.get_text(strip=True)
        if text and ("Road" in text or "St" in text or "Ave" in text or "路" in text or "街" in text):
            return text
