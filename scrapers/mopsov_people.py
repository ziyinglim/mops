"""
Scrapes people moves from mopsov ezsearch.
Categories: B00 > B02 and B00 > M08 (Change in Key Personnel)
Filters by C-suite keywords, parses all 7 statement fields.
"""
import logging
import re
from playwright.async_api import Page
from .base import MOPSSession, with_retry
from .mopsov_fund import _search_announcements, _matches_keywords, _extract_statement
from processors.statement_parser import parse_statement_fields

logger = logging.getLogger(__name__)


async def scrape_people_moves(
    session: MOPSSession,
    stock_code: str,
    keywords: list[str],
) -> list[dict]:
    async def _run():
        page = await session.new_page()
        all_announcements = []
        try:
            for subcategory in ["B02", "M08"]:
                try:
                    announcements = await _search_announcements(page, stock_code, "B00", subcategory)
                    all_announcements.extend(announcements)
                except Exception as exc:
                    logger.warning("B00/%s search failed for %s: %s", subcategory, stock_code, exc)

            results = []
            for ann in all_announcements:
                if not _matches_keywords(ann.get("summary", ""), keywords):
                    continue
                detail = await _scrape_people_detail(session, ann["url"], stock_code)
                if detail:
                    results.append(detail)

            logger.info("Found %d people moves for %s", len(results), stock_code)
            return results
        except Exception as exc:
            logger.error("People moves scrape failed for %s: %s", stock_code, exc)
            raise
        finally:
            await page.close()

    return await with_retry(_run)


async def _scrape_people_detail(session: MOPSSession, url: str, stock_code: str) -> dict | None:
    if not url:
        return None

    detail_page = await session.new_page()
    try:
        await detail_page.goto(url)
        await detail_page.wait_for_load_state("networkidle")

        statement_text = await _extract_statement(detail_page)
        if not statement_text:
            logger.warning("Empty people moves statement at %s", url)
            return None

        fields = parse_statement_fields(statement_text)

        # Field 1: Role type
        role_type = _clean_field_header(fields.get(1, ""), "Type of personnel changed")
        # Field 2: Date of change
        date_raw = _clean_field_header(fields.get(2, ""), "Date of occurrence of the change")
        date_match = re.search(r"\d{4}/\d{2}/\d{2}|\d{4}-\d{2}-\d{2}", date_raw)
        change_date = date_match.group(0) if date_match else date_raw.strip()
        # Field 3: Previous holder
        previous_holder = _clean_field_header(fields.get(3, ""), "Name, title, and resume of the previous position holder")
        # Field 4: New holder
        new_holder = _clean_field_header(fields.get(4, ""), "Name, title, and resume of the new position holder")
        # Field 5: Change type
        change_type = _clean_field_header(fields.get(5, ""), "Type of the change")
        # Field 6: Reason
        reason = _clean_field_header(fields.get(6, ""), "Reason for the change")
        # Field 7: Effective date
        effective_raw = _clean_field_header(fields.get(7, ""), "Effective date")
        eff_match = re.search(r"\d{4}/\d{2}/\d{2}|\d{4}-\d{2}-\d{2}", effective_raw)
        effective_date = eff_match.group(0) if eff_match else effective_raw.strip()

        # Determine if this is a C-suite role we care about
        if not _matches_keywords(role_type, _CSUITE_ROLES):
            logger.debug("Skipping non-C-suite role: %s", role_type)
            return None

        return {
            "stock_code": stock_code,
            "announcement_url": url,
            "role_type": role_type.strip(),
            "change_date": change_date,
            "previous_holder": previous_holder.strip(),
            "new_holder": new_holder.strip(),
            "change_type": change_type.strip(),
            "reason": reason.strip(),
            "effective_date": effective_date,
            "statement_raw": statement_text,
        }
    except Exception as exc:
        logger.error("People detail scrape failed for %s: %s", url, exc)
        return None
    finally:
        await detail_page.close()


def _clean_field_header(text: str, header_hint: str) -> str:
    """Remove the field label prefix if present, return just the value."""
    if not text:
        return ""
    # Remove patterns like "Type of personnel changed (please enter: ...):"
    # by splitting on the first colon after a long label
    if ":" in text:
        parts = text.split(":", 1)
        # If the part before colon looks like a label (>10 chars), discard it
        if len(parts[0]) > 10:
            return parts[1].strip()
    return text.strip()


_CSUITE_ROLES = [
    "Chief Executive Officer", "Chief Investment Officer",
    "Chief Financial Officer", "Chief Risk Officer",
    "Chief Operating Officer", "Chief Information Officer",
    "Chief Marketing Officer", "Chief Strategy Officer",
    "Chief Internal Auditor", "General Manager", "President",
    "CEO", "CIO", "CFO", "CRO", "COO", "CMO", "CSO",
    "執行長", "投資長", "財務長", "風控長", "總經理",
]
