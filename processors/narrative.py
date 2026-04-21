"""
Generates human-readable English narrative sentences for key events.
People Moves: "The TWD X bn investor has appointed Y as CIO, effective D."
AUM is sourced from the latest stored balance sheet for the company.
"""
import logging
from pathlib import Path
from storage.state_store import load_latest_balance_sheet

logger = logging.getLogger(__name__)

_BILLION = 1_000_000_000
_MILLION = 1_000_000


def build_people_move_narrative(
    record: dict,
    watchlist_entry: dict,
    state_dir: Path,
) -> str:
    """
    Build a narrative sentence from a people_moves record.
    Pulls AUM from latest balance sheet if available.
    """
    company_name = watchlist_entry.get("name_en", "The investor")
    stock_code = record.get("stock_code", "")
    role = record.get("role_type", "key position")
    new_holder = record.get("new_holder", "")
    previous_holder = record.get("previous_holder", "")
    change_type = record.get("change_type", "").lower()
    effective_date = _format_date(record.get("effective_date", ""))

    # Get AUM from latest balance sheet
    aum_str = _get_aum_string(stock_code, state_dir)
    company_ref = f"The {aum_str} {company_name}" if aum_str else f"{company_name}"

    # Build action phrase
    if "new replacement" in change_type or "appointment" in change_type:
        action = f"has appointed {new_holder} as its new {role}"
    elif "resignation" in change_type:
        action = f"'s {role}, {previous_holder}, has resigned"
    elif "retirement" in change_type:
        action = f"'s {role}, {previous_holder}, has retired"
    elif "position adjustment" in change_type:
        action = f"has reassigned {previous_holder or new_holder} to the role of {role}"
    elif "dismissal" in change_type:
        action = f"has dismissed {previous_holder} from the role of {role}"
    else:
        action = f"has announced a change in its {role}: {new_holder}"

    sentence = f"{company_ref} {action}"
    if effective_date:
        sentence += f", effective {effective_date}"
    sentence += "."

    return sentence


def _get_aum_string(stock_code: str, state_dir: Path) -> str:
    """Load latest balance sheet and return formatted AUM string like 'TWD 130bn'."""
    try:
        bs = load_latest_balance_sheet(stock_code, state_dir)
        if not bs:
            return ""
        total_assets = bs.get("total_assets_numeric")
        currency = bs.get("currency", "TWD").replace(" (thousands)", "")
        if total_assets is None:
            return ""

        # If stored in thousands (common for TWD filings), convert
        if "thousands" in bs.get("currency", ""):
            total_assets *= 1000

        if total_assets >= _BILLION:
            amount = f"{total_assets / _BILLION:.0f}bn"
        elif total_assets >= _MILLION:
            amount = f"{total_assets / _MILLION:.0f}mn"
        else:
            amount = f"{total_assets:,.0f}"

        return f"{currency} {amount}"
    except Exception as exc:
        logger.warning("Could not load AUM for %s: %s", stock_code, exc)
        return ""


def _format_date(date_str: str) -> str:
    """Convert YYYY/MM/DD or YYYY-MM-DD to '6 March 2026' format."""
    import re
    from datetime import datetime
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%-d %B %Y")
        except (ValueError, AttributeError):
            continue
    return date_str
