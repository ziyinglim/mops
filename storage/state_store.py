"""
Manages persistent JSON state and raw JSON archives.
- State: lightweight hash snapshots per category per company
- Archive: full timestamped extraction records for historical access
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_CATEGORIES = ["company_profile", "balance_sheet", "fund_commitments", "people_moves"]


def get_state_path(state_dir: Path, stock_code: str, category: str) -> Path:
    return state_dir / f"{stock_code}_{category}.json"


def archive_records(archive_dir: Path, stock_code: str, category: str, records: list[dict]) -> Path:
    """Write full records to timestamped archive file. Returns archive path."""
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    filename = f"{stock_code}_{category}_{timestamp}.json"
    path = archive_dir / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "stock_code": stock_code,
        "category": category,
        "scraped_at": now.isoformat(),
        "record_count": len(records),
        "records": records,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Archived %d records to %s", len(records), path)
    return path


def load_latest_balance_sheet(stock_code: str, state_dir: Path) -> dict | None:
    """
    Return the most recent balance sheet record for a company.
    Looks for the latest archive file matching the pattern.
    """
    archive_dir = state_dir.parent / "archive"
    pattern = f"{stock_code}_balance_sheet_*.json"
    files = sorted(archive_dir.glob(pattern), reverse=True)
    if not files:
        logger.debug("No balance sheet archive found for %s", stock_code)
        return None
    try:
        data = json.loads(files[0].read_text(encoding="utf-8"))
        records = data.get("records", [])
        return records[0] if records else None
    except Exception as exc:
        logger.warning("Could not load balance sheet archive for %s: %s", stock_code, exc)
        return None


def load_run_summary(state_dir: Path) -> dict:
    """Load a summary of last run times per company/category."""
    summary_path = state_dir / "run_summary.json"
    if summary_path.exists():
        try:
            return json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_run_summary(state_dir: Path, summary: dict) -> None:
    summary_path = state_dir / "run_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
