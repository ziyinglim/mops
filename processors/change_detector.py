"""
SHA-256 based change detection.
Compares current extracted records against stored state snapshots.
Flags each record as NEW, CHANGED, or UNCHANGED.
"""
import hashlib
import json
import logging
from pathlib import Path
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

STATUS_NEW = "NEW"
STATUS_CHANGED = "CHANGED"
STATUS_UNCHANGED = "UNCHANGED"


def compute_hash(record: dict) -> str:
    """SHA-256 of the record's content fields (excluding metadata like run timestamps)."""
    # Exclude keys that change every run but don't reflect content changes
    exclude = {"scraped_at", "run_id", "status", "hash"}
    stable = {k: v for k, v in record.items() if k not in exclude}
    canonical = json.dumps(stable, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def detect_changes(
    records: list[dict],
    state_path: Path,
    key_fields: list[str],
) -> list[dict]:
    """
    Compare `records` against stored state at `state_path`.
    Each record is tagged with a `status` field (NEW / CHANGED / UNCHANGED).
    Saves updated state back to disk.

    `key_fields`: list of field names that form the unique key for a record,
                  e.g. ["stock_code", "fund_name", "commitment_date"]
    """
    stored = _load_state(state_path)
    updated_store = dict(stored)
    now = datetime.now(timezone.utc).isoformat()

    tagged = []
    for record in records:
        record_key = _make_key(record, key_fields)
        current_hash = compute_hash(record)
        record["hash"] = current_hash
        record["scraped_at"] = now

        if record_key not in stored:
            record["status"] = STATUS_NEW
            logger.info("NEW record: %s", record_key)
        elif stored[record_key]["hash"] != current_hash:
            record["status"] = STATUS_CHANGED
            record["previous_hash"] = stored[record_key]["hash"]
            logger.info("CHANGED record: %s", record_key)
        else:
            record["status"] = STATUS_UNCHANGED

        updated_store[record_key] = {
            "hash": current_hash,
            "last_seen": now,
            "key": record_key,
        }
        tagged.append(record)

    _save_state(state_path, updated_store)
    new_count = sum(1 for r in tagged if r["status"] == STATUS_NEW)
    changed_count = sum(1 for r in tagged if r["status"] == STATUS_CHANGED)
    logger.info(
        "Change detection complete — %d new, %d changed, %d unchanged",
        new_count, changed_count, len(tagged) - new_count - changed_count,
    )
    return tagged


def _make_key(record: dict, key_fields: list[str]) -> str:
    parts = [str(record.get(f, "")) for f in key_fields]
    return "|".join(parts)


def _load_state(state_path: Path) -> dict:
    if state_path.exists():
        try:
            return json.loads(state_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Could not load state from %s: %s", state_path, exc)
    return {}


def _save_state(state_path: Path, state: dict) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(state, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
