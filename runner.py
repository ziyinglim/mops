"""
Main orchestrator for one full MOPS extraction run.
Flow: load config → scrape each company → detect changes → translate → write Excel → archive
"""
import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import yaml

from scrapers.base import MOPSSession
from scrapers.emops_profile import scrape_profile
from scrapers.emops_balance_sheet import scrape_balance_sheet
from scrapers.mopsov_fund import scrape_fund_commitments
from scrapers.mopsov_people import scrape_people_moves
from processors.change_detector import detect_changes
from processors.translator import translate_record
from processors.narrative import build_people_move_narrative
from storage.state_store import archive_records, get_state_path, load_run_summary, save_run_summary
from output.excel_writer import write_excel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("runner")

BASE_DIR = Path(__file__).parent
CONFIG_DIR = BASE_DIR / "config"
STORAGE_DIR = BASE_DIR / "storage"
STATE_DIR = STORAGE_DIR / "state"
ARCHIVE_DIR = STORAGE_DIR / "archive"


def load_config():
    settings = yaml.safe_load((CONFIG_DIR / "settings.yaml").read_text(encoding="utf-8"))
    watchlist = json.loads((CONFIG_DIR / "watchlist.json").read_text(encoding="utf-8"))
    keywords = yaml.safe_load((CONFIG_DIR / "keywords.yaml").read_text(encoding="utf-8"))
    return settings, watchlist, keywords


async def run_all(headless: bool = True, companies: list[str] | None = None):
    settings, watchlist, keywords = load_config()

    # Optional: limit to specific stock codes for testing
    if companies:
        watchlist = [w for w in watchlist if w["stock_code"] in companies]

    logger.info("Starting run for %d companies", len(watchlist))

    all_profiles = []
    all_balance_sheets = []
    all_fund_commitments = []
    all_people_moves = []

    async with MOPSSession(
        headless=headless,
        timeout_ms=settings["scraper"]["timeout_ms"],
    ) as session:
        for entry in watchlist:
            code = entry["stock_code"]
            logger.info("─── Processing %s (%s) ───", code, entry["name_en"])

            # 1. Company Profile
            if settings["categories"]["company_profile"]["enabled"]:
                try:
                    profile = await scrape_profile(session, code)
                    profile = translate_record(profile, ["company_name_zh", "address"])
                    all_profiles.append(profile)
                    archive_records(ARCHIVE_DIR, code, "company_profile", [profile])
                except Exception as exc:
                    logger.error("Profile failed for %s: %s", code, exc)
                    all_profiles.append({"stock_code": code, "error": str(exc), "status": "ERROR"})

            # 2. Balance Sheet
            if settings["categories"]["balance_sheet"]["enabled"]:
                try:
                    bs = await scrape_balance_sheet(session, code)
                    all_balance_sheets.append(bs)
                    archive_records(ARCHIVE_DIR, code, "balance_sheet", [bs])
                except Exception as exc:
                    logger.error("Balance sheet failed for %s: %s", code, exc)
                    all_balance_sheets.append({"stock_code": code, "error": str(exc), "status": "ERROR"})

            # 3. Fund Commitments
            if settings["categories"]["fund_commitments"]["enabled"]:
                try:
                    funds = await scrape_fund_commitments(
                        session, code, keywords["fund_commitments"]["include"]
                    )
                    funds = detect_changes(
                        funds,
                        get_state_path(STATE_DIR, code, "fund_commitments"),
                        key_fields=["stock_code", "fund_name", "commitment_date"],
                    )
                    funds = [translate_record(f, ["fund_name", "fund_type"]) for f in funds]
                    all_fund_commitments.extend(funds)
                    archive_records(ARCHIVE_DIR, code, "fund_commitments", funds)
                except Exception as exc:
                    logger.error("Fund commitments failed for %s: %s", code, exc)

            # 4. People Moves
            if settings["categories"]["people_moves"]["enabled"]:
                try:
                    moves = await scrape_people_moves(
                        session, code, keywords["people_moves"]["include"]
                    )
                    moves = detect_changes(
                        moves,
                        get_state_path(STATE_DIR, code, "people_moves"),
                        key_fields=["stock_code", "role_type", "change_date"],
                    )
                    for move in moves:
                        move["narrative_en"] = build_people_move_narrative(move, entry, STATE_DIR)
                    all_people_moves.extend(moves)
                    archive_records(ARCHIVE_DIR, code, "people_moves", moves)
                except Exception as exc:
                    logger.error("People moves failed for %s: %s", code, exc)

    # Print results to terminal
    _print_results(all_profiles, all_balance_sheets)

    # Write Excel output
    date_str = datetime.now().strftime("%Y%m%d_%H%M")
    excel_filename = settings["output"]["excel_filename"].format(date=date_str)
    excel_path = BASE_DIR / "output" / excel_filename
    write_excel(
        excel_path,
        profiles=all_profiles,
        balance_sheets=all_balance_sheets,
        fund_commitments=all_fund_commitments,
        people_moves=all_people_moves,
        watchlist=watchlist,
    )

    # Update run summary
    summary = load_run_summary(STATE_DIR)
    summary["last_run"] = datetime.now().isoformat()
    summary["companies_processed"] = len(watchlist)
    summary["new_fund_commitments"] = sum(1 for r in all_fund_commitments if r.get("status") == "NEW")
    summary["new_people_moves"] = sum(1 for r in all_people_moves if r.get("status") == "NEW")
    save_run_summary(STATE_DIR, summary)

    logger.info(
        "Run complete. Excel: %s | New funds: %d | New moves: %d",
        excel_path,
        summary["new_fund_commitments"],
        summary["new_people_moves"],
    )
    return excel_path


def _print_results(profiles: list[dict], balance_sheets: list[dict]):
    print("\n" + "═" * 80)
    print(f"  COMPANY PROFILES ({len(profiles)} firms)")
    print("═" * 80)
    for p in profiles:
        print(f"\n  [{p.get('stock_code')}] {p.get('company_name_en') or p.get('company_name_zh', 'N/A')}")
        print(f"    Address   : {p.get('address', 'N/A')}")
        print(f"    Telephone : {p.get('telephone', 'N/A')}")
        print(f"    Website   : {p.get('web_address', 'N/A')}")
        if p.get("error"):
            print(f"    ERROR     : {p['error']}")

    print("\n" + "═" * 80)
    print(f"  BALANCE SHEETS ({len(balance_sheets)} firms)")
    print("═" * 80)
    for b in balance_sheets:
        print(f"\n  [{b.get('stock_code')}] {b.get('period', '')}")
        print(f"    Currency            : {b.get('currency', 'N/A')}")
        print(f"    Total Assets        : {b.get('total_assets_raw', 'N/A')}")
        print(f"    Investment Property : {b.get('investment_property_raw', 'N/A')}")
        if b.get("error"):
            print(f"    ERROR               : {b['error']}")
    print("\n" + "═" * 80 + "\n")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="TWSE MOPS Monitor")
    parser.add_argument("--visible", action="store_true", help="Run browser in visible mode (non-headless)")
    parser.add_argument("--companies", nargs="+", help="Limit run to specific stock codes e.g. 2882 2330")
    args = parser.parse_args()

    asyncio.run(run_all(headless=not args.visible, companies=args.companies))
