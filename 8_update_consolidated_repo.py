"""
Update Consolidated Repo Script.

This module reads the migration blueprint (temp-data/migration_blueprint.csv)
and moves soft-deleted legacy rule files from the active repository
(defined by REPO_PATH in .env) into the archived directory. It records
completed MLS targets in the persistent ledger file (temp-data/processed_mls_ledger.json)
and writes log entries to 'logs/pipeline_YYYY-MM-DD.log'.

Usage:
    python 8_update_consolidated_repo.py
"""

import os
import re
import sys
import json
import shutil
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from pipeline_logger import setup_logger

load_dotenv()
logger = setup_logger("Stage8_RepoArchive")


def sanitize_filename(raw_name: str) -> str:
    """Matches Script 1's filename sanitization logic."""
    clean = raw_name.replace("/", "__").replace("\\", "__")
    clean = re.sub(r'[\x00-\x1f:*?"<>|]', '', clean).strip()

    if not clean:
        clean = "unnamed_rule"

    if not clean.lower().endswith(".py"):
        clean = f"{clean}.py"

    return clean


def load_old_rule_names(blueprint_path: Path) -> list[str]:
    """Parses the migration blueprint and returns legacy rule names."""
    if not blueprint_path.exists():
        logger.error(f"Blueprint file not found at '{blueprint_path}'")
        sys.exit(1)

    df = pd.read_csv(blueprint_path) if blueprint_path.suffix == ".csv" else pd.read_excel(blueprint_path)

    column_name = "old_name"
    if column_name not in df.columns:
        logger.error(f"Required column '{column_name}' missing from blueprint.")
        sys.exit(1)

    return df[column_name].dropna().astype(str).unique().tolist()


def archive_legacy_rules(blueprint_path: Path, repo_base_path: Path) -> bool:
    """Moves legacy rules from active/ to archived/ using REPO_PATH."""
    old_rule_names = load_old_rule_names(blueprint_path)
    logger.info(f"Loaded {len(old_rule_names)} legacy rule name(s) from blueprint.")

    ui_rules_root = repo_base_path / "ui-rules"
    active_dir = ui_rules_root / "active"
    archived_dir = ui_rules_root / "archived"

    if not active_dir.exists():
        logger.error(f"Active directory not found at '{active_dir}'")
        return False

    archived_dir.mkdir(parents=True, exist_ok=True)

    archived_count = 0
    missing_count = 0

    for rule_name in old_rule_names:
        found = False

        # Look for exact or collisions in active/
        for item in active_dir.glob("*.py"):
            if item.stem == rule_name or item.stem.startswith(f"{rule_name}_collision_"):
                destination = archived_dir / item.name
                shutil.move(str(item), str(destination))
                logger.info(f"Archived: {item.name} ==> ui-rules/archived/{item.name}")
                archived_count += 1
                found = True

        if not found:
            logger.warning(f"Rule file for '{rule_name}' was not found in '{active_dir}'")
            missing_count += 1

    logger.info(f"Summary: Archived {archived_count} file(s), Missing {missing_count} file(s)")
    return True


def update_processed_ledger() -> None:
    """Appends currently processed MLS IDs from test_mls.txt to the ledger."""
    test_mls_file = Path("temp-data/test_mls.txt")
    ledger_file = Path("temp-data/processed_mls_ledger.json")

    if not test_mls_file.exists():
        return

    # Read current batch MLS IDs
    current_mls = [line.strip() for line in test_mls_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not current_mls:
        return

    # Read existing completed MLS IDs
    completed_mls = set()
    if ledger_file.exists():
        try:
            data = json.loads(ledger_file.read_text(encoding="utf-8"))
            completed_mls = set(data.get("completed_mls_ids", []))
        except Exception:
            pass

    # Update and save ledger
    completed_mls.update(current_mls)
    ledger_file.parent.mkdir(parents=True, exist_ok=True)
    ledger_file.write_text(json.dumps({"completed_mls_ids": sorted(list(completed_mls))}, indent=2), encoding="utf-8")

    logger.info(f"Ledger Updated: {len(current_mls)} MLS ID(s) ({', '.join(current_mls)}) recorded in '{ledger_file}'.")


def cleanup_temp_directories(temp_dirs: list[Path]) -> None:
    """Removes temporary working directories after sync finishes."""
    logger.info("Cleaning up temporary working files...")
    for temp_dir in temp_dirs:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
            logger.info(f"Removed folder: '{temp_dir}'")
        else:
            logger.info(f"Folder '{temp_dir}' is already clean.")


def main() -> None:
    BLUEPRINT_FILE = Path("temp-data/migration_blueprint.csv")

    repo_base_raw = os.getenv("REPO_PATH", "")
    repo_base = repo_base_raw.strip().strip("'\"")

    if not repo_base:
        logger.error("REPO_PATH not set in environment.")
        return

    REPO_PATH = Path(repo_base)
    TEMP_DIRS_TO_REMOVE = [Path("temp-data")]

    logger.info("--- 📂 dm-consolidated-rules Archive Sync Tool ---")
    logger.info(f"Target REPO_PATH: {REPO_PATH}")

    success = archive_legacy_rules(BLUEPRINT_FILE, REPO_PATH)

    if not success:
        logger.error("Sync halted due to error.")
        return

    # Update batch tracking ledger after successful archive
    update_processed_ledger()

    confirm = input("\nDo you want to delete temporary working files ('temp-data/')? (y/N): ")
    if confirm.lower() == "y":
        cleanup_temp_directories(TEMP_DIRS_TO_REMOVE)
        logger.info("Clean complete! Workspace repository updated.")
    else:
        logger.info("Cleanup skipped. Working files retained in 'temp-data/'.")


if __name__ == "__main__":
    main()
