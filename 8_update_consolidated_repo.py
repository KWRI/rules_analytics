"""
Update Consolidated Repo Script (With Safe Archival Check).

Moves soft-deleted legacy rule files from active/ to archived/ in the git repository ONLY
if they have been soft-deleted in the database. Updates processed_mls_ledger.json.
"""

import os
import re
import sys
import json
import shutil
import pandas as pd
import psycopg2
import paramiko
import io
from pathlib import Path
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from cryptography.hazmat.primitives import serialization
from pipeline_logger import setup_logger

load_dotenv()
logger = setup_logger("Stage8_RepoArchive")


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


def get_deleted_rules_set(rule_names: list[str]) -> set[str]:
    """Checks database to return only rule names that have deleted_at IS NOT NULL."""
    ssh_host = os.getenv("SSH_HOST", "").strip("'\"")
    ssh_user = os.getenv("SSH_USER", "").strip("'\"")
    ssh_key_path = os.getenv("SSH_KEY_PATH", "").strip("'\"")
    ssh_passphrase = os.getenv("SSH_KEY_PASSPHRASE", "").strip("'\"")

    db_host = os.getenv("DB_HOST", "").strip("'\"")
    db_name = os.getenv("DB_NAME", "").strip("'\"")
    db_user = os.getenv("DB_USER", "").strip("'\"")
    db_pass = os.getenv("DB_PASSWORD", "").strip("'\"")

    deleted_rules = set()

    try:
        with open(ssh_key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=ssh_passphrase.encode() if ssh_passphrase else None,
            )
        pem_data = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
        mypkey = paramiko.RSAKey.from_private_key(io.StringIO(pem_data.decode()))

        with SSHTunnelForwarder(
            (ssh_host, 22),
            ssh_username=ssh_user,
            ssh_pkey=mypkey,
            remote_bind_address=(db_host, 5432),
        ) as tunnel:
            with psycopg2.connect(
                dbname=db_name,
                user=db_user,
                password=db_pass,
                host="127.0.0.1",
                port=tunnel.local_bind_port,
            ) as conn:
                with conn.cursor() as cur:
                    query = """
                        SELECT name FROM public.process_rule
                        WHERE deleted_at IS NOT NULL AND name = ANY(%s);
                    """
                    cur.execute(query, (rule_names,))
                    for (name,) in cur.fetchall():
                        deleted_rules.add(name)

    except Exception as e:
        logger.error(f"Failed to check deleted rules in DB: {e}", exc_info=True)

    return deleted_rules


def archive_legacy_rules(blueprint_path: Path, repo_base_path: Path) -> bool:
    """Moves legacy rules from active/ to archived/ ONLY if soft-deleted in DB."""
    old_rule_names = load_old_rule_names(blueprint_path)
    logger.info(f"Loaded {len(old_rule_names)} legacy rule name(s) from blueprint.")

    # Check which rules are soft-deleted in DB
    deleted_rules = get_deleted_rules_set(old_rule_names)
    logger.info(f"Verified {len(deleted_rules)}/{len(old_rule_names)} rule(s) are soft-deleted in DB.")

    ui_rules_root = repo_base_path / "ui-rules"
    active_dir = ui_rules_root / "active"
    archived_dir = ui_rules_root / "archived"

    if not active_dir.exists():
        logger.error(f"Active directory not found at '{active_dir}'")
        return False

    archived_dir.mkdir(parents=True, exist_ok=True)
    archived_count = 0

    for rule_name in old_rule_names:
        if rule_name not in deleted_rules:
            logger.info(f"  ℹ️ Keeping '{rule_name}.py' in active/ (Still used by unmigrated MLSs)")
            continue

        for item in active_dir.glob("*.py"):
            if item.stem == rule_name or item.stem.startswith(f"{rule_name}_collision_"):
                destination = archived_dir / item.name
                shutil.move(str(item), str(destination))
                logger.info(f"  📦 Archived: {item.name} ==> ui-rules/archived/{item.name}")
                archived_count += 1

    logger.info(f"Archival Summary: Moved {archived_count} file(s) to ui-rules/archived/.")
    return True


def update_processed_ledger() -> None:
    """Appends currently processed MLS IDs from test_mls.txt to the ledger."""
    test_mls_file = Path("temp-data/test_mls.txt")
    ledger_file = Path("temp-data/processed_mls_ledger.json")

    if not test_mls_file.exists():
        return

    current_mls = [line.strip() for line in test_mls_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not current_mls:
        return

    completed_mls = set()
    if ledger_file.exists():
        try:
            data = json.loads(ledger_file.read_text(encoding="utf-8"))
            completed_mls = set(data.get("completed_mls_ids", []))
        except Exception:
            pass

    completed_mls.update(current_mls)
    ledger_file.parent.mkdir(parents=True, exist_ok=True)
    ledger_file.write_text(json.dumps({"completed_mls_ids": sorted(list(completed_mls))}, indent=2), encoding="utf-8")

    logger.info(f"Ledger Updated: Recorded {len(current_mls)} MLS ID(s) ({', '.join(current_mls)}) in '{ledger_file}'.")


def cleanup_temp_directories(temp_dirs: list[Path]) -> None:
    """Removes temporary working directories after sync finishes."""
    logger.info("Cleaning up temporary working files...")
    for temp_dir in temp_dirs:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
            logger.info(f"Removed folder: '{temp_dir}'")


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

    update_processed_ledger()

    confirm = input("\nDo you want to delete temporary working files ('temp-data/')? (y/N): ")
    if confirm.lower() == "y":
        cleanup_temp_directories(TEMP_DIRS_TO_REMOVE)
        logger.info("Clean complete! Workspace repository updated.")
    else:
        logger.info("Cleanup skipped. Working files retained in 'temp-data/'.")


if __name__ == "__main__":
    main()
