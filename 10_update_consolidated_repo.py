"""
Stage 10: Update Consolidated Repo Script (With Safe Archival Check)

Moves soft-deleted legacy rule files from active/ to archived/ in the git repository ONLY
if they have been soft-deleted in the database. Updates processed_mls_ledger.json and
cleans up local workspace artifacts.
"""

import os
import sys
import json
import io
import shutil
import argparse
import pandas as pd
import psycopg2
import paramiko
from pathlib import Path
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from cryptography.hazmat.primitives import serialization

from pipeline_logger import setup_logger

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
logger = setup_logger("Stage10_RepoArchive")


def load_old_rule_names(blueprint_path: Path) -> list[str]:
    """Parses the migration blueprint and returns unique legacy rule names."""
    if not blueprint_path.exists():
        logger.error(f"❌ Blueprint file not found at '{blueprint_path}'. Aborting.")
        sys.exit(1)

    path_str = str(blueprint_path)
    df = pd.read_csv(path_str) if path_str.endswith(".csv") else pd.read_excel(path_str)

    column_name = "old_name"
    if column_name not in df.columns:
        logger.error(f"❌ Required column '{column_name}' missing from blueprint.")
        sys.exit(1)

    legacy_rules = df[column_name].dropna().astype(str).str.strip()
    return [r for r in legacy_rules.unique().tolist() if r and r.lower() != "nan"]


def get_deleted_rules_set(rule_names: list[str]) -> set[str]:
    """
    Checks database over SSH tunnel to return rule names that have deleted_at IS NOT NULL.
    Uses LOWER() matching for case-insensitive accuracy.
    """
    ssh_host = os.getenv("SSH_HOST", "").strip("'\"")
    ssh_user = os.getenv("SSH_USER", "").strip("'\"")
    ssh_key_path = os.getenv("SSH_KEY_PATH", "").strip("'\"")
    ssh_passphrase = os.getenv("SSH_KEY_PASSPHRASE", "").strip("'\"")

    db_host = os.getenv("DB_HOST", "").strip("'\"")
    db_name = os.getenv("DB_NAME", "").strip("'\"")
    db_user = os.getenv("DB_USER", "shrisha_vanga").strip("'\"")
    db_pass = os.getenv("DB_PASSWORD", "").strip("'\"")

    deleted_rules = set()
    lowercased_targets = [name.lower() for name in rule_names]

    if not ssh_host or not ssh_key_path:
        logger.error("❌ SSH parameters missing from .env file. Unable to verify DB deleted status.")
        sys.exit(1)

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
                        WHERE deleted_at IS NOT NULL AND LOWER(name) = ANY(%s);
                    """
                    cur.execute(query, (lowercased_targets,))
                    for (db_rule_name,) in cur.fetchall():
                        # Map match back to blueprint rule names
                        for orig_name in rule_names:
                            if orig_name.lower() == db_rule_name.lower():
                                deleted_rules.add(orig_name)

    except Exception as e:
        logger.error(f"❌ Failed to check deleted rules in DB: {e}", exc_info=True)
        sys.exit(1)

    return deleted_rules


def archive_legacy_rules(blueprint_path: Path, repo_base_path: Path) -> bool:
    """Moves legacy rules from active/ to archived/ ONLY if soft-deleted in DB."""
    old_rule_names = load_old_rule_names(blueprint_path)
    if not old_rule_names:
        logger.info("ℹ️ No legacy rules found in blueprint. Skipping repo archival step.")
        return True

    logger.info(f"📄 Loaded {len(old_rule_names)} legacy rule name candidate(s) from blueprint.")

    # Check which rules are soft-deleted in DB
    logger.info("🔍 Checking DB to identify soft-deleted legacy rules...")
    deleted_rules = get_deleted_rules_set(old_rule_names)
    logger.info(f"Verified {len(deleted_rules)}/{len(old_rule_names)} rule(s) are soft-deleted in DB.")

    ui_rules_root = repo_base_path / "ui-rules"
    if not ui_rules_root.exists() and (repo_base_path / "active").exists():
        ui_rules_root = repo_base_path

    active_dir = ui_rules_root / "active"
    archived_dir = ui_rules_root / "archived"

    if not active_dir.exists():
        logger.error(f"❌ Active directory not found at '{active_dir}'.")
        return False

    archived_dir.mkdir(parents=True, exist_ok=True)
    archived_count = 0

    for rule_name in old_rule_names:
        if rule_name not in deleted_rules:
            logger.info(f"   ℹ️ Keeping '{rule_name}.py' in active/ (Not yet soft-deleted in DB)")
            continue

        for item in active_dir.glob("*.py"):
            if item.stem.lower() == rule_name.lower() or item.stem.lower().startswith(f"{rule_name.lower()}_collision_"):
                destination = archived_dir / item.name
                shutil.move(str(item), str(destination))
                logger.info(f"   📦 Archived: {item.name} ==> ui-rules/archived/{item.name}")
                archived_count += 1

    logger.info(f"📦 Archival Summary: Moved {archived_count} file(s) to ui-rules/archived/.")
    return True


def update_processed_ledger(temp_dir: Path) -> None:
    """Appends currently processed MLS IDs from api_mls.txt and rets_mls.txt to the processed ledger."""
    candidate_files = [
        temp_dir / "api_mls.txt",
        temp_dir / "rets_mls.txt",
    ]
    ledger_file = temp_dir / "processed_mls_ledger.json"

    current_mls = set()
    for mls_file in candidate_files:
        if mls_file.exists():
            for line in mls_file.read_text(encoding="utf-8").splitlines():
                cleaned = line.strip().strip("'\"")
                if cleaned and not cleaned.startswith("#"):
                    try:
                        current_mls.add(int(cleaned))
                    except ValueError:
                        pass

    if not current_mls:
        return

    completed_mls = set()
    if ledger_file.exists():
        try:
            data = json.loads(ledger_file.read_text(encoding="utf-8"))
            completed_mls = set(int(x) for x in data.get("completed_mls_ids", []))
        except Exception:
            pass

    completed_mls.update(current_mls)
    ledger_file.parent.mkdir(parents=True, exist_ok=True)
    ledger_file.write_text(json.dumps({"completed_mls_ids": sorted(list(completed_mls))}, indent=2), encoding="utf-8")

    logger.info(f"✅ Ledger Updated: Recorded {len(current_mls)} MLS ID(s) ({', '.join(str(x) for x in sorted(list(current_mls)))}) in '{ledger_file}'.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Archive soft-deleted rules and update processed ledger.")
    parser.add_argument("-y", "--clean-temp", action="store_true", help="Automatically clean temporary working files without prompt.")
    args = parser.parse_args()

    temp_dir = current_dir / "temp-data"
    blueprint_file = temp_dir / "migration_blueprint.csv"

    repo_base_raw = os.getenv("REPO_PATH", "") or os.getenv("UI_RULES_DIR", "")
    repo_base = repo_base_raw.strip().strip("'\"")

    if not repo_base:
        # Fallback to local sibling repo if env var is missing
        repo_base = str(current_dir.parent / "dm-consolidated-rules")

    repo_path = Path(repo_base)

    logger.info("============================================================")
    logger.info("📂 STAGE 10: CONSOLIDATED REPO ARCHIVE SYNC TOOL")
    logger.info("============================================================")
    logger.info(f"Target REPO_PATH: {repo_path}")

    success = archive_legacy_rules(blueprint_file, repo_path)

    if not success:
        logger.error("❌ Sync halted due to error.")
        return

    # Update ledger prior to optional temp folder cleanup
    update_processed_ledger(temp_dir)

    do_clean = args.clean_temp
    if not do_clean and sys.stdin.isatty():
        confirm = input("\nDo you want to delete temporary working files ('temp-data/' excluding ledger)? (y/N): ")
        if confirm.lower() == "y":
            do_clean = True

    if do_clean:
        for item in temp_dir.glob("*"):
            if item.name != "processed_mls_ledger.json":
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
        logger.info("🧹 Clean complete! Temporary working files removed (ledger preserved).")
    else:
        logger.info("ℹ️ Cleanup skipped. Working files retained in 'temp-data/'.")


if __name__ == "__main__":
    main()
