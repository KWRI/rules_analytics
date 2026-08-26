"""
Stage 9: Soft Delete Legacy Rules Script (With Active Reference Safety Guard)

Parses 'temp-data/migration_blueprint.csv' and checks the staging database to ensure
that targeted legacy rules are NOT currently in use by other unmigrated MLS sources.
Only soft-deletes rules that have ZERO remaining active references via the MLS Admin API.

API Specification:
    Method: DELETE
    Endpoint: https://stage-ext-ms.data.kw.com/v1/mls-admin/rules
"""

import os
import io
import sys
import argparse
import requests
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
logger = setup_logger("Stage9_SoftDelete")

API_BASE_URL = os.getenv("MLS_ADMIN_API_URL", "https://stage-ext-ms.data.kw.com").strip().strip("'\"").rstrip("/")
API_KEY = os.getenv("MLS_ADMIN_API_KEY") or os.getenv("API_KEY", "")
DELETED_BY_USER = os.getenv("UPDATED_BY_USER") or os.getenv("DB_USER", "shrisha_vanga")

HEADERS = {
    "accept": "application/json",
    "api-key": API_KEY,
}


def load_old_rule_names_from_blueprint(blueprint_path: Path) -> list[str]:
    """Parses the migration blueprint file and extracts unique legacy rule names."""
    if not blueprint_path.exists():
        logger.error(f"❌ Blueprint file not found at '{blueprint_path}'. Aborting.")
        sys.exit(1)

    path_str = str(blueprint_path)
    df = pd.read_csv(path_str) if path_str.endswith(".csv") else pd.read_excel(path_str)

    column_name = "old_name"
    if column_name not in df.columns:
        logger.error(f"❌ Column '{column_name}' not found in '{blueprint_path}'. Aborting.")
        sys.exit(1)

    # Filter out empty or null legacy names
    legacy_rules = df[column_name].dropna().astype(str).str.strip()
    valid_rules = [r for r in legacy_rules.unique().tolist() if r and r.lower() != "nan"]
    return valid_rules


def get_active_rule_usage_counts(rule_names: list[str]) -> dict[str, int]:
    """
    Connects to DB over SSH tunnel to check if rules are still referenced by ANY active MLS mapping.
    Uses LOWER() matching to guard against rule name casing variations.
    """
    ssh_host = os.getenv("SSH_HOST", "").strip("'\"")
    ssh_user = os.getenv("SSH_USER", "").strip("'\"")
    ssh_key_path = os.getenv("SSH_KEY_PATH", "").strip("'\"")
    ssh_passphrase = os.getenv("SSH_KEY_PASSPHRASE", "").strip("'\"")

    db_host = os.getenv("DB_HOST", "").strip("'\"")
    db_name = os.getenv("DB_NAME", "").strip("'\"")
    db_user = os.getenv("DB_USER", "shrisha_vanga").strip("'\"")
    db_pass = os.getenv("DB_PASSWORD", "").strip("'\"")

    usage_counts = {name: 0 for name in rule_names}
    lowercased_targets = [name.lower() for name in rule_names]

    if not ssh_host or not ssh_key_path:
        logger.error("❌ SSH parameters missing from .env file. Unable to verify DB dependencies.")
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
                    # Query active process_rule associations with CASE-INSENSITIVE match
                    query = """
                        SELECT pr.name, COUNT(mra.id) AS active_refs
                        FROM public.process_rule pr
                        JOIN public.map_rule_association mra ON pr.id = mra.process_rule_id
                        JOIN public.mls_resource mr ON mra.process_map_id = mr.process_map_id
                        JOIN public.mls m ON mr.mls_id = m.id
                        WHERE pr.deleted_at IS NULL 
                          AND m.mls_status_id = 2
                          AND LOWER(pr.name) = ANY(%s)
                        GROUP BY pr.name;
                    """
                    cur.execute(query, (lowercased_targets,))
                    for rule_name, count in cur.fetchall():
                        # Map count back to original rule name array
                        for orig_name in rule_names:
                            if orig_name.lower() == rule_name.lower():
                                usage_counts[orig_name] += count

    except Exception as e:
        logger.error(f"❌ Failed to verify active rule usage from DB: {e}", exc_info=True)
        sys.exit(1)

    return usage_counts


def delete_rule_via_api(rule_name: str, deleted_by: str) -> bool:
    """Sends a DELETE request matching the exact Swagger API contract."""
    url = f"{API_BASE_URL}/v1/mls-admin/rules"
    params = {"name": rule_name, "deleted_by": deleted_by}

    try:
        response = requests.delete(url, headers=HEADERS, params=params, timeout=15)
        if response.status_code in (200, 204):
            try:
                data = response.json()
                rule_id = data.get("id", "N/A")
            except Exception:
                rule_id = "N/A"
            logger.info(f"   ✅ Soft-deleted rule: '{rule_name}' (ID: {rule_id}, by {deleted_by})")
            return True
        else:
            logger.error(f"   ❌ Failed for '{rule_name}' [HTTP {response.status_code}]: {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"   ❌ Request exception for '{rule_name}': {e}", exc_info=True)
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Soft delete legacy rules unlinked from active MLS sources.")
    parser.add_argument("-y", "--force", action="store_true", help="Auto-confirm soft deletion without user prompt.")
    args = parser.parse_args()

    blueprint_file = current_dir / "temp-data" / "migration_blueprint.csv"
    logger.info("============================================================")
    logger.info("🗑️ STAGE 9: MLS ADMIN MICROSERVICE SAFE SOFT DELETE TOOL")
    logger.info("============================================================")

    rule_names = load_old_rule_names_from_blueprint(blueprint_file)
    if not rule_names:
        logger.info("ℹ️ No legacy rules ('old_name') found in blueprint. Nothing to delete.")
        return

    logger.info(f"📄 Loaded {len(rule_names)} legacy rule candidate(s) from migration blueprint.")

    # Check active usage across remaining MLS sources
    logger.info("🔍 Verifying active rule dependencies across unmigrated MLS sources...")
    usage_counts = get_active_rule_usage_counts(rule_names)

    rules_to_delete = []
    rules_to_keep = []

    for name in rule_names:
        active_refs = usage_counts.get(name, 0)
        if active_refs > 0:
            rules_to_keep.append((name, active_refs))
        else:
            rules_to_delete.append(name)

    if rules_to_keep:
        logger.info(f"⚠️ Retaining {len(rules_to_keep)} rule(s) because they are still used by unmigrated MLSs:")
        for name, refs in rules_to_keep:
            logger.info(f"   • '{name}' (Used by {refs} active mapping(s) — SKIPPED)")

    if not rules_to_delete:
        logger.info("🎉 No rules are ready for soft-deletion in this batch (all rules are still shared by unmigrated MLSs).")
        return

    logger.info(f"✅ Ready to soft-delete {len(rules_to_delete)} fully unlinked legacy rule(s):")
    for idx, name in enumerate(rules_to_delete, 1):
        logger.info(f"   {idx}. {name}")

    if not args.force:
        if not sys.stdin.isatty():
            logger.warning("⚠️ Non-interactive session detected without --force flag. Aborting for safety.")
            return

        confirm = input(f"\nConfirm soft-deletion of {len(rules_to_delete)} unlinked rule(s)? (y/N): ")
        if confirm.lower() != "y":
            logger.info("Operation cancelled by user.")
            return

    logger.info("🚀 Starting API deletion sequence...")
    success_count = 0
    for name in rules_to_delete:
        if delete_rule_via_api(name, DELETED_BY_USER):
            success_count += 1

    logger.info("============================================================")
    logger.info(f"✅ STAGE 9 FINISHED: Soft-deleted {success_count}/{len(rules_to_delete)} rule(s) via API.")
    logger.info("============================================================")


if __name__ == "__main__":
    main()
