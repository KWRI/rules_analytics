"""
Pipeline Stage 0: Automated Batch Target Selector & Reverse Promotion Invoker (API Variant).

Selects unmigrated API MLS sources (filtered by optional assigned MLS list),
locks targets in public.processed_mls_ledger, triggers reverse promotion (Prod -> Stage),
and builds working batch manifests in temp-data/.
"""

import os
import io
import csv
import sys
import argparse
import signal
import warnings
from pathlib import Path
import psycopg2
import paramiko
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from cryptography.utils import CryptographyDeprecationWarning
from cryptography.hazmat.primitives import serialization

# --- OS-LEVEL INSTANT TERMINAL EXIT ---
def force_terminal_exit(sig, frame):
    print("\n⛔ [TERMINAL ABORT] Killing process tree immediately...")
    os._exit(1)

signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

from reverse_promotion_prod_to_stage import main as run_reverse_promotion
from rules_utils import (
    load_skip_mls,
    load_processed_ledger,
    get_db_locked_mls_ids,
    claim_mls_batch_in_db,
    load_discrepant_mls,
)
from pipeline_logger import setup_logger

warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
warnings.filterwarnings("ignore", message=".*TripleDES.*")

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
logger = setup_logger("Stage0_APIBatchPrepare")


def parse_assigned_mls_ids(cli_input: str | None, root_dir: Path) -> set[int]:
    """Parses assigned MLS IDs from CLI argument or my_assigned_mls.txt if present."""
    assigned = set()
    if cli_input:
        for item in cli_input.split(","):
            cleaned = item.strip()
            if cleaned.isdigit():
                assigned.add(int(cleaned))
        if assigned:
            logger.info(f"🎯 Jira Ticket Targeting (CLI): Filtering strictly for MLS IDs {sorted(list(assigned))}")
            return assigned

    assigned_file = root_dir / "my_assigned_mls.txt"
    if assigned_file.exists():
        for line in assigned_file.read_text(encoding="utf-8").splitlines():
            cleaned = line.split("#")[0].strip().strip("'\"")
            if cleaned.isdigit():
                assigned.add(int(cleaned))
        if assigned:
            logger.info(f"🎯 Jira Ticket Targeting (my_assigned_mls.txt): Filtering strictly for MLS IDs {sorted(list(assigned))}")

    return assigned


def get_active_prod_mls_ids() -> set[int]:
    """Queries Production database over SSH tunnel to return active MLS primary key IDs (mls_status_id = 2)."""
    ssh_host = (os.getenv("PROD_SSH_HOST") or os.getenv("REMOTE_SSH_HOST") or os.getenv("SSH_HOST", "")).strip("'\"")
    ssh_user = (os.getenv("SSH_USER") or os.getenv("REMOTE_SSH_USER", "")).strip("'\"")
    ssh_key_path = (os.getenv("SSH_KEY_PATH") or os.getenv("REMOTE_SSH_KEY_PATH", "")).strip("'\"")
    ssh_passphrase = (os.getenv("SSH_KEY_PASSPHRASE") or os.getenv("REMOTE_SSH_KEY_PASSPHRASE", "")).strip("'\"")

    db_host = (os.getenv("PROD_DB_HOST") or os.getenv("DB_HOST", "")).strip("'\"")
    db_name = (os.getenv("DB_NAME", "mls_admin")).strip("'\"")
    db_user = (os.getenv("DB_USER", "")).strip("'\"")
    db_pass = (os.getenv("DB_PASSWORD", "")).strip("'\"")

    if not ssh_host or not ssh_key_path:
        logger.error("❌ SSH parameters missing for Production database query.")
        sys.exit(1)

    prod_active_ids = set()
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
                    cur.execute("SELECT id FROM public.mls WHERE mls_status_id = 2;")
                    for (m_id,) in cur.fetchall():
                        if m_id is not None:
                            prod_active_ids.add(int(m_id))
    except Exception as e:
        logger.error(f"❌ Failed to query Production database active status: {e}", exc_info=True)
        sys.exit(1)

    return prod_active_ids


def prompt_user_confirmation(prompt_text: str) -> bool:
    """Prompts the operator for explicit Y/N confirmation."""
    try:
        answer = input(f"\n⚠️  {prompt_text} (y/N): ").strip().lower()
        return answer == "y"
    except (EOFError, KeyboardInterrupt):
        logger.warning("\n⛔ Operator cancelled input prompt. Exiting Stage 0 immediately.")
        os._exit(1)


def prepare_api_batch(limit: int, auto_approve: bool, assigned_mls_input: str | None = None) -> None:
    temp_dir = current_dir / "temp-data"
    temp_dir.mkdir(parents=True, exist_ok=True)

    assigned_ids = parse_assigned_mls_ids(assigned_mls_input, current_dir)

    # 1. Fetch live active Production MLS primary key IDs
    logger.info("🔍 Fetching live active MLS IDs from Production DB (mls_status_id = 2)...")
    prod_active_ids = get_active_prod_mls_ids()
    logger.info(f"✅ Production Check Complete: Identified {len(prod_active_ids)} active MLS ID(s) in Production.")

    # 2. Load exclusions (skip_mls.txt, local processed ledger, root discrepant_mls.txt)
    skip_ids = load_skip_mls(temp_dir)
    completed_ids = load_processed_ledger(temp_dir)
    discrepant_ids = load_discrepant_mls(current_dir)

    if discrepant_ids:
        logger.info(f"Loaded {len(discrepant_ids)} discrepant MLS ID(s) from discrepant_mls.txt.")

    ssh_host = (os.getenv("STAGE_SSH_HOST") or os.getenv("REMOTE_SSH_HOST") or os.getenv("SSH_HOST", "")).strip("'\"")
    ssh_user = (os.getenv("SSH_USER") or os.getenv("REMOTE_SSH_USER", "")).strip("'\"")
    ssh_key_path = (os.getenv("SSH_KEY_PATH") or os.getenv("REMOTE_SSH_KEY_PATH", "")).strip("'\"")
    ssh_passphrase = (os.getenv("SSH_KEY_PASSPHRASE") or os.getenv("REMOTE_SSH_KEY_PASSPHRASE", "")).strip("'\"")

    db_host = (os.getenv("STAGE_DB_HOST") or os.getenv("DB_HOST", "")).strip("'\"")
    db_name = (os.getenv("DB_NAME", "mls_admin")).strip("'\"")
    db_user = (os.getenv("DB_USER", "")).strip("'\"")
    db_pass = (os.getenv("DB_PASSWORD", "")).strip("'\"")

    current_user = (os.getenv("DB_USER") or os.getenv("USERNAME") or "migration_pipeline_bot").strip("'\"")

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

        logger.info("Connecting via SSH tunnel to Staging database...")
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
                db_locked_ids = get_db_locked_mls_ids(conn)
                logger.info(f"Loaded {len(db_locked_ids)} locked/completed MLS ID(s) from DB ledger table.")

                all_excluded_ids = skip_ids | completed_ids | db_locked_ids | discrepant_ids

                logger.info("Step 1: Identifying target active API MLS sources in Staging...")
                select_query = """
                    WITH candidate_rules AS (
                        SELECT 
                            pr.id AS rule_id,
                            pr.name AS rule_name,
                            COUNT(DISTINCT m.id) AS total_mls_count
                        FROM public.process_rule pr
                        JOIN public.map_rule_association mra ON mra.process_rule_id = pr.id
                        JOIN public.mls_resource mr ON mr.process_map_id = mra.process_map_id
                        JOIN public.mls m ON m.id = mr.mls_id
                        WHERE pr.deleted_at IS NULL
                          AND m.mls_status_id = 2
                          AND pr.name !~ '^[A-Z0-9][a-zA-Z0-9]*$'
                        GROUP BY pr.id, pr.name
                    )
                    SELECT DISTINCT
                        m.id     AS mls_id,
                        m.id_str AS mls_id_str,
                        cr.rule_name,
                        cr.total_mls_count
                    FROM candidate_rules cr
                    JOIN public.map_rule_association mra ON mra.process_rule_id = cr.rule_id
                    JOIN public.mls_resource mr ON mr.process_map_id = mra.process_map_id
                    JOIN public.mls m ON m.id = mr.mls_id
                    JOIN public.download_mls dm ON dm.mls_id = m.id
                    JOIN public.download_config dc ON dc.id = dm.download_config_id
                    JOIN public.download_protocol dp ON dp.id = dc.download_protocol_id
                    WHERE m.mls_status_id = 2
                      AND (LOWER(dp.name) LIKE '%%api%%' OR LOWER(dp.name) LIKE '%%rest%%' OR LOWER(dp.name) LIKE '%%oauth%%')
                    ORDER BY cr.total_mls_count ASC, cr.rule_name ASC, m.id ASC;
                """

                selected_targets = []
                selected_mls_ids = []
                with conn.cursor() as cur:
                    cur.execute(select_query)
                    for m_id, m_id_str, _, _ in cur.fetchall():
                        m_id_int = int(m_id)

                        if assigned_ids and m_id_int not in assigned_ids:
                            continue

                        if m_id_int not in all_excluded_ids and m_id_int in prod_active_ids:
                            if m_id_int not in selected_mls_ids:
                                selected_mls_ids.append(m_id_int)
                                selected_targets.append((m_id_int, str(m_id_str)))
                                if len(selected_mls_ids) >= limit:
                                    break

                if not selected_mls_ids:
                    logger.info("ℹ️ No eligible unmigrated API MLS sources match the selection criteria.")
                    return

                logger.info(f"Selected target Production-Verified API MLS IDs ({len(selected_mls_ids)}): {selected_mls_ids}")

                # Lock claimed targets in DB ledger
                claim_mls_batch_in_db(conn, selected_targets, current_user)
                logger.info(f"🔒 Claimed target MLS batch in DB ledger (IN_PROGRESS): {selected_mls_ids}")

                # Write target reverse promotion manifest
                rev_manifest_path = temp_dir / "reverse_promotion_sources.txt"
                rev_manifest_path.write_text("\n".join(str(x) for x in selected_mls_ids) + "\n", encoding="utf-8")
                logger.info("============================================================")
                logger.info(f"📄 Target Reverse Promotion Manifest written: {rev_manifest_path}")
                logger.info(f"🎯 Target Sources Loaded from File ({len(selected_mls_ids)}): {selected_mls_ids}")
                logger.info("============================================================")

                if not auto_approve:
                    if not prompt_user_confirmation(f"Ready to promote the following sources from 'reverse_promotion_sources.txt' (PROD -> STAGE)?\n   Sources: {selected_mls_ids}"):
                        logger.warning("Pipeline paused by user prior to Reverse Promotion.")
                        sys.exit(0)

                # Trigger Reverse Promotion (Prod -> Stage) safely isolating sys.argv
                logger.info("🚀 Triggering Reverse Promotion (Prod -> Stage) for target batch...")
                run_reverse_promotion(args_list=[])

                logger.info("Step 2: Fetching granular metadata for target API MLS sources...")
                detail_query = """
                    SELECT DISTINCT
                        v.name               AS vendor_name,
                        m.id_str             AS mls_id_str,
                        m.id                 AS mls_id,
                        m.name               AS mls_name,
                        dp.name              AS download_protocol,
                        ctm.content_type     AS content_type,
                        ctm.content_sub_type AS content_sub_type,
                        'true'               AS is_enabled,
                        pr.name              AS rule_name
                    FROM public.mls m
                    JOIN public.vendor v ON v.id = m.vendor_id
                    JOIN public.mls_resource mr ON mr.mls_id = m.id
                    JOIN public.content_type_map ctm ON ctm.id = mr.content_type_map_id
                    JOIN public.map_rule_association mra ON mra.process_map_id = mr.process_map_id
                    JOIN public.process_rule pr ON pr.id = mra.process_rule_id
                    JOIN public.download_mls dm ON dm.mls_id = m.id
                    JOIN public.download_config dc ON dc.id = dm.download_config_id
                    JOIN public.download_protocol dp ON dp.id = dc.download_protocol_id
                    WHERE m.id = ANY(%s)
                      AND pr.deleted_at IS NULL
                      AND pr.name !~ '^[A-Z0-9][a-zA-Z0-9]*$'
                    ORDER BY m.id ASC, pr.name ASC;
                """

                rows = []
                target_rules = set()

                with conn.cursor() as cur:
                    cur.execute(detail_query, (selected_mls_ids,))
                    for row in cur.fetchall():
                        (
                            vendor_name, mls_id_str, mls_id, mls_name,
                            download_protocol, content_type, content_sub_type,
                            is_enabled, rule_name
                        ) = row

                        rows.append({
                            "vendor_name": vendor_name,
                            "mls_id_str": mls_id_str,
                            "mls_id": mls_id,
                            "mls_name": mls_name,
                            "download_protocol": download_protocol,
                            "content_type": content_type,
                            "content_sub_type": content_sub_type,
                            "is_enabled": is_enabled,
                        })
                        target_rules.add(rule_name)

                # Write local manifests in temp-data/
                api_mls_file = temp_dir / "api_mls.txt"
                api_rules_file = temp_dir / "api_rules.txt"
                batch_csv_file = temp_dir / "batch_mls_targets_api.csv"
                master_csv_file = temp_dir / "batch_mls_targets.csv"

                api_mls_file.write_text("\n".join(str(x) for x in selected_mls_ids) + "\n", encoding="utf-8")
                api_rules_file.write_text("\n".join(sorted(list(target_rules))) + "\n", encoding="utf-8")

                fieldnames = [
                    "vendor_name", "mls_id_str", "mls_id", "mls_name",
                    "download_protocol", "content_type", "content_sub_type", "is_enabled"
                ]

                with open(batch_csv_file, mode="w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)

                with open(master_csv_file, mode="w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)

                logger.info("============================================================")
                logger.info("🎯 AUTOMATED API BATCH SELECTION COMPLETE")
                logger.info("============================================================")
                logger.info(f"Target MLS IDs ({len(selected_mls_ids)}): {', '.join(str(x) for x in selected_mls_ids)}")
                logger.info(f"Target Rules ({len(target_rules)}): {', '.join(sorted(list(target_rules)))}")
                logger.info(f"Generated: {api_mls_file}")
                logger.info(f"Generated: {api_rules_file}")
                logger.info(f"Generated: {batch_csv_file}")
                logger.info(f"Updated Working Target: {master_csv_file}")
                logger.info("============================================================")

    except Exception as e:
        logger.error(f"❌ Failed during Stage 0 API batch preparation: {e}", exc_info=True)
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 0: Prepare Target API MLS Batch")
    parser.add_argument("-l", "--limit", type=int, default=4, help="Maximum number of target API MLS sources to select")
    parser.add_argument("-a", "--assigned-mls", type=str, default=None, help="Comma-separated assigned MLS IDs (e.g., 502,162)")
    parser.add_argument("-y", "--yes", action="store_true", dest="auto_approve", help="Auto-approve interactive prompts")
    args = parser.parse_args()

    prepare_api_batch(limit=args.limit, auto_approve=args.auto_approve, assigned_mls_input=args.assigned_mls)


if __name__ == "__main__":
    main()
