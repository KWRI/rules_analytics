"""
Pipeline Stage 0 (RETS Variant): Streamlined High-Performance Batch Target Generator.

Targets ONLY RETS MLS sources that are active in both Staging and Production.
Intersects real-time Production active state (mls_status_id = 2) with Staging candidate rules.
Excludes completed ledger IDs and manual exclusions in skip_mls.txt.

Generates 'rets_mls.txt', 'rets_rules.txt', and 'batch_mls_targets_rets.csv', then merges into 'batch_mls_targets.csv'.

Usage:
    python 0_prepare_batch_targets_rets.py           # Uses default limit (5 MLSs)
    python 0_prepare_batch_targets_rets.py --limit 2 # Picks targets across 2 distinct MLSs
    python 0_prepare_batch_targets_rets.py -l 10     # Picks targets across 10 distinct MLSs
"""

import os
import io
import re
import csv
import argparse
import warnings
from pathlib import Path
import psycopg2
import paramiko
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from cryptography.utils import CryptographyDeprecationWarning
from cryptography.hazmat.primitives import serialization

from rules_utils import load_processed_ledger, load_skip_mls, get_live_active_prod_mls
from pipeline_logger import setup_logger

warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
warnings.filterwarnings("ignore", message=".*TripleDES.*")

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / "..env")
logger = setup_logger("Stage0_RETSBatchPrepare")

DEFAULT_BATCH_MLS_LIMIT = 5
TEMP_DATA_DIR = current_dir / "temp-data"

RETS_PROTOCOL_ID = int(os.getenv("RETS_PROTOCOL_ID", 3487705513709191176))


def is_pascal_case(name: str) -> bool:
    """Checks if a rule name is standard PascalCase."""
    if not name or re.match(r'^\d', name) or any(c in name for c in ('_', ' ', '-', '/')):
        return False
    return bool(re.match(r'^[A-Z][a-zA-Z0-9]*$', name))


def save_unified_working_targets(current_records: list[dict]):
    """Merges newly generated records into 'batch_mls_targets.csv'."""
    api_path = TEMP_DATA_DIR / "batch_mls_targets_api.csv"
    rets_path = TEMP_DATA_DIR / "batch_mls_targets_rets.csv"
    working_path = TEMP_DATA_DIR / "batch_mls_targets.csv"

    unified_map = {}

    if api_path.exists():
        with open(api_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = (str(row["mls_id"]), row["content_type"], row["content_sub_type"], row["legacy_rule_name"])
                unified_map[key] = row

    if rets_path.exists():
        with open(rets_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = (str(row["mls_id"]), row["content_type"], row["content_sub_type"], row["legacy_rule_name"])
                unified_map[key] = row

    for rec in current_records:
        key = (str(rec["mls_id"]), rec["content_type"], rec["content_sub_type"], rec["legacy_rule_name"])
        unified_map[key] = rec

    csv_headers = [
        "mls_id", "mls_id_str", "sa_mls_id", "sa_mls_id_str",
        "mls_status_id", "download_protocol", "content_type",
        "content_sub_type", "legacy_rule_name"
    ]

    sorted_records = sorted(
        unified_map.values(),
        key=lambda r: (int(r["mls_id"]), r["legacy_rule_name"])
    )

    with open(working_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_headers)
        writer.writeheader()
        writer.writerows(sorted_records)


def generate_batch(mls_limit: int):
    stage_ssh_host = (os.getenv("STAGE_SSH_HOST") or os.getenv("REMOTE_SSH_HOST") or os.getenv("SSH_HOST", "")).strip("'\"")
    ssh_user = (os.getenv("SSH_USER") or os.getenv("REMOTE_SSH_USER", "")).strip("'\"")
    ssh_key_path = (os.getenv("SSH_KEY_PATH") or os.getenv("REMOTE_SSH_KEY_PATH", "")).strip("'\"")
    ssh_passphrase = (os.getenv("SSH_KEY_PASSPHRASE") or os.getenv("REMOTE_SSH_KEY_PASSPHRASE", "")).strip("'\"")

    stage_db_host = (os.getenv("STAGE_DB_HOST") or os.getenv("DB_HOST", "")).strip("'\"")
    db_name = (os.getenv("DB_NAME", "mls_admin")).strip("'\"")
    db_user = (os.getenv("DB_USER", "")).strip("'\"")
    db_pass = (os.getenv("DB_PASSWORD", "")).strip("'\"")

    # 1. Fetch live active Production MLS IDs (Dynamic check)
    active_prod_mls = get_live_active_prod_mls(logger=logger)
    if not active_prod_mls:
        logger.error("❌ No active MLS sources returned from Production DB. Aborting target generation.")
        return

    # 2. Load static exclusions (Ledger + Manual skip_mls.txt)
    already_processed_mls = load_processed_ledger(TEMP_DATA_DIR)
    manual_skip_mls = load_skip_mls(TEMP_DATA_DIR)

    logger.info(f"Loaded {len(already_processed_mls)} completed MLS ID(s) from ledger.")
    if manual_skip_mls:
        logger.info(f"Loaded {len(manual_skip_mls)} manual exclusion ID(s) from skip_mls.txt.")

    # Exclusions to pass into SQL query
    excluded_mls_list = list(already_processed_mls.union(manual_skip_mls))
    allowed_prod_mls_list = list(active_prod_mls)

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
                (stage_ssh_host, 22),
                ssh_username=ssh_user,
                ssh_pkey=mypkey,
                remote_bind_address=(stage_db_host, 5432),
        ) as tunnel:
            with psycopg2.connect(
                    dbname=db_name,
                    user=db_user,
                    password=db_pass,
                    host="127.0.0.1",
                    port=tunnel.local_bind_port,
            ) as conn:
                with conn.cursor() as cur:
                    logger.info("Step 1: Identifying target active RETS MLS sources in Staging...")
                    query_1 = """
                        WITH distinct_active_rules AS (
                            SELECT DISTINCT 
                                pr.name AS rule_name,
                                m.id AS mls_id
                            FROM public.process_rule pr
                            JOIN public.map_rule_association mra ON pr.id = mra.process_rule_id
                            JOIN public.process_map pm ON mra.process_map_id = pm.id
                            JOIN public.mls_resource mr ON pm.id = mr.process_map_id
                            JOIN public.mls m ON mr.mls_id = m.id
                            JOIN public.download_mls dm ON m.id = dm.mls_id
                            JOIN public.download_config dc ON dc.id = dm.download_config_id
                            WHERE pr.deleted_at IS NULL 
                              AND pr.name IS NOT NULL
                              AND m.mls_status_id = 2
                              AND dc.download_protocol_id = %s
                              AND m.id = ANY(%s)
                              AND NOT (m.id = ANY(%s))
                        ),
                        rule_counts AS (
                            SELECT 
                                rule_name,
                                mls_id,
                                COUNT(*) OVER (PARTITION BY rule_name) AS total_mls_count
                            FROM distinct_active_rules
                        )
                        SELECT DISTINCT rule_name, total_mls_count, mls_id
                        FROM rule_counts
                        ORDER BY total_mls_count ASC, rule_name ASC, mls_id ASC;
                    """
                    cur.execute(query_1, (RETS_PROTOCOL_ID, allowed_prod_mls_list, excluded_mls_list or [-1]))
                    ranked_rows = cur.fetchall()

                    selected_mls_set = set()
                    for rule_name, count, mls_id in ranked_rows:
                        if not is_pascal_case(rule_name):
                            selected_mls_set.add(mls_id)
                            if len(selected_mls_set) == mls_limit:
                                break

                    if not selected_mls_set:
                        logger.info("🎉 No unmigrated legacy rules found for active RETS sources!")
                        return

                    target_mls_list = sorted(list(selected_mls_set))
                    logger.info(f"Selected target Production-Verified RETS MLS IDs ({len(target_mls_list)}): {target_mls_list}")

                    logger.info("Step 2: Fetching granular metadata for target RETS MLS sources...")
                    query_2 = """
                        SELECT DISTINCT
                            pr.name AS legacy_rule_name,
                            m.id AS mls_id,
                            m.id_str AS mls_id_str,
                            m.sa_mls_id,
                            m.sa_mls_id_str,
                            m.mls_status_id,
                            'RETS Integration' AS download_protocol,
                            ctm.content_type,
                            ctm.content_sub_type
                        FROM public.process_rule pr
                        JOIN public.map_rule_association mra ON pr.id = mra.process_rule_id
                        JOIN public.process_map pm ON mra.process_map_id = pm.id
                        JOIN public.mls_resource mr ON pm.id = mr.process_map_id
                        JOIN public.content_type_map ctm ON mr.content_type_map_id = ctm.id
                        JOIN public.mls m ON mr.mls_id = m.id
                        JOIN public.download_mls dm ON m.id = dm.mls_id
                        JOIN public.download_config dc ON dc.id = dm.download_config_id
                        WHERE pr.deleted_at IS NULL
                          AND m.mls_status_id = 2
                          AND dc.download_protocol_id = %s
                          AND m.id = ANY(%s);
                    """
                    cur.execute(query_2, (RETS_PROTOCOL_ID, target_mls_list))
                    metadata_rows = cur.fetchall()

        csv_records = []
        target_rules_set = set()

        for row in metadata_rows:
            rule_name, mls_id, mls_id_str, sa_mls_id, sa_mls_id_str, mls_status_id, download_protocol, content_type, content_sub_type = row

            if not is_pascal_case(rule_name):
                target_rules_set.add(rule_name)
                csv_records.append({
                    "mls_id": mls_id,
                    "mls_id_str": mls_id_str,
                    "sa_mls_id": sa_mls_id if sa_mls_id is not None else "",
                    "sa_mls_id_str": sa_mls_id_str if sa_mls_id_str is not None else "",
                    "mls_status_id": mls_status_id,
                    "download_protocol": download_protocol,
                    "content_type": content_type,
                    "content_sub_type": content_sub_type,
                    "legacy_rule_name": rule_name
                })

        sorted_csv_records = sorted(
            csv_records,
            key=lambda r: (r["mls_id"], r["legacy_rule_name"])
        )
        final_sorted_rules = sorted(list(target_rules_set))

        TEMP_DATA_DIR.mkdir(parents=True, exist_ok=True)

        rets_mls_path = TEMP_DATA_DIR / "rets_mls.txt"
        rets_rules_path = TEMP_DATA_DIR / "rets_rules.txt"
        rets_targets_csv_path = TEMP_DATA_DIR / "batch_mls_targets_rets.csv"

        rets_mls_path.write_text("\n".join(str(m) for m in target_mls_list) + "\n", encoding="utf-8")
        rets_rules_path.write_text("\n".join(final_sorted_rules) + "\n", encoding="utf-8")

        csv_headers = [
            "mls_id", "mls_id_str", "sa_mls_id", "sa_mls_id_str",
            "mls_status_id", "download_protocol", "content_type",
            "content_sub_type", "legacy_rule_name"
        ]

        with open(rets_targets_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=csv_headers)
            writer.writeheader()
            writer.writerows(sorted_csv_records)

        save_unified_working_targets(sorted_csv_records)

        logger.info("============================================================")
        logger.info("🎯 AUTOMATED RETS BATCH SELECTION COMPLETE")
        logger.info("============================================================")
        logger.info(f"Target MLS IDs ({len(target_mls_list)}): {', '.join(str(m) for m in target_mls_list)}")
        logger.info(f"Target Rules ({len(final_sorted_rules)}): {', '.join(final_sorted_rules)}")
        logger.info(f"Generated: {rets_mls_path}")
        logger.info(f"Generated: {rets_rules_path}")
        logger.info(f"Generated: {rets_targets_csv_path}")
        logger.info(f"Updated Working Target: {TEMP_DATA_DIR / 'batch_mls_targets.csv'}")
        logger.info("============================================================")

    except Exception as e:
        logger.error(f"RETS Batch preparation failed: {e}", exc_info=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automated RETS Batch Target Generator (MLS Limit)")
    parser.add_argument(
        "-l", "--limit",
        type=int,
        default=DEFAULT_BATCH_MLS_LIMIT,
        help=f"Number of RETS MLS sources to process in this batch (default: {DEFAULT_BATCH_MLS_LIMIT})"
    )
    args = parser.parse_args()

    generate_batch(mls_limit=args.limit)
