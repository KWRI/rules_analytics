"""
Pipeline Stage 0: Two-Step High-Performance Batch Target Generator.

Targets ONLY ACTIVE API MLS sources (mls_status_id = 2 & REST/OAuth2 protocols).
Generates 'test_mls.txt', 'test_rules.txt', and 'batch_mls_targets.csv' based on
a configurable MLS target limit (--limit / -l).

Sorting Hierarchy:
    1. Rules ranked by distinct active API MLS count (ASCENDING)
    2. Rules ranked by Rule Name (ASCENDING)
    3. Affected MLSs ranked by integer MLS ID (ASCENDING)

Usage:
    python 0_batch_prepare.py           # Uses default limit (5 MLSs)
    python 0_batch_prepare.py --limit 2 # Picks targets across 2 distinct MLSs
    python 0_batch_prepare.py -l 10     # Picks targets across 10 distinct MLSs
"""

import os
import io
import re
import csv
import json
import argparse
import warnings
from pathlib import Path
import psycopg2
import paramiko
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from cryptography.utils import CryptographyDeprecationWarning
from cryptography.hazmat.primitives import serialization

from pipeline_logger import setup_logger

warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
warnings.filterwarnings("ignore", message=".*TripleDES.*")

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
logger = setup_logger("Stage0_BatchPrepare")

DEFAULT_BATCH_MLS_LIMIT = 5
TEMP_DATA_DIR = current_dir / "temp-data"
PROCESSED_LOG_FILE = TEMP_DATA_DIR / "processed_mls_ledger.json"

# Protocol IDs (REST & OAuth2)
REST_PROTOCOL_ID = 3487705513625387016
OAUTH2_PROTOCOL_ID = 3487705513674153992


def is_pascal_case(name: str) -> bool:
    """
    Checks if a rule name is standard PascalCase.
    Rejects names with underscores, spaces, hyphens, slashes, or leading digits.
    """
    if re.match(r'^\d', name) or '_' in name or ' ' in name or '-' in name or '/' in name:
        return False
    return bool(re.match(r'^[A-Z][a-zA-Z0-9]*$', name))


def load_processed_mls() -> set[int]:
    """Loads previously completed integer MLS IDs from the local ledger."""
    if PROCESSED_LOG_FILE.exists():
        try:
            data = json.loads(PROCESSED_LOG_FILE.read_text(encoding="utf-8"))
            return set(int(x) for x in data.get("completed_mls_ids", []))
        except Exception as e:
            logger.warning(f"Failed to read ledger file: {e}")
            return set()
    return set()


def generate_batch(mls_limit: int):
    """
    Selects up to `mls_limit` distinct active API MLS sources starting from the
    lowest-impact legacy rules, and writes test_mls.txt, test_rules.txt, and batch_mls_targets.csv.
    """
    ssh_host = os.getenv("SSH_HOST", "").strip("'\"")
    ssh_user = os.getenv("SSH_USER", "").strip("'\"")
    ssh_key_path = os.getenv("SSH_KEY_PATH", "").strip("'\"")
    ssh_passphrase = os.getenv("SSH_KEY_PASSPHRASE", "").strip("'\"")

    db_host = os.getenv("DB_HOST", "").strip("'\"")
    db_name = os.getenv("DB_NAME", "").strip("'\"")
    db_user = os.getenv("DB_USER", "shrisha_vanga").strip("'\"")
    db_pass = os.getenv("DB_PASSWORD", "").strip("'\"")

    already_processed_mls = load_processed_mls()
    logger.info(f"Loaded {len(already_processed_mls)} previously processed MLS ID(s) from ledger.")
    logger.info(f"Target MLS batch size limit: {mls_limit}")

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

        logger.info("Connecting via SSH tunnel to staging database...")
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
                    # -------------------------------------------------------------
                    # QUERY 1: Map legacy rules to active API MLS IDs, ordered by rule impact
                    # -------------------------------------------------------------
                    logger.info("Step 1: Identifying and ranking target active API MLS sources...")
                    query_1 = """
                        WITH rule_counts AS (
                            SELECT 
                                pr.name AS rule_name,
                                COUNT(DISTINCT m.id) AS total_mls_count
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
                              AND dc.download_protocol_id IN (%s, %s)
                            GROUP BY pr.name
                        )
                        SELECT DISTINCT
                            rc.rule_name,
                            rc.total_mls_count,
                            m.id AS mls_id
                        FROM rule_counts rc
                        JOIN public.process_rule pr ON pr.name = rc.rule_name
                        JOIN public.map_rule_association mra ON pr.id = mra.process_rule_id
                        JOIN public.process_map pm ON mra.process_map_id = pm.id
                        JOIN public.mls_resource mr ON pm.id = mr.process_map_id
                        JOIN public.mls m ON mr.mls_id = m.id
                        JOIN public.download_mls dm ON m.id = dm.mls_id
                        JOIN public.download_config dc ON dc.id = dm.download_config_id
                        WHERE pr.deleted_at IS NULL
                          AND m.mls_status_id = 2
                          AND dc.download_protocol_id IN (%s, %s)
                        ORDER BY rc.total_mls_count ASC, rc.rule_name ASC, mls_id ASC;
                    """
                    cur.execute(
                        query_1,
                        (REST_PROTOCOL_ID, OAUTH2_PROTOCOL_ID, REST_PROTOCOL_ID, OAUTH2_PROTOCOL_ID)
                    )
                    ranked_rows = cur.fetchall()

                    # Walk down ranked rules to pick up to 'mls_limit' distinct MLS IDs
                    selected_mls_set = set()
                    for rule_name, count, mls_id in ranked_rows:
                        if mls_id in already_processed_mls:
                            continue

                        if not is_pascal_case(rule_name):
                            selected_mls_set.add(mls_id)
                            if len(selected_mls_set) == mls_limit:
                                break

                    if not selected_mls_set:
                        logger.info("🎉 No unmigrated legacy rules found for unprocessed active API sources!")
                        return

                    target_mls_list = sorted(list(selected_mls_set))
                    logger.info(f"Selected target MLS IDs ({len(target_mls_list)}): {target_mls_list}")

                    # -------------------------------------------------------------
                    # QUERY 2: Fetch detailed metadata for ALL legacy rules on selected MLSs
                    # -------------------------------------------------------------
                    logger.info("Step 2: Fetching granular metadata for target MLS sources...")
                    query_2 = """
                        SELECT DISTINCT
                            pr.name AS legacy_rule_name,
                            m.id AS mls_id,
                            m.id_str AS mls_id_str,
                            m.mls_status_id,
                            CASE 
                                WHEN dc.download_protocol_id = %s THEN 'REST'
                                WHEN dc.download_protocol_id = %s THEN 'OAuth2'
                                ELSE 'UNKNOWN'
                            END AS download_protocol,
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
                          AND dc.download_protocol_id IN (%s, %s)
                          AND m.id = ANY(%s);
                    """
                    cur.execute(
                        query_2,
                        (REST_PROTOCOL_ID, OAUTH2_PROTOCOL_ID, REST_PROTOCOL_ID, OAUTH2_PROTOCOL_ID, target_mls_list)
                    )
                    metadata_rows = cur.fetchall()

        # Filter out PascalCase rules and build records
        csv_records = []
        target_rules_set = set()

        for row in metadata_rows:
            rule_name, mls_id, mls_id_str, mls_status_id, download_protocol, content_type, content_sub_type = row

            if not is_pascal_case(rule_name):
                target_rules_set.add(rule_name)
                csv_records.append({
                    "mls_id": mls_id,
                    "mls_id_str": mls_id_str,
                    "mls_status_id": mls_status_id,
                    "download_protocol": download_protocol,
                    "content_type": content_type,
                    "content_sub_type": content_sub_type,
                    "legacy_rule_name": rule_name
                })

        # Sort CSV records strictly by: MLS ID ASC -> Rule Name ASC
        sorted_csv_records = sorted(
            csv_records,
            key=lambda r: (r["mls_id"], r["legacy_rule_name"])
        )

        final_sorted_rules = sorted(list(target_rules_set))

        # Write output files into temp-data/
        TEMP_DATA_DIR.mkdir(parents=True, exist_ok=True)

        test_mls_path = TEMP_DATA_DIR / "test_mls.txt"
        test_rules_path = TEMP_DATA_DIR / "test_rules.txt"
        batch_targets_csv_path = TEMP_DATA_DIR / "batch_mls_targets.csv"

        test_mls_path.write_text("\n".join(str(m) for m in target_mls_list) + "\n", encoding="utf-8")
        test_rules_path.write_text("\n".join(final_sorted_rules) + "\n", encoding="utf-8")

        csv_headers = [
            "mls_id",
            "mls_id_str",
            "mls_status_id",
            "download_protocol",
            "content_type",
            "content_sub_type",
            "legacy_rule_name"
        ]

        with open(batch_targets_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=csv_headers)
            writer.writeheader()
            for rec in sorted_csv_records:
                writer.writerow({k: rec[k] for k in csv_headers})

        logger.info("============================================================")
        logger.info("🎯 AUTOMATED BATCH SELECTION COMPLETE")
        logger.info("============================================================")
        logger.info(f"Target MLS IDs ({len(target_mls_list)}): {', '.join(str(m) for m in target_mls_list)}")
        logger.info(f"Target Rules ({len(final_sorted_rules)}): {', '.join(final_sorted_rules)}")
        logger.info(f"Generated: {test_mls_path}")
        logger.info(f"Generated: {test_rules_path}")
        logger.info(f"Generated: {batch_targets_csv_path}")
        logger.info("============================================================")

    except Exception as e:
        logger.error(f"Batch preparation failed: {e}", exc_info=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automated Batch Target Generator (MLS Limit)")
    parser.add_argument(
        "-l", "--limit",
        type=int,
        default=DEFAULT_BATCH_MLS_LIMIT,
        help=f"Number of MLS sources to process in this batch (default: {DEFAULT_BATCH_MLS_LIMIT})"
    )
    args = parser.parse_args()

    generate_batch(mls_limit=args.limit)
