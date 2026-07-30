"""
Pipeline Stage 0: Automated Batch Target Generator.

Queries the staging database to find unmigrated legacy rules across active MLS sources.
Generates 'temp-data/test_mls.txt' and 'temp-data/test_rules.txt' dynamically based on
a configurable MLS target limit, maintaining a local ledger to prevent duplicate work.

Usage:
    python 0_prepare_batch_targets.py           # Uses default limit (5 MLSs)
    python 0_prepare_batch_targets.py --limit 2 # Overrides limit to 2 MLSs
    python 0_prepare_batch_targets.py -l 10    # Overrides limit to 10 MLSs
"""

import os
import io
import re
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

load_dotenv()
logger = setup_logger("Stage0_BatchPrepare")

DEFAULT_BATCH_MLS_LIMIT = 5
PROCESSED_LOG_FILE = Path("temp-data/processed_mls_ledger.json")
TEMP_DATA_DIR = Path("temp-data")


def is_pascal_case(name: str) -> bool:
    """
    Checks if a rule name is already standard PascalCase.
    Rejects names with underscores, spaces, or leading numbers/prefixes
    (e.g., '102_str_to_array', '33MunicipalCode').
    """
    if re.match(r'^\d', name) or '_' in name or ' ' in name or '-' in name:
        return False
    return bool(re.match(r'^[A-Z][a-zA-Z0-9]*$', name))


def load_processed_mls() -> set[str]:
    """Loads previously completed MLS IDs from the ledger."""
    if PROCESSED_LOG_FILE.exists():
        try:
            data = json.loads(PROCESSED_LOG_FILE.read_text(encoding="utf-8"))
            return set(data.get("completed_mls_ids", []))
        except Exception as e:
            logger.warning(f"Failed to read ledger file: {e}")
            return set()
    return set()


def parse_mls_int(mls_id: str) -> int:
    """Helper to safely convert MLS ID strings to integers for numerical sorting."""
    try:
        return int(mls_id)
    except ValueError:
        return 99999999  # Fallback for non-numeric IDs to push them to the end


def generate_batch(batch_limit: int):
    ssh_host = os.getenv("SSH_HOST", "").strip("'\"")
    ssh_user = os.getenv("SSH_USER", "").strip("'\"")
    ssh_key_path = os.getenv("SSH_KEY_PATH", "").strip("'\"")
    ssh_passphrase = os.getenv("SSH_KEY_PASSPHRASE", "").strip("'\"")

    db_host = os.getenv("DB_HOST", "").strip("'\"")
    db_name = os.getenv("DB_NAME", "").strip("'\"")
    db_user = os.getenv("DB_USER", "").strip("'\"")
    db_pass = os.getenv("DB_PASSWORD", "").strip("'\"")

    already_processed_mls = load_processed_mls()
    logger.info(f"Loaded {len(already_processed_mls)} previously processed MLS ID(s) from ledger.")
    logger.info(f"Target MLS batch size: {batch_limit}")

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

        logger.info("Connecting to database via SSH tunnel to identify unmigrated rules...")
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
                        SELECT DISTINCT 
                            pr.name AS rule_name, 
                            mr.mls_id::text AS mls_id
                        FROM public.process_rule pr
                        JOIN public.map_rule_association mra ON pr.id = mra.process_rule_id
                        JOIN public.mls_resource mr ON mra.process_map_id = mr.process_map_id
                        JOIN public.mls m ON mr.mls_id = m.id
                        WHERE pr.deleted_at IS NULL 
                          AND pr.name IS NOT NULL;
                    """
                    cur.execute(query)
                    rows = cur.fetchall()

        # Aggregate unmigrated rules by MLS ID
        mls_to_legacy_rules = {}
        for rule_name, mls_id in rows:
            if mls_id in already_processed_mls:
                continue

            if not is_pascal_case(rule_name):
                mls_to_legacy_rules.setdefault(mls_id, set()).add(rule_name)

        if not mls_to_legacy_rules:
            logger.info("🎉 No remaining unmigrated legacy rules found for unprocessed MLS sources!")
            return

        # Sort available MLS IDs:
        # Primary: Ascending order of unmigrated rule count
        # Secondary: Ascending numerical MLS ID
        sorted_mls = sorted(
            mls_to_legacy_rules.keys(),
            key=lambda mls: (len(mls_to_legacy_rules[mls]), parse_mls_int(mls))
        )

        # Pick top batch_limit targets
        selected_mls_slice = sorted_mls[:batch_limit]

        # Sort selected MLS targets in strict ascending numeric order for clean file output
        final_sorted_mls = sorted(selected_mls_slice, key=parse_mls_int)

        # Collect all legacy rules associated with selected MLS sources
        selected_rules = set()
        for mls in final_sorted_mls:
            selected_rules.update(mls_to_legacy_rules[mls])

        # Write output files into temp-data/
        TEMP_DATA_DIR.mkdir(parents=True, exist_ok=True)

        test_mls_path = TEMP_DATA_DIR / "test_mls.txt"
        test_rules_path = TEMP_DATA_DIR / "test_rules.txt"

        test_mls_path.write_text("\n".join(final_sorted_mls) + "\n", encoding="utf-8")
        test_rules_path.write_text("\n".join(sorted(selected_rules)) + "\n", encoding="utf-8")

        logger.info("============================================================")
        logger.info("🎯 AUTOMATED BATCH SELECTION COMPLETE")
        logger.info("============================================================")
        logger.info(f"Target MLS IDs ({len(final_sorted_mls)}): {', '.join(final_sorted_mls)}")
        logger.info(f"Target Legacy Rules ({len(selected_rules)}): {', '.join(sorted(selected_rules))}")
        logger.info(f"Generated: {test_mls_path}")
        logger.info(f"Generated: {test_rules_path}")
        logger.info("============================================================")

    except Exception as e:
        logger.error(f"Batch preparation failed: {e}", exc_info=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automated Batch Target Generator")
    parser.add_argument(
        "-l", "--limit",
        type=int,
        default=DEFAULT_BATCH_MLS_LIMIT,
        help=f"Number of MLS sources to process in this batch (default: {DEFAULT_BATCH_MLS_LIMIT})"
    )
    args = parser.parse_args()

    generate_batch(batch_limit=args.limit)
