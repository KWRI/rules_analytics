"""
Pipeline Stage 0: Automated Batch Target Generator.

Queries the staging database to find unmigrated legacy rules across active MLS sources.
Generates 'temp-data/test_mls.txt' and 'temp-data/test_rules.txt' dynamically based on
a configurable MLS target limit, maintaining a local history log to prevent duplicate work.
"""

import os
import io
import re
import json
import warnings
from pathlib import Path
import psycopg2
import paramiko
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from cryptography.utils import CryptographyDeprecationWarning
from cryptography.hazmat.primitives import serialization

warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
warnings.filterwarnings("ignore", message=".*TripleDES.*")

load_dotenv()

# Configuration
BATCH_MLS_LIMIT = 5  # Number of MLS sources to process in one batch
PROCESSED_LOG_FILE = Path("temp-data/processed_mls_ledger.json")
TEMP_DATA_DIR = Path("temp-data")


def is_pascal_case(name: str) -> bool:
    """
    Checks if a rule name is already standard PascalCase.
    Rejects names with underscores, spaces, or leading numbers/prefixes (e.g., '102_str_to_array', '33MunicipalCode').
    """
    # Reject if it starts with digits or contains special characters/underscores/spaces
    if re.match(r'^\d', name) or '_' in name or ' ' in name or '-' in name:
        return False
    # Valid PascalCase pattern
    return bool(re.match(r'^[A-Z][a-zA-Z0-9]*$', name))


def load_processed_mls() -> set[str]:
    """Loads previously completed MLS IDs from the ledger."""
    if PROCESSED_LOG_FILE.exists():
        try:
            data = json.loads(PROCESSED_LOG_FILE.read_text(encoding="utf-8"))
            return set(data.get("completed_mls_ids", []))
        except Exception:
            return set()
    return set()


def save_processed_mls(completed_mls: set[str]) -> None:
    """Saves completed MLS IDs to the ledger."""
    PROCESSED_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {"completed_mls_ids": sorted(list(completed_mls))}
    PROCESSED_LOG_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def generate_batch():
    ssh_host = os.getenv("SSH_HOST", "").strip("'\"")
    ssh_user = os.getenv("SSH_USER", "").strip("'\"")
    ssh_key_path = os.getenv("SSH_KEY_PATH", "").strip("'\"")
    ssh_passphrase = os.getenv("SSH_KEY_PASSPHRASE", "").strip("'\"")

    db_host = os.getenv("DB_HOST", "").strip("'\"")
    db_name = os.getenv("DB_NAME", "").strip("'\"")
    db_user = os.getenv("DB_USER", "").strip("'\"")
    db_pass = os.getenv("DB_PASSWORD", "").strip("'\"")

    already_processed_mls = load_processed_mls()
    print(f"📋 Loaded {len(already_processed_mls)} previously processed MLS ID(s) from ledger.")

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
                    # Query active rules and their associated MLS IDs
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

            # Identify rules that need standardization
            if not is_pascal_case(rule_name):
                mls_to_legacy_rules.setdefault(mls_id, set()).add(rule_name)

        if not mls_to_legacy_rules:
            print("🎉 No remaining unmigrated legacy rules found for unprocessed MLS sources!")
            return

        # Sort MLS IDs by fewest unmigrated rules to process manageable batches
        sorted_mls = sorted(mls_to_legacy_rules.keys(), key=lambda k: len(mls_to_legacy_rules[k]))
        selected_mls = sorted_mls[:BATCH_MLS_LIMIT]

        selected_rules = set()
        for mls in selected_mls:
            selected_rules.update(mls_to_legacy_rules[mls])

        # Write output files into temp-data/
        TEMP_DATA_DIR.mkdir(parents=True, exist_ok=True)

        test_mls_path = TEMP_DATA_DIR / "test_mls.txt"
        test_rules_path = TEMP_DATA_DIR / "test_rules.txt"

        test_mls_path.write_text("\n".join(sorted(selected_mls)) + "\n", encoding="utf-8")
        test_rules_path.write_text("\n".join(sorted(selected_rules)) + "\n", encoding="utf-8")

        print("\n" + "=" * 60)
        print("🎯 AUTOMATED BATCH SELECTION COMPLETE")
        print("=" * 60)
        print(f" Target MLS IDs ({len(selected_mls)}): {', '.join(selected_mls)}")
        print(f" Target Legacy Rules ({len(selected_rules)}): {', '.join(sorted(selected_rules))}")
        print(f" Generated: {test_mls_path}")
        print(f" Generated: {test_rules_path}")
        print("=" * 60 + "\n")

    except Exception as e:
        print(f"❌ Batch preparation failed: {e}")


if __name__ == "__main__":
    generate_batch()
