"""
Pipeline Utilities Module.

Provides shared configuration loaders, target file parsers, ledger trackers,
manual skip list managers, and real-time Production DB status verification.
"""

import os
import io
import json
from pathlib import Path
import psycopg2
import paramiko
from sshtunnel import SSHTunnelForwarder
from cryptography.hazmat.primitives import serialization


def load_target_rules(temp_data_dir: Path) -> set[str]:
    """
    Reads rule names from 'api_rules.txt' and 'rets_rules.txt' inside temp-data/.
    Returns a unified set of rule names. Safely skips missing files or commented lines.
    """
    target_rules = set()
    candidate_files = [
        temp_data_dir / "api_rules.txt",
        temp_data_dir / "rets_rules.txt",
    ]

    for rule_file in candidate_files:
        if rule_file.exists():
            lines = rule_file.read_text(encoding="utf-8").splitlines()
            for line in lines:
                cleaned = line.strip().strip("'\"")
                if cleaned and not cleaned.startswith("#"):
                    target_rules.add(cleaned)

    return target_rules


def load_target_mls(temp_data_dir: Path) -> set[int]:
    """
    Reads numeric MLS IDs from 'api_mls.txt' and 'rets_mls.txt' inside temp-data/.
    Returns a unified set of integer MLS IDs (e.g., {189, 533}).
    Safely skips missing files, non-numeric values, or commented lines.
    """
    target_ids = set()
    candidate_files = [
        temp_data_dir / "api_mls.txt",
        temp_data_dir / "rets_mls.txt",
    ]

    for mls_file in candidate_files:
        if mls_file.exists():
            lines = mls_file.read_text(encoding="utf-8").splitlines()
            for line in lines:
                cleaned = line.strip().strip("'\"")
                if cleaned and not cleaned.startswith("#"):
                    try:
                        target_ids.add(int(cleaned))
                    except ValueError:
                        print(f"⚠️ Warning: Skipping non-numeric MLS ID '{cleaned}' in {mls_file.name}")

    return target_ids


def load_processed_ledger(temp_data_dir: Path) -> set[int]:
    """
    Reads previously processed MLS IDs from 'processed_mls_ledger.json' inside temp-data/.
    Returns a set of integer MLS IDs.
    """
    ledger_file = temp_data_dir / "processed_mls_ledger.json"
    if not ledger_file.exists():
        return set()

    try:
        data = json.loads(ledger_file.read_text(encoding="utf-8"))
        return set(int(x) for x in data.get("completed_mls_ids", []))
    except Exception:
        return set()


def sort_and_format_skip_file(skip_file: Path) -> None:
    """
    Cleans, sorts in ascending order by MLS ID, and overwrites skip_mls.txt.
    Eliminates leading blank lines and duplicate entries while preserving comments.
    """
    if not skip_file.exists():
        return

    entries = {}
    lines = skip_file.read_text(encoding="utf-8").splitlines()

    for line in lines:
        raw_line = line.strip()
        if not raw_line or raw_line.startswith("#"):
            continue

        parts = raw_line.split("#", 1)
        mls_str = parts[0].strip()
        comment = parts[1].strip() if len(parts) > 1 else ""

        if mls_str.isdigit():
            mls_id = int(mls_str)
            if mls_id in entries:
                if comment and comment not in entries[mls_id]:
                    entries[mls_id] = f"{entries[mls_id]}; {comment}" if entries[mls_id] else comment
            else:
                entries[mls_id] = comment

    if not entries:
        return

    sorted_lines = []
    for mls_id in sorted(entries.keys()):
        comment = entries[mls_id]
        if comment:
            sorted_lines.append(f"{mls_id}  # {comment}")
        else:
            sorted_lines.append(f"{mls_id}")

    formatted_content = "\n".join(sorted_lines) + "\n"
    skip_file.write_text(formatted_content, encoding="utf-8")


def load_skip_mls(temp_data_dir: Path) -> set[int]:
    """
    Checks for 'skip_mls.txt' across temp-data/, root project, or script directory.
    Merges entries across all locations and formats files in-place.
    """
    candidates = [
        temp_data_dir / "skip_mls.txt",
        temp_data_dir.parent / "skip_mls.txt",
        Path(__file__).resolve().parent / "skip_mls.txt"
    ]

    skip_ids = set()
    for skip_file in candidates:
        if skip_file.exists():
            sort_and_format_skip_file(skip_file)

            for line in skip_file.read_text(encoding="utf-8").splitlines():
                cleaned = line.split("#")[0].strip()
                if cleaned.isdigit():
                    skip_ids.add(int(cleaned))

    return skip_ids


def append_to_skip_file(temp_data_dir: Path, mls_id: int, reason: str) -> None:
    """Appends an invalid/inactive MLS ID to skip_mls.txt and auto-sorts the file."""
    temp_data_dir.mkdir(parents=True, exist_ok=True)
    skip_file = temp_data_dir / "skip_mls.txt"

    existing_skips = load_skip_mls(temp_data_dir)

    if mls_id not in existing_skips:
        with open(skip_file, "a", encoding="utf-8") as f:
            f.write(f"\n{mls_id}  # {reason}\n")

        sort_and_format_skip_file(skip_file)


def get_live_active_prod_mls(logger=None) -> set[int]:
    """
    Queries Production DB in real time for all currently active MLS sources (mls_status_id = 2).
    Always returns fresh state to seamlessly handle status flips across pipeline runs.
    """
    prod_ssh_host = (os.getenv("PROD_SSH_HOST") or os.getenv("REMOTE_SSH_HOST") or os.getenv("SSH_HOST", "")).strip("'\"")
    prod_ssh_user = (os.getenv("SSH_USER") or os.getenv("REMOTE_SSH_USER", "")).strip("'\"")
    prod_ssh_key = (os.getenv("SSH_KEY_PATH") or os.getenv("REMOTE_SSH_KEY_PATH", "")).strip("'\"")
    prod_ssh_pass = (os.getenv("SSH_KEY_PASSPHRASE") or os.getenv("REMOTE_SSH_KEY_PASSPHRASE", "")).strip("'\"")

    prod_db_host = (os.getenv("PROD_DB_HOST") or os.getenv("DB_HOST", "")).strip("'\"")
    prod_db_name = (os.getenv("DB_NAME", "mls_admin")).strip("'\"")
    prod_db_user = (os.getenv("DB_USER", "")).strip("'\"")
    prod_db_pass = (os.getenv("DB_PASSWORD", "")).strip("'\"")

    if logger:
        logger.info("🔍 Fetching live active MLS IDs from Production DB (mls_status_id = 2)...")

    active_prod_ids = set()

    try:
        with open(prod_ssh_key, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=prod_ssh_pass.encode() if prod_ssh_pass else None,
            )
        pem_data = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
        mypkey = paramiko.RSAKey.from_private_key(io.StringIO(pem_data.decode()))

        with SSHTunnelForwarder(
            (prod_ssh_host, 22),
            ssh_username=prod_ssh_user,
            ssh_pkey=mypkey,
            remote_bind_address=(prod_db_host, 5432),
        ) as tunnel:
            with psycopg2.connect(
                dbname=prod_db_name,
                user=prod_db_user,
                password=prod_db_pass,
                host="127.0.0.1",
                port=tunnel.local_bind_port,
            ) as conn:
                with conn.cursor() as cur:
                    query = "SELECT id FROM public.mls WHERE mls_status_id = 2;"
                    cur.execute(query)
                    active_prod_ids = {int(row[0]) for row in cur.fetchall()}

        if logger:
            logger.info(f"✅ Production Check Complete: Identified {len(active_prod_ids)} active MLS ID(s) in Production.")

    except Exception as err:
        if logger:
            logger.error(f"❌ Failed real-time Production active check: {err}", exc_info=True)

    return active_prod_ids
