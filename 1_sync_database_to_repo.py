"""
Pipeline Stage 1: Core Database Ingress Sync Framework.

Establishes a secure SSH tunnel to staging infrastructure to extract raw
rule records (`public.process_rule`) and write them to disk concurrently.
"""

import os
import io
import re
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
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
logger = setup_logger("Stage1_DBSync")


def write_backup_file(target_path: Path, content: str | None) -> None:
    """Worker task executed concurrently across the thread pool."""
    payload = content if content else "# No code content defined in database block\n"
    target_path.write_text(payload, encoding="utf-8")


def sanitize_filename(raw_name: str) -> str:
    """Sanitize rule names into safe filenames, ensuring a single .py extension."""
    clean = raw_name.replace("/", "__").replace("\\", "__")
    clean = re.sub(r'[\x00-\x1f:*?"<>|]', '', clean).strip()

    if not clean:
        clean = "unnamed_rule"

    if not clean.lower().endswith(".py"):
        clean = f"{clean}.py"

    return clean


def sync_database_to_repo():
    repo_base_raw = os.getenv("REPO_PATH", "")
    repo_base = repo_base_raw.strip().strip("'\"")
    if not repo_base:
        logger.error("REPO_PATH not set in environment.")
        return

    ssh_host = os.getenv("SSH_HOST", "").strip().strip("'\"")
    ssh_user = os.getenv("SSH_USER", "").strip().strip("'\"")
    ssh_key_path = os.getenv("SSH_KEY_PATH", "").strip().strip("'\"")
    ssh_passphrase = os.getenv("SSH_KEY_PASSPHRASE", "").strip().strip("'\"")

    db_host = os.getenv("DB_HOST", "").strip().strip("'\"")
    db_name = os.getenv("DB_NAME", "").strip().strip("'\"")
    db_user = os.getenv("DB_USER", "").strip().strip("'\"")
    db_pass = os.getenv("DB_PASSWORD", "").strip().strip("'\"")

    ui_rules_root = Path(repo_base) / "ui-rules"
    active_base = ui_rules_root / "active"
    archived_base = ui_rules_root / "archived"

    logger.info("Starting Core Database Ingress Sync...")
    logger.info(f"Target UI Rules Directory: {ui_rules_root}")

    # Reset directories cleanly
    for folder in [active_base, archived_base]:
        folder.mkdir(parents=True, exist_ok=True)
        for item in folder.iterdir():
            if item.is_file():
                item.unlink()

    try:
        # Load and decrypt OpenSSH/RSA key using cryptography serialization
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

        logger.info(f"Establishing SSH tunnel to {ssh_host}...")
        with SSHTunnelForwarder(
                (ssh_host, 22),
                ssh_username=ssh_user,
                ssh_pkey=mypkey,
                remote_bind_address=(db_host, 5432),
        ) as tunnel:
            logger.info(f"Connected to database '{db_name}' via SSH tunnel port {tunnel.local_bind_port}.")
            with psycopg2.connect(
                    dbname=db_name,
                    user=db_user,
                    password=db_pass,
                    host="127.0.0.1",
                    port=tunnel.local_bind_port,
            ) as conn:
                conn.set_session(isolation_level="REPEATABLE READ", readonly=True)

                with conn.cursor(name="rule_pull_streaming_cursor") as streaming_cur:
                    streaming_cur.itersize = 200
                    streaming_cur.execute(
                        "SELECT name, content, deleted_at FROM public.process_rule;"
                    )

                    file_count = 0
                    seen_active = {}
                    seen_archived = {}
                    futures = []

                    with ThreadPoolExecutor(max_workers=16) as executor:
                        for name, content, deleted_at in streaming_cur:
                            if not name:
                                continue

                            safe_name = sanitize_filename(name)
                            stem = safe_name[:-3]
                            lowercase_stem = stem.lower()

                            if deleted_at:
                                target_dir = archived_base
                                tracker = seen_archived
                            else:
                                target_dir = active_base
                                tracker = seen_active

                            if lowercase_stem in tracker:
                                tracker[lowercase_stem] += 1
                                final_filename = f"{stem}_collision_{tracker[lowercase_stem]}.py"
                            else:
                                tracker[lowercase_stem] = 0
                                final_filename = safe_name

                            target_file = target_dir / final_filename
                            futures.append(
                                executor.submit(write_backup_file, target_file, content)
                            )
                            file_count += 1

                        for future in as_completed(futures):
                            future.result()

                    logger.info("============================================================")
                    logger.info("🚀 STAGE 1 COMPLETE: PERFECT REPOSITORY BACKUP SYNC")
                    logger.info("============================================================")
                    logger.info(f"Total Unique Files Written to Disk: {file_count}")
                    logger.info("============================================================")

    except Exception as e:
        logger.error(f"Execution failed: {e}", exc_info=True)


if __name__ == "__main__":
    sync_database_to_repo()
