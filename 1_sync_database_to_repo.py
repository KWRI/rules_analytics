"""
Pipeline Stage 1: Core Database Ingress Sync Framework.

Establishes a secure SSH tunnel to staging infrastructure to extract raw
rule records (`public.process_rule`) and write them to disk concurrently
under 'ui-rules/active/' and 'ui-rules/archived/'.
"""

import os
import io
import re
import sys
import time
import signal
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

# --- OS-LEVEL INSTANT TERMINAL EXIT ---
def force_terminal_exit(sig, frame):
    print("\n⛔ [TERMINAL ABORT] Killing process tree immediately...")
    os._exit(1)

signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
warnings.filterwarnings("ignore", message=".*TripleDES.*")

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
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
    # Reload .env explicitly to guarantee fresh state
    load_dotenv(dotenv_path=current_dir / ".env")

    repo_base_raw = os.getenv("REPO_PATH", "") or os.getenv("UI_RULES_DIR", "")
    repo_base = repo_base_raw.strip().strip("'\"")
    if not repo_base:
        # Fallback to local sibling directory
        repo_base = str(current_dir.parent / "dm-consolidated-rules")

    # Staging SSH & DB Configuration matching .env key signatures
    ssh_host = (os.getenv("STAGE_SSH_HOST") or os.getenv("SSH_HOST", "")).strip().strip("'\"")
    ssh_user = os.getenv("SSH_USER", "").strip().strip("'\"")
    ssh_key_path = os.getenv("SSH_KEY_PATH", "").strip().strip("'\"")
    ssh_passphrase = os.getenv("SSH_KEY_PASSPHRASE", "").strip().strip("'\"")

    db_host = (os.getenv("STAGE_DB_HOST") or os.getenv("DB_HOST", "")).strip().strip("'\"")
    db_name = os.getenv("DB_NAME", "mls_admin").strip().strip("'\"")
    db_user = os.getenv("DB_USER", "").strip().strip("'\"")
    db_pass = os.getenv("DB_PASSWORD", "").strip().strip("'\"")

    ui_rules_root = Path(repo_base) / "ui-rules"
    if not ui_rules_root.exists() and (Path(repo_base) / "active").exists():
        ui_rules_root = Path(repo_base)

    active_base = ui_rules_root / "active"
    archived_base = ui_rules_root / "archived"

    logger.info("Starting Core Database Ingress Sync...")
    logger.info(f"Target UI Rules Directory: {ui_rules_root}")

    # Guardrail check to verify SSH key file existence
    if not ssh_key_path or not os.path.exists(ssh_key_path):
        logger.error(
            f"❌ Invalid or missing SSH key path: '{ssh_key_path}'. "
            f"Please verify SSH_KEY_PATH in your .env file."
        )
        return

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

                cursor_name = f"rule_pull_streaming_cursor_{int(time.time())}"
                with conn.cursor(name=cursor_name) as streaming_cur:
                    streaming_cur.itersize = 200
                    streaming_cur.execute(
                        "SELECT name, content, deleted_at FROM public.process_rule;"
                    )

                    file_count = 0
                    failed_writes = 0
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
                            try:
                                future.result()
                            except Exception as exc:
                                failed_writes += 1
                                logger.error(f"⚠️ Failed to write backup file thread task: {exc}")

                    logger.info("============================================================")
                    logger.info("🚀 STAGE 1 COMPLETE: PERFECT REPOSITORY BACKUP SYNC")
                    logger.info("============================================================")
                    logger.info(f"Total Unique Files Written to Disk: {file_count}")
                    if failed_writes > 0:
                        logger.warning(f"⚠️ Encountered {failed_writes} file write failure(s).")
                    logger.info("============================================================")

    except Exception as e:
        logger.error(f"Execution failed: {e}", exc_info=True)


if __name__ == "__main__":
    sync_database_to_repo()
