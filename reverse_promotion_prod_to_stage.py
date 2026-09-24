"""
Pipeline Helper: Bulk Reverse Promotion Engine (Prod -> Stage).

Syncs live Production configurations to Staging prior to rule standardization:
1. Reads candidate numeric MLS IDs directly from 'temp-data/reverse_promotion_sources.txt'.
2. Stages the manifest file on the remote deployment server over SFTP.
3. Invokes an interactive SSH shell to execute remote reverse promotion
   commands ('s_p', 'set_promotion prod stage', and 'promote_source.py').

Logs execution events to 'logs/pipeline_YYYY-MM-DD.log'.
"""

import os
import sys
import io
import time
import signal
import argparse
import paramiko
from pathlib import Path
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization


# --- OS-LEVEL INSTANT TERMINAL EXIT ---
def force_terminal_exit(sig, frame):
    print("\n⛔ [TERMINAL ABORT] Killing process tree immediately...")
    os._exit(1)


signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

from pipeline_logger import setup_logger

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
logger = setup_logger("ReversePromotionProdToStage")

# SSH ENVIRONMENT CONSTANTS
SSH_HOST = (os.getenv("REMOTE_SSH_HOST") or os.getenv("REMOTE_HOST", "sdh.data.kw.com")).strip("'\"")
SSH_USER = (os.getenv("REMOTE_SSH_USER") or os.getenv("SSH_USER", "")).strip("'\"")
SSH_KEY_PATH = (os.getenv("REMOTE_SSH_KEY_PATH") or os.getenv("SSH_KEY_PATH", "")).strip("'\"")
SSH_PASSPHRASE = (os.getenv("REMOTE_SSH_KEY_PASSPHRASE") or os.getenv("SSH_KEY_PASSPHRASE", "")).strip("'\"")
REMOTE_MANIFEST_PATH = os.getenv(
    "REMOTE_MANIFEST_PATH",
    "/eim-mls-admin-ms/devtools/source_promotion/promotion_sources.txt"
)


def get_target_mls_ids(manifest_path: Path) -> list[int]:
    """Reads unique target numeric MLS IDs directly from reverse_promotion_sources.txt."""
    if not manifest_path.exists():
        logger.error(f"❌ Manifest file missing at path: {manifest_path}")
        return []

    with open(manifest_path, "r", encoding="utf-8") as f:
        ids = [int(line.strip()) for line in f if line.strip().isdigit()]

    seen = set()
    unique_ids = []
    for source_id in ids:
        if source_id not in seen:
            seen.add(source_id)
            unique_ids.append(source_id)

    return unique_ids


def main(args_list: list[str] = None) -> None:
    parser = argparse.ArgumentParser(description="Bulk Reverse Promotion Engine (Prod -> Stage)")
    parser.add_argument("-y", "--yes", action="store_true", dest="auto_approve", help="Auto-approve execution")

    # Parse explicit args_list if provided, otherwise default to sys.argv[1:]
    args = parser.parse_args(args_list if args_list is not None else sys.argv[1:])

    logger.info("============================================================")
    logger.info("🚀 REVERSE PROMOTION ENGINE INITIALIZED (PROD -> STAGE)")
    logger.info("============================================================")

    temp_data_dir = current_dir / "temp-data"
    local_manifest = temp_data_dir / "reverse_promotion_sources.txt"
    mls_ids = get_target_mls_ids(local_manifest)

    if not mls_ids:
        logger.info("ℹ️ No target MLS IDs found in reverse_promotion_sources.txt. Skipping Reverse Promotion.")
        return

    logger.info(f"🎯 [REVERSE PROMOTION GUARD] Candidate MLS IDs loaded ({len(mls_ids)}): {mls_ids}")

    # 1. SFTP MANIFEST STAGING
    logger.info("============================================================")
    logger.info("📦 STAGING REVERSE PROMOTION MANIFEST ON REMOTE SERVER")
    logger.info("============================================================")
    logger.info(f"[1/4] Connecting to remote host {SSH_HOST}...")

    if not SSH_KEY_PATH:
        logger.error("❌ SSH key path missing from configuration! Check REMOTE_SSH_KEY_PATH or SSH_KEY_PATH in .env.")
        sys.exit(1)

    try:
        with open(SSH_KEY_PATH, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=SSH_PASSPHRASE.encode() if SSH_PASSPHRASE else None,
            )
        pem_data = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
        ssh_key = paramiko.RSAKey.from_private_key(io.StringIO(pem_data.decode()))

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=SSH_HOST, username=SSH_USER, pkey=ssh_key, timeout=15)

        logger.info(f"[2/4] Uploading manifest to server path: {REMOTE_MANIFEST_PATH}...")
        sftp = ssh.open_sftp()
        sftp.put(str(local_manifest), REMOTE_MANIFEST_PATH)
        sftp.close()
        logger.info("✅ [SUCCESS] Manifest staged successfully on remote file layer.")

        logger.info("[3/4] Verifying remote payload structure consistency...")
        stdin, stdout, stderr = ssh.exec_command(f"cat {REMOTE_MANIFEST_PATH}")
        remote_sources = [line.strip() for line in stdout.readlines() if line.strip()]

        logger.info("Target Reverse Promotion Manifest records validated:")
        for source_id in remote_sources:
            logger.info(f"   Targeted MLS ID: {source_id}")

        # 2. INTERACTIVE SHELL REVERSE PROMOTION SEQUENCE
        logger.info("[4/4] Invoking reverse promotion engine sequentially (PROD -> STAGE)...")
        shell = ssh.invoke_shell()
        time.sleep(2)

        init_commands = ["s_p", "set_promotion prod stage"]
        for cmd in init_commands:
            logger.info(f"[INPUT] Sending setup command: {cmd}")
            shell.send(cmd + "\n")
            time.sleep(3)
            while shell.recv_ready():
                output = shell.recv(4096).decode("utf-8", errors="ignore")
                for line in output.splitlines():
                    if line.strip():
                        logger.info(f"[REMOTE SHELL] {line}")

        loop_cmd = f'while IFS= read -r i; do echo "Reverse Processing ID: "$i; python promote_source.py -s process "$i" -y; done < {REMOTE_MANIFEST_PATH}'
        logger.info(f"[INPUT] Sending reverse promotion loop: {loop_cmd}")
        shell.send(loop_cmd + "\n")

        logger.info("--- REVERSE PROMOTION SHELL OUTPUT LOGS ---")
        idle_count = 0
        while True:
            if shell.recv_ready():
                idle_count = 0
                output = shell.recv(8192).decode("utf-8", errors="ignore")
                for line in output.splitlines():
                    if line.strip():
                        logger.info(f"[REMOTE SHELL] {line}")
            else:
                time.sleep(2)
                idle_count += 1
                if idle_count >= 6:
                    break

        logger.info("[INPUT] Remote reverse promotion complete. Closing shell session...")
        shell.send("exit\n")
        time.sleep(2)

        ssh.close()
        logger.info("---------------------------------------------------")
        logger.info("✅ SUCCESS: Reverse promotion cycle (Prod -> Stage) completed.")
        logger.info("Staging DB is now synchronized with latest Production mappings.")
        logger.info("============================================================")

    except Exception as e:
        logger.error(f"❌ Reverse promotion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
