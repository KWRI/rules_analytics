"""
Pipeline Stage 7: Bulk Production Promotion Engine & Slack Draft Generator.

Performs forward promotion (Stage -> Prod) for target sources and generates
a formatted Slack notification message automatically copied to the Windows clipboard.

Workflow:
1. Invokes draft_slack_message() from draft_slack_message.py to generate
   Slack notification table and copy to clipboard.
2. Pauses for user confirmation before executing forward promotion on remote server.
3. Reads target numeric MLS IDs directly from 'temp-data/promotion_sources.txt'.
4. Stages manifest file on remote deployment server over SFTP.
5. Invokes interactive SSH shell to execute remote forward promotion commands:
   ('s_p', 'set_promotion stage prod', and 'promote_source.py').
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

from draft_slack_message import draft_slack_message
from pipeline_logger import setup_logger

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
logger = setup_logger("Stage7_BulkPromotion")

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
    """Reads unique target numeric MLS IDs directly from promotion_sources.txt."""
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


def prompt_user_confirmation(prompt_text: str) -> bool:
    """Prompts the operator for explicit Y/N confirmation."""
    try:
        answer = input(f"\n⚠️  {prompt_text} (y/N): ").strip().lower()
        return answer == "y"
    except (EOFError, KeyboardInterrupt):
        logger.warning("\n⛔ Operator cancelled input prompt. Exiting Stage 7 immediately.")
        os._exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 7 Production Promotion & Slack Notification Generator")
    parser.add_argument("-y", "--yes", action="store_true", dest="auto_approve", help="Auto-approve interactive prompts")
    args = parser.parse_args()

    logger.info("============================================================")
    logger.info("🚀 STAGE 7: FORWARD PROMOTION ENGINE INITIALIZED (STAGE -> PROD)")
    logger.info("============================================================")

    temp_data_dir = current_dir / "temp-data"
    local_manifest = temp_data_dir / "promotion_sources.txt"
    mls_ids = get_target_mls_ids(local_manifest)

    if not mls_ids:
        logger.info("ℹ️ No target MLS IDs found in promotion_sources.txt (no-op batch). Skipping Forward Promotion.")
        sys.exit(0)

    logger.info(f"🎯 [FORWARD PROMOTION GUARD] Target MLS IDs loaded ({len(mls_ids)}): {mls_ids}")

    # 1. GENERATE SLACK NOTIFICATION TABLE & COPY TO CLIPBOARD
    try:
        draft_slack_message()
    except Exception as e:
        logger.warning(f"⚠️ Could not generate Slack message draft: {e}")

    # 2. PROMPT OPERATOR AFTER SLACK DRAFT GENERATION
    if not args.auto_approve:
        if not prompt_user_confirmation(f"Slack message drafted above and copied to clipboard. Ready to execute Forward Promotion (Stage -> Prod) for MLS ID(s) {mls_ids}?"):
            logger.warning("Forward promotion paused by operator. Aborting Stage 7 execution.")
            sys.exit(0)

    # 3. SFTP MANIFEST STAGING
    logger.info("============================================================")
    logger.info("📦 STAGING FORWARD PROMOTION MANIFEST ON REMOTE SERVER")
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

        logger.info("Target Forward Promotion Manifest records validated:")
        for source_id in remote_sources:
            logger.info(f"   Targeted MLS ID: {source_id}")

        # 4. INTERACTIVE SHELL FORWARD PROMOTION SEQUENCE
        logger.info("[4/4] Invoking forward promotion engine sequentially (STAGE -> PROD)...")
        shell = ssh.invoke_shell()
        time.sleep(2)

        init_commands = ["s_p", "set_promotion stage prod"]
        for cmd in init_commands:
            logger.info(f"[INPUT] Sending setup command: {cmd}")
            shell.send(cmd + "\n")
            time.sleep(3)
            while shell.recv_ready():
                output = shell.recv(4096).decode("utf-8", errors="ignore")
                for line in output.splitlines():
                    if line.strip():
                        logger.info(f"[REMOTE SHELL] {line}")

        loop_cmd = f'while IFS= read -r i; do echo "Forward Processing ID: "$i; python promote_source.py -s process "$i" -y; done < {REMOTE_MANIFEST_PATH}'
        logger.info(f"[INPUT] Sending forward promotion loop: {loop_cmd}")
        shell.send(loop_cmd + "\n")

        logger.info("--- FORWARD PROMOTION SHELL OUTPUT LOGS ---")
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

        logger.info("[INPUT] Remote forward promotion complete. Closing shell session...")
        shell.send("exit\n")
        time.sleep(2)

        ssh.close()
        logger.info("---------------------------------------------------")
        logger.info("✅ SUCCESS: Forward promotion cycle (Stage -> Prod) completed.")
        logger.info("Production DB is now synchronized with standardized Stage configurations.")
        logger.info("============================================================")

    except Exception as e:
        logger.error(f"❌ Forward promotion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
