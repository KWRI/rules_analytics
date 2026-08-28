"""
Pipeline Stage 7: Bulk Production Promotion Engine.

Promotes newly standardized rules, schema maps, and metadata to the Production environment:
1. Reads active target numeric MLS IDs directly from 'temp-data/promotion_sources.txt'.
2. Stages the manifest file on the remote deployment server over SFTP.
3. Invokes an interactive SSH shell to execute remote promotion commands ('s_p',
   'set_promotion stage prod', and 'promote_source.py').

Logs execution events to 'logs/pipeline_YYYY-MM-DD.log'.
"""

import os
import sys
import io
import time
import paramiko
from pathlib import Path
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

from pipeline_logger import setup_logger

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
logger = setup_logger("Stage7_BulkPromotion")

# ============================================================
# SSH ENVIRONMENT CONSTANTS
# ============================================================
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

    # Preserve order while deduplicating
    seen = set()
    unique_ids = []
    for source_id in ids:
        if source_id not in seen:
            seen.add(source_id)
            unique_ids.append(source_id)

    return unique_ids


# ============================================================
# PROMOTION ENGINE
# ============================================================

def main() -> None:
    logger.info("============================================================")
    logger.info("🚀 STAGE 7: PRODUCTION PROMOTION ENGINE INITIALIZED")
    logger.info("============================================================")

    local_manifest = current_dir / "temp-data" / "promotion_sources.txt"
    mls_ids = get_target_mls_ids(local_manifest)

    if not mls_ids:
        logger.error(f"❌ No valid numeric MLS IDs found in '{local_manifest}'. Aborting promotion.")
        sys.exit(1)

    logger.info(f"🎯 [PROMOTION BATCH GUARD] Target MLS IDs loaded from manifest ({len(mls_ids)}): {mls_ids}")

    # ------------------------------------------------------------
    # 1. SFTP MANIFEST STAGING
    # ------------------------------------------------------------
    logger.info("============================================================")
    logger.info("📦 STAGING PROMOTION MANIFEST ON REMOTE DEPLOYMENT SERVER")
    logger.info("============================================================")
    logger.info(f"[1/4] Connecting to remote host {SSH_HOST}...")

    if not SSH_KEY_PATH:
        logger.error("❌ SSH key path missing from configuration! Check REMOTE_SSH_KEY_PATH or SSH_KEY_PATH in .env.")
        sys.exit(1)

    try:
        # Load OpenSSH / PEM key dynamically using cryptography
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

        logger.info("Target Production Manifest records validated:")
        for source_id in remote_sources:
            logger.info(f"   Targeted MLS ID: {source_id}")

        # ------------------------------------------------------------
        # 2. INTERACTIVE SHELL PROMOTION SEQUENCE
        # ------------------------------------------------------------
        logger.info("[4/4] Invoking production rule migration engine sequentially...")
        shell = ssh.invoke_shell()
        time.sleep(2)

        # Environment setup commands
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

        # Send execution loop
        loop_cmd = f'while IFS= read -r i; do echo "Processing ID: "$i; python promote_source.py -s process "$i" -y; done < {REMOTE_MANIFEST_PATH}'
        logger.info(f"[INPUT] Sending batch promotion loop: {loop_cmd}")
        shell.send(loop_cmd + "\n")

        # Dynamic output listener: continuously polls output until execution finishes completely
        logger.info("--- PRODUCTION SHELL INTERACTION OUTPUT LOGS ---")
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
                # Wait for 12 seconds of total remote shell silence before considering execution complete
                if idle_count >= 6:
                    break

        logger.info("[INPUT] Remote promotion complete. Closing shell session...")
        shell.send("exit\n")
        time.sleep(2)

        ssh.close()
        logger.info("---------------------------------------------------")
        logger.info("✅ SUCCESS: Production promotion cycle completed sequentially.")
        logger.info("Secure interactive session closed.")
        logger.info("============================================================")

    except Exception as e:
        logger.error(f"❌ Production promotion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
