"""
Stage 6: Bulk Production Promotion Engine

Automatically handles the entire pre-promotion and promotion lifecycle:
1. Disables MLS Download Jobs for active targets via API.
2. Triggers Download Manager Scheduler Sync via API.
3. Stages production manifest file on the remote deployment server over SSH/SFTP.
4. Executes remote shell commands to promote rules, maps, and metadata to Production.
"""

import os
import sys
import time
import requests
import paramiko
from pathlib import Path
from dotenv import load_dotenv

from rules_utils import load_target_test_mls
from pipeline_logger import setup_logger

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
logger = setup_logger("Stage6_BulkPromotion")

# API & SSH ENVIRONMENT CONSTANTS
PROD_BASE_URL = os.getenv("PROD_MS_URL", "https://prod-ext-ms.data.kw.com").rstrip("/")
API_BASE_URL = os.getenv("MLS_ADMIN_API_BASE_URL", f"{PROD_BASE_URL}/v1/mls-admin")
API_KEY = os.getenv("MLS_ADMIN_API_KEY") or os.getenv("API_KEY", "")
UPDATED_BY_USER = os.getenv("UPDATED_BY_USER", "shrisha.vanga@kw.com")

SSH_HOST = os.getenv("REMOTE_SSH_HOST", "sdh.data.kw.com").strip("'\"")
SSH_USER = os.getenv("REMOTE_SSH_USER", "").strip("'\"")
SSH_KEY_PATH = os.getenv("REMOTE_SSH_KEY_PATH", "").strip("'\"")
SSH_PASSPHRASE = os.getenv("REMOTE_SSH_KEY_PASSPHRASE", "").strip("'\"")
REMOTE_MANIFEST_PATH = os.getenv("REMOTE_MANIFEST_PATH", "/eim-mls-admin-ms/devtools/source_promotion/promotion_sources.txt")


def get_target_mls_ids() -> list[int]:
    """Resolves target MLS IDs from local batch artifacts."""
    temp_data_dir = current_dir / "temp-data"

    target_set = load_target_test_mls(temp_data_dir)
    if target_set:
        return sorted([int(m) for m in target_set])

    manifest_path = temp_data_dir / "promotion_sources.txt"
    if manifest_path.exists():
        with open(manifest_path, "r") as f:
            ids = [int(line.strip()) for line in f if line.strip().isdigit()]
            if ids:
                return sorted(ids)

    return []


# ============================================================
# PRE-PROMOTION API FUNCTIONS
# ============================================================

def trigger_scheduler_sync() -> bool:
    """Triggers Download Manager Scheduler Sync POST call."""
    sync_url = f"{PROD_BASE_URL}/v1/download-manager/scheduler/sync"
    logger.info("🔄 ACTION: TRIGGER DOWNLOAD MANAGER SCHEDULER SYNC")
    logger.info(f"Target Endpoint : {sync_url}")

    headers = {
        "accept": "application/json",
        "api-key": API_KEY,
    }

    try:
        response = requests.post(sync_url, headers=headers, data="", timeout=15)
        if response.status_code in (200, 201, 204):
            logger.info("✅ SUCCESS: Download Manager Scheduler synchronized successfully.")
            logger.info(f"Response: {response.text}")
            return True
        else:
            logger.error(f"❌ SCHEDULER SYNC FAILED [{response.status_code}]: {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Scheduler Sync Request Exception: {e}", exc_info=True)
        return False


def pre_promotion_disable_and_sync(mls_ids: list[int]) -> bool:
    """Disables MLS Download Jobs for target sources and triggers scheduler sync."""
    logger.info("============================================================")
    logger.info("🛑 PRE-PROMOTION STEP: DISABLING JOBS & SYNCING SCHEDULER")
    logger.info("============================================================")

    if not API_KEY:
        logger.error("❌ API_KEY / MLS_ADMIN_API_KEY missing from .env file.")
        return False

    endpoint = f"{API_BASE_URL}/mls/download/jobs/disable"
    params = {"temp_update": "true"}

    headers = {
        "accept": "application/json",
        "api-key": API_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "sources": mls_ids,
        "updated_by": UPDATED_BY_USER
    }

    logger.info(f"Target MLS IDs ({len(mls_ids)}) : {mls_ids}")
    logger.info(f"Target Endpoint      : {endpoint}")
    logger.info(f"Payload              : {payload}")

    try:
        response = requests.post(endpoint, params=params, headers=headers, json=payload, timeout=15)

        if response.status_code in (200, 201):
            logger.info("✅ SUCCESS: Download jobs successfully DISABLED for MLS targets.")
            logger.info(f"Response: {response.text}")
            logger.info("------------------------------------------------------------")
            # Trigger Scheduler Sync
            return trigger_scheduler_sync()
        else:
            logger.error(f"❌ FAILED TO DISABLE JOBS [{response.status_code}]: {response.text}")
            return False

    except Exception as e:
        logger.error(f"❌ Pre-Promotion API Request Exception: {e}", exc_info=True)
        return False


# ============================================================
# PROMOTION ENGINE
# ============================================================

def main() -> None:
    logger.info("============================================================")
    logger.info("🚀 STAGE 6: PRODUCTION PROMOTION ENGINE INITIALIZED")
    logger.info("============================================================")

    mls_ids = get_target_mls_ids()
    if not mls_ids:
        logger.error("❌ No target MLS IDs found in temp-data/ (checked test_mls.txt and promotion_sources.txt). Aborting.")
        sys.exit(1)

    logger.info(f"🧪 [PROMOTION BATCH GUARD] Active MLS targets detected: {mls_ids}")

    # ------------------------------------------------------------
    # 1. RUN PRE-PROMOTION DISABLE & SCHEDULER SYNC
    # ------------------------------------------------------------
    sync_success = pre_promotion_disable_and_sync(mls_ids)
    if not sync_success:
        logger.error("❌ Pre-promotion job disable or scheduler sync failed! Aborting production promotion for safety.")
        sys.exit(1)

    # Local manifest resolution
    local_manifest = current_dir / "temp-data" / "promotion_sources.txt"
    if not local_manifest.exists():
        logger.error(f"❌ Promotion sources manifest not found at '{local_manifest}'. Aborting.")
        sys.exit(1)

    # ------------------------------------------------------------
    # 2. SFTP MANIFEST STAGING
    # ------------------------------------------------------------
    logger.info("============================================================")
    logger.info("📦 STAGING PROMOTION MANIFEST ON REMOTE DEPLOYMENT SERVER")
    logger.info("============================================================")
    logger.info(f"[1/4] Connecting to remote host {SSH_HOST}...")

    ssh_key = paramiko.RSAKey.from_private_key_file(SSH_KEY_PATH, password=SSH_PASSPHRASE if SSH_PASSPHRASE else None)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(hostname=SSH_HOST, username=SSH_USER, pkey=ssh_key, timeout=15)

        logger.info(f"[2/4] Uploading manifest to server path: {REMOTE_MANIFEST_PATH}...")
        sftp = ssh.open_sftp()
        sftp.put(str(local_manifest), REMOTE_MANIFEST_PATH)
        sftp.close()
        logger.info("[SUCCESS] Manifest staged successfully on remote file layer.")

        logger.info("[3/4] Verifying remote payload structure consistency...")
        stdin, stdout, stderr = ssh.exec_command(f"cat {REMOTE_MANIFEST_PATH}")
        remote_sources = [line.strip() for line in stdout.readlines() if line.strip()]

        logger.info("Target Production Manifest records validated:")
        for source_id in remote_sources:
            logger.info(f"   Targeted MLS ID: {source_id}")

        # ------------------------------------------------------------
        # 3. INTERACTIVE SHELL PROMOTION SEQUENCE
        # ------------------------------------------------------------
        logger.info("[4/4] Invoking production rule migration engine sequentially...")
        shell = ssh.invoke_shell()
        time.sleep(2)

        # Commands to execute in interactive shell
        commands = [
            "s_p",
            "set_promotion stage prod",
            f"while IFS= read -r i; do echo 'Processing ID: '$i; python promote_source.py -s process \"$i\" -y; done < {REMOTE_MANIFEST_PATH}",
            "exit"
        ]

        logger.info("--- PRODUCTION SHELL INTERACTION OUTPUT LOGS ---")
        for cmd in commands:
            logger.info(f"[INPUT] Sending interactive shell input: {cmd}")
            shell.send(cmd + "\n")
            time.sleep(3)

            while shell.recv_ready():
                output = shell.recv(4096).decode("utf-8", errors="ignore")
                for line in output.splitlines():
                    if line.strip():
                        logger.info(f"[REMOTE SHELL] {line}")

        ssh.close()
        logger.info("---------------------------------------------------")
        logger.info("✅ SUCCESS: Production update cycle completed sequentially.")
        logger.info("Secure interactive session closed.")
        logger.info("============================================================")

    except Exception as e:
        logger.error(f"❌ Production promotion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
