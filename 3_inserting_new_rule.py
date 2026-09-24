"""
Pipeline Stage 3: Microservice Rule Registration Engine.

Strictly operates in Target Batch Mode when batch targets exist in temp-data/.
Guarantees Full Production Mode is NEVER entered during batch execution.
"""

import os
import csv
import io
import time
import sys
import signal
import warnings
from pathlib import Path
import psycopg2
import paramiko
import requests
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from cryptography.utils import CryptographyDeprecationWarning
from cryptography.hazmat.primitives import serialization

# --- OS-LEVEL INSTANT TERMINAL EXIT ---
def force_terminal_exit(sig, frame):
    print("\n⛔ [TERMINAL ABORT] Killing process tree immediately...")
    os._exit(1)

signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

from rules_utils import load_target_rules
from pipeline_logger import setup_logger

warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
warnings.filterwarnings("ignore", message=".*TripleDES.*")

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
logger = setup_logger("Stage3_RegisterRules")


def register_rules_via_microservice():
    temp_data_dir = current_dir / "temp-data"
    blueprint_path = temp_data_dir / "migration_blueprint.csv"

    # Detect if Target Batch Mode is active
    target_mode_active = (temp_data_dir / "batch_mls_targets.csv").exists()

    target_batch_rules = load_target_rules(temp_data_dir)
    env_target_rule = os.getenv("TARGET_RULE_NAME", "").strip().strip("'\"") or None

    if env_target_rule:
        target_batch_rules.add(env_target_rule)

    # STRICT GUARD: If Target Mode is active, NEVER run Full Production
    if target_mode_active:
        if not target_batch_rules:
            logger.info("🎯 [TARGET BATCH MODE] Target batch active, but 0 new rules require microservice registration.")
            logger.info("ℹ️ Skipping Stage 3 registration.")
            sys.exit(0)
    else:
        logger.info("🚀 [FULL PRODUCTION MODE] Executing manual full database rule registration...")

    if not blueprint_path.exists():
        logger.error(f"Migration blueprint missing at {blueprint_path}")
        logger.error("Please execute Stage 2 standardization first.")
        return

    api_key = os.getenv("MLS_ADMIN_API_KEY", "").strip().strip("'\"")
    if not api_key:
        logger.error("MLS_ADMIN_API_KEY missing from environment configuration.")
        return

    stage_url_raw = os.getenv(
        "MLS_ADMIN_STAGE_URL",
        "https://stage-ext-ms.data.kw.com/v1/mls-admin"
    )
    stage_url = stage_url_raw.strip().strip("'\"").rstrip("/")
    rules_endpoint = f"{stage_url}/rules"

    migrations_to_execute = []
    with open(blueprint_path, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) >= 2:
                old_name, target_new_name = row[0].strip(), row[1].strip()
                clean_new_name = target_new_name[:-3] if target_new_name.endswith(".py") else target_new_name

                if target_batch_rules and old_name not in target_batch_rules:
                    continue

                if old_name != clean_new_name:
                    migrations_to_execute.append((old_name, clean_new_name))

    if not migrations_to_execute:
        logger.info("ℹ️ No pending migration entries found to insert.")
        return

    if target_batch_rules:
        logger.info(f"🎯 [TARGET BATCH MODE] Targeting {len(migrations_to_execute)} rule(s)...")

    ssh_host = (os.getenv("STAGE_SSH_HOST") or os.getenv("REMOTE_SSH_HOST") or os.getenv("SSH_HOST", "")).strip("'\"")
    ssh_user = (os.getenv("SSH_USER") or os.getenv("REMOTE_SSH_USER", "")).strip("'\"")
    ssh_key_path = (os.getenv("SSH_KEY_PATH") or os.getenv("REMOTE_SSH_KEY_PATH", "")).strip("'\"")
    ssh_passphrase = (os.getenv("SSH_KEY_PASSPHRASE") or os.getenv("REMOTE_SSH_KEY_PASSPHRASE", "")).strip("'\"")

    db_host = (os.getenv("STAGE_DB_HOST") or os.getenv("DB_HOST", "")).strip("'\"")
    db_name = (os.getenv("DB_NAME", "mls_admin")).strip("'\"")
    db_user = (os.getenv("DB_USER", "")).strip("'\"")
    db_pass = (os.getenv("DB_PASSWORD", "")).strip("'\"")

    registered_count = 0
    skipped_count = 0
    created_by_user = (
        os.getenv("CREATED_BY_USER") or
        os.getenv("UPDATED_BY_USER") or
        db_user
    ).strip().strip("'\"")

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

        logger.info(f"Establishing SSH tunnel to connect to database '{db_name}'...")
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
                    for old_name, new_name in migrations_to_execute:
                        select_query = """
                            SELECT pr.content, pr.description, rl.name as language_name, rg.name as group_name
                            FROM public.process_rule pr
                            LEFT JOIN public.rule_language rl ON rl.id = pr.rule_language_id
                            LEFT JOIN public.rule_group_association rga ON rga.process_rule_id = pr.id
                            LEFT JOIN public.rule_group rg ON rg.id = rga.rule_group_id
                            WHERE pr.name = %s AND pr.deleted_at IS NULL;
                        """
                        cur.execute(select_query, (old_name,))
                        legacy_record = cur.fetchone()

                        if not legacy_record:
                            logger.warning(f"Source rule '{old_name}' not found in DB.")
                            continue

                        content, description, lang_name, group_name = legacy_record
                        groups_payload = [group_name] if group_name else None

                        payload = {
                            "name": new_name,
                            "content": content or "",
                            "created_by": created_by_user,
                            "language": lang_name or "Python",
                            "description": description or "",
                            "params": {},
                            "groups": groups_payload,
                        }

                        headers = {
                            "Content-Type": "application/json",
                            "api-key": api_key,
                        }

                        for attempt in range(1, 4):
                            try:
                                res = requests.post(rules_endpoint, json=payload, headers=headers, timeout=30)

                                if res.status_code in [200, 201]:
                                    res_data = res.json()
                                    logger.info(f"Created '{new_name}' | ID: {res_data.get('id')}")
                                    registered_count += 1
                                    break
                                elif res.status_code == 409 or "already exists" in res.text.lower():
                                    logger.info(f"Skipped '{new_name}' (Already exists)")
                                    skipped_count += 1
                                    break
                                elif res.status_code in [502, 503, 504] and attempt < 3:
                                    time.sleep(attempt * 2)
                                    continue
                                else:
                                    logger.error(f"Failed to create '{new_name}' [{res.status_code}]: {res.text}")
                                    break
                            except requests.RequestException as req_err:
                                if attempt < 3:
                                    time.sleep(attempt * 2)
                                    continue
                                logger.error(f"HTTP request exception for '{new_name}': {req_err}")

                    logger.info("============================================================")
                    logger.info("🚀 STAGE 3 COMPLETE: MICROSERVICE RULE REGISTRATION FINISHED")
                    logger.info("============================================================")
                    logger.info(f"Target Blueprint Entries Evaluated : {len(migrations_to_execute)}")
                    logger.info(f"Successfully Registered Rules      : {registered_count}")
                    logger.info(f"Skipped / Already Existing Rules    : {skipped_count}")
                    logger.info("============================================================")

    except Exception as err:
        logger.error(f"Pipeline Stage 3 execution failure: {err}", exc_info=True)


if __name__ == "__main__":
    register_rules_via_microservice()
