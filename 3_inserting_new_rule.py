"""
Pipeline Stage 3: Microservice Rule Registration Engine.

Parses 'temp-data/migration_blueprint.csv' and registers newly standardized PascalCase
rules via the MLS Admin Microservice (`POST /rules`). This automatically handles Snowflake ID
generation, populates language/group associations, and immediately invalidates UI caches.

Supports target batch testing mode via 'temp-data/test_rules.txt' (via rules_utils.py)
or single-rule testing mode via the `TEST_RULE_NAME` env variable.
"""

import os
import csv
import io
import warnings
from pathlib import Path
import psycopg2
import paramiko
import requests
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from cryptography.utils import CryptographyDeprecationWarning
from cryptography.hazmat.primitives import serialization

from rules_utils import load_target_test_rules

warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
warnings.filterwarnings("ignore", message=".*TripleDES.*")

# Load environment variables
load_dotenv()


def register_rules_via_microservice():
    project_dir = Path(__file__).resolve().parent
    temp_data_dir = project_dir / "temp-data"
    blueprint_path = temp_data_dir / "migration_blueprint.csv"

    if not blueprint_path.exists():
        print(f"❌ [ERROR] Migration blueprint missing at {blueprint_path}")
        print("   Please execute Stage 2 standardization first.")
        return

    api_key = os.getenv("MLS_ADMIN_API_KEY", "").strip().strip("'\"")
    if not api_key:
        print("❌ [ERROR] MLS_ADMIN_API_KEY missing from environment configuration.")
        return

    stage_url_raw = os.getenv(
        "MLS_ADMIN_STAGE_URL",
        "https://stage-ext-ms.data.kw.com/v1/mls-admin"
    )
    stage_url = stage_url_raw.strip().strip("'\"").rstrip("/")
    rules_endpoint = f"{stage_url}/rules"

    # Check for target batch test file or single rule env var
    target_test_rules = load_target_test_rules(temp_data_dir)
    env_test_rule = os.getenv("TEST_RULE_NAME", "").strip().strip("'\"") or None

    if env_test_rule:
        target_test_rules.add(env_test_rule)

    # Parse blueprint matrix
    migrations_to_execute = []
    with open(blueprint_path, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # Skip Header Row
        for row in reader:
            if len(row) >= 2:
                old_name, target_new_name = row[0].strip(), row[1].strip()
                clean_new_name = target_new_name[:-3] if target_new_name.endswith(".py") else target_new_name

                # Filter by test_rules.txt or TEST_RULE_NAME if present
                if target_test_rules and old_name not in target_test_rules:
                    continue

                if old_name != clean_new_name:
                    migrations_to_execute.append((old_name, clean_new_name))

    if not migrations_to_execute:
        print("ℹ️ [INFO] No pending migration entries found to insert.")
        return

    if target_test_rules:
        print(f"🧪 [BATCH TEST MODE] Targeting {len(migrations_to_execute)} rule(s)...")
    else:
        print(f"🚀 [FULL PRODUCTION MODE] Registering {len(migrations_to_execute)} rules via microservice...")

    # Fetch source content & metadata over SSH DB connection
    ssh_host = os.getenv("SSH_HOST", "").strip().strip("'\"")
    ssh_user = os.getenv("SSH_USER", "").strip().strip("'\"")
    ssh_key_path = os.getenv("SSH_KEY_PATH", "").strip().strip("'\"")
    ssh_passphrase = os.getenv("SSH_KEY_PASSPHRASE", "").strip().strip("'\"")

    db_host = os.getenv("DB_HOST", "").strip().strip("'\"")
    db_name = os.getenv("DB_NAME", "").strip().strip("'\"")
    db_user = os.getenv("DB_USER", "").strip().strip("'\"")
    db_pass = os.getenv("DB_PASSWORD", "").strip().strip("'\"")

    registered_count = 0
    skipped_count = 0
    created_by_user = os.getenv("CREATED_BY_USER", db_user or "shrisha_vanga").strip().strip("'\"")

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
                    for old_name, new_name in migrations_to_execute:
                        # 1. Query source rule content and description
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
                            print(f"⚠️ [WARNING] Source rule '{old_name}' not found in DB.")
                            continue

                        content, description, lang_name, group_name = legacy_record

                        # Pass [group_name] if present, otherwise set to None (JSON null)
                        groups_payload = [group_name] if group_name else None

                        # 2. Build payload for POST /rules endpoint
                        payload = {
                            "name": new_name,
                            "content": content or "",
                            "created_by": created_by_user,
                            "language": lang_name or "Python",
                            "description": description or "",  # Defaults to empty string
                            "params": {},                       # Explicitly sets empty JSON object {}
                            "groups": groups_payload,
                        }

                        # 3. Post to microservice endpoint
                        headers = {
                            "Content-Type": "application/json",
                            "api-key": api_key,
                        }

                        res = requests.post(rules_endpoint, json=payload, headers=headers)

                        if res.status_code in [200, 201]:
                            res_data = res.json()
                            print(f"  ✅ Created '{new_name}' | ID: {res_data.get('id')}")
                            registered_count += 1
                        elif res.status_code == 409 or "already exists" in res.text.lower():
                            print(f"  ℹ️ Skipped '{new_name}' (Already exists)")
                            skipped_count += 1
                        else:
                            print(f"  ❌ Failed to create '{new_name}' [{res.status_code}]: {res.text}")

                    print("\n" + "=" * 60)
                    print("🚀 STAGE 3 COMPLETE: MICROSERVICE RULE REGISTRATION FINISHED")
                    print("=" * 60)
                    print(f" Target Blueprint Entries Evaluated : {len(migrations_to_execute)}")
                    print(f" Successfully Registered Rules      : {registered_count}")
                    print(f" Skipped / Already Existing Rules    : {skipped_count}")
                    print("=" * 60 + "\n")

    except Exception as err:
        print(f"❌ [ERROR] Pipeline Stage 3 execution failure: {err}")


if __name__ == "__main__":
    register_rules_via_microservice()
