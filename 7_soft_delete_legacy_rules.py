"""
Soft Delete Legacy Rules Script.

This module processes a migration blueprint file (CSV or Excel) containing old/legacy
rule definitions and executes soft-deletion requests via the MLS Admin Microservice API.

API Specification:
    Method: DELETE
    Endpoint: https://stage-ext-ms.data.kw.com/v1/mls-admin/rules
    Headers:
        - accept: application/json
        - api-key: <API_KEY>
    Query Params:
        - name (str, required): Rule name to soft delete
        - deleted_by (str, optional): User performing the deletion (uses DB_USER from .env)

"""

import os
import sys
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Read API credentials and DB_USER from environment variables
API_BASE_URL = os.getenv("MLS_ADMIN_API_URL", "https://stage-ext-ms.data.kw.com")
API_KEY = os.getenv("MLS_ADMIN_API_KEY", "")
DELETED_BY_USER = os.getenv("DB_USER", "mls_admin")

HEADERS = {
    "accept": "application/json",
    "api-key": API_KEY,
}


def load_old_rule_names_from_blueprint(blueprint_path: str) -> list[str]:
    """Parses the migration blueprint file and extracts unique legacy rule names."""
    if not os.path.exists(blueprint_path):
        print(f"❌ Error: Blueprint file not found at '{blueprint_path}'")
        sys.exit(1)

    df = pd.read_csv(blueprint_path) if blueprint_path.endswith(".csv") else pd.read_excel(blueprint_path)

    column_name = "old_name"
    if column_name not in df.columns:
        print(f"❌ Error: Column '{column_name}' not found in '{blueprint_path}'.")
        sys.exit(1)

    return df[column_name].dropna().astype(str).unique().tolist()


def delete_rule_via_api(rule_name: str, deleted_by: str) -> bool:
    """Sends a DELETE request matching the exact Swagger curl call."""
    # Matched path: /v1/mls-admin/rules
    url = f"{API_BASE_URL.rstrip('/')}/v1/mls-admin/rules"

    params = {
        "name": rule_name,
        "deleted_by": deleted_by
    }

    try:
        response = requests.delete(url, headers=HEADERS, params=params, timeout=10)

        if response.status_code in (200, 204):
            data = response.json()
            rule_id = data.get("id", "N/A")
            print(f"  ✅ Soft-deleted rule: '{rule_name}' (ID: {rule_id}, by {deleted_by})")
            return True
        else:
            print(f"  ❌ Failed for '{rule_name}' [HTTP {response.status_code}]: {response.text}")
            return False

    except requests.RequestException as e:
        print(f"  ❌ Request exception for '{rule_name}': {e}")
        return False


def main() -> None:
    BLUEPRINT_FILE = "temp-data/migration_blueprint.csv"

    print("--- 🗑️  MLS Admin Microservice Soft Delete Tool ---")

    rule_names = load_old_rule_names_from_blueprint(BLUEPRINT_FILE)
    print(f"📄 Found {len(rule_names)} legacy rule(s) targeted for soft-deletion.")
    print(f"👤 Deletion attribution: deleted_by = '{DELETED_BY_USER}'\n")

    print("📋 Legacy Rules to be deleted:")
    for idx, name in enumerate(rule_names, 1):
        print(f"   {idx}. {name}")
    print()

    confirm = input(f"Are you sure you want to delete these {len(rule_names)} rule(s) via API? (y/N): ")
    if confirm.lower() != "y":
        print("Operation cancelled.")
        return

    print("\n🚀 Starting deletion process...")
    success_count = 0
    for name in rule_names:
        if delete_rule_via_api(name, DELETED_BY_USER):
            success_count += 1

    print(f"\n✅ Finished! Successfully soft-deleted {success_count}/{len(rule_names)} rules via API.")


if __name__ == "__main__":
    main()
