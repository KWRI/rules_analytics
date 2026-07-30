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

Logs execution events to 'logs/pipeline_YYYY-MM-DD.log'.
"""

import os
import sys
import requests
import pandas as pd
from dotenv import load_dotenv
from pipeline_logger import setup_logger

load_dotenv()
logger = setup_logger("Stage7_SoftDelete")

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
        logger.error(f"Blueprint file not found at '{blueprint_path}'")
        sys.exit(1)

    df = pd.read_csv(blueprint_path) if blueprint_path.endswith(".csv") else pd.read_excel(blueprint_path)

    column_name = "old_name"
    if column_name not in df.columns:
        logger.error(f"Column '{column_name}' not found in '{blueprint_path}'.")
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
            logger.info(f"Soft-deleted rule: '{rule_name}' (ID: {rule_id}, by {deleted_by})")
            return True
        else:
            logger.error(f"Failed for '{rule_name}' [HTTP {response.status_code}]: {response.text}")
            return False

    except requests.RequestException as e:
        logger.error(f"Request exception for '{rule_name}': {e}", exc_info=True)
        return False


def main() -> None:
    BLUEPRINT_FILE = "temp-data/migration_blueprint.csv"

    logger.info("--- 🗑️  MLS Admin Microservice Soft Delete Tool ---")

    rule_names = load_old_rule_names_from_blueprint(BLUEPRINT_FILE)
    logger.info(f"Found {len(rule_names)} legacy rule(s) targeted for soft-deletion.")
    logger.info(f"Deletion attribution: deleted_by = '{DELETED_BY_USER}'")

    logger.info("Legacy Rules targeted for deletion:")
    for idx, name in enumerate(rule_names, 1):
        logger.info(f"   {idx}. {name}")

    confirm = input(f"\nAre you sure you want to delete these {len(rule_names)} rule(s) via API? (y/N): ")
    if confirm.lower() != "y":
        logger.info("Operation cancelled by user.")
        return

    logger.info("Starting API deletion sequence...")
    success_count = 0
    for name in rule_names:
        if delete_rule_via_api(name, DELETED_BY_USER):
            success_count += 1

    logger.info(f"Finished! Successfully soft-deleted {success_count}/{len(rule_names)} rule(s) via API.")


if __name__ == "__main__":
    main()
