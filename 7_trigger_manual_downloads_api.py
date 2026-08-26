"""
Stage 7: Trigger Production Manual Downloads (API Variant)

Dynamically reads MLS IDs, MLS ID strings, content types, and sub-types
from batch_mls_targets_api.csv and triggers 3-day data syncs via Production API.
Saves trigger details to 'temp-data/triggered_downloads_log_api.csv' for Stage 8.
"""

import os
import sys
import csv
import requests
from pathlib import Path
from dotenv import load_dotenv

from pipeline_logger import setup_logger

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
logger = setup_logger("Stage7_ManualDownloads")

# ============================================================
# API ENVIRONMENT CONSTANTS
# ============================================================
MLS_ADMIN_PROD_URL = os.getenv(
    "MLS_ADMIN_PROD_URL",
    "https://prod-ext-ms.data.kw.com/v1/mls-admin"
).strip().strip("'\"").rstrip("/")

PROD_BASE_URL = os.getenv(
    "PROD_MS_URL",
    MLS_ADMIN_PROD_URL.split("/v1/mls-admin")[0]
).strip().strip("'\"").rstrip("/")

API_KEY = os.getenv("MLS_ADMIN_API_KEY") or os.getenv("API_KEY", "")

HEADERS = {
    "accept": "application/json",
    "api-key": API_KEY,
}

DEFAULT_RELATIVE_PERIOD = "3"
DEFAULT_RELATIVE_PERIOD_UNIT = "day"


def load_targets_from_batch_csv() -> list[dict]:
    """Reads temp-data/batch_mls_targets_api.csv and extracts deduplicated combinations."""
    batch_csv_path = current_dir / "temp-data" / "batch_mls_targets_api.csv"
    if not batch_csv_path.exists():
        logger.error(f"❌ Batch targets file not found at '{batch_csv_path}'. Aborting.")
        return []

    unique_targets = {}
    with open(batch_csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mls_id = int(row["mls_id"].strip())
            mls_id_str = row.get("mls_id_str", f"ID_{mls_id}").strip()
            content_type = row["content_type"].strip()
            content_sub_type = row["content_sub_type"].strip()

            key = (mls_id, content_type, content_sub_type)
            if key not in unique_targets:
                unique_targets[key] = {
                    "mls_id": mls_id,
                    "mls_id_str": mls_id_str,
                    "content_type": content_type,
                    "content_sub_type": content_sub_type,
                }

    return list(unique_targets.values())


def trigger_download(mls_id: int, mls_id_str: str, content_type: str, content_sub_type: str) -> str | None:
    """Triggers download for a single target and returns the batch_id string if successful."""
    url = f"{PROD_BASE_URL}/v1/listing-mflow/reprocess/download/mls/{mls_id}"

    params = {
        "dl_type": "range",
        "relative_period": DEFAULT_RELATIVE_PERIOD,
        "relative_period_unit": DEFAULT_RELATIVE_PERIOD_UNIT,
        "listing_id_name": "ListingId",
        "use_in_operator": "true",
        "content_type": content_type,
        "content_sub_type": content_sub_type,
        "query_validation": "false",
        "force": "true",
    }

    logger.info(
        f"🚀 Triggering 3-Day Download | MLS: {mls_id} ({mls_id_str}) | [{content_type} / {content_sub_type}]"
    )

    try:
        response = requests.post(url, headers=HEADERS, params=params, data="", timeout=30)

        if response.status_code in (200, 201, 202):
            data = response.json() if response.text else {}
            batch_id = str(data.get("batch_id", "")).strip()
            logger.info(f"   ✅ SUCCESS [{response.status_code}]: Triggered Batch ID = {batch_id}")
            return batch_id if batch_id else None

        elif response.status_code == 403:
            err = response.json().get("message", response.text) if response.text else "Forbidden"
            logger.error(f"   ❌ AUTH ERROR [403]: Invalid API key. ({err})")
            return None

        elif response.status_code in (406, 422):
            err = response.json().get("message", response.text) if response.text else "Validation error"
            logger.error(
                f"   ❌ INPUT ERROR [{response.status_code}]: Invalid request for MLS {mls_id} "
                f"({content_type}/{content_sub_type}). Message: {err}"
            )
            return None

        else:
            logger.error(f"   ❌ UNEXPECTED ERROR [{response.status_code}]: {response.text}")
            return None

    except requests.RequestException as e:
        logger.error(f"   ❌ Request Exception: {e}", exc_info=True)
        return None


def save_triggered_downloads_log(batch_records: list[dict]) -> Path:
    """Saves batch IDs and target metadata to temp-data/triggered_downloads_log_api.csv."""
    temp_dir = current_dir / "temp-data"
    temp_dir.mkdir(parents=True, exist_ok=True)
    csv_file = temp_dir / "triggered_downloads_log_api.csv"

    fieldnames = ["mls_id", "mls_id_str", "content_type", "content_sub_type", "batch_id"]
    with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(batch_records)

    logger.info(f"📄 Saved {len(batch_records)} batch trigger record(s) to '{csv_file}'.")
    return csv_file


def main() -> None:
    logger.info("============================================================")
    logger.info("🚀 STAGE 7: TRIGGERING MANUAL PRODUCTION DOWNLOADS")
    logger.info("============================================================")

    if not API_KEY:
        logger.error("❌ API_KEY / MLS_ADMIN_API_KEY missing from .env file. Aborting.")
        sys.exit(1)

    targets = load_targets_from_batch_csv()
    if not targets:
        logger.error("❌ No targets resolved from temp-data/batch_mls_targets_api.csv. Aborting.")
        sys.exit(1)

    logger.info(f"🎯 Target download payload(s) loaded from batch CSV ({len(targets)}):")
    for t in targets:
        logger.info(
            f"   - MLS: {t['mls_id']} ({t['mls_id_str']}) | content_type: {t['content_type']} | content_sub_type: {t['content_sub_type']}"
        )

    triggered_records = []
    total_triggered = 0
    total_failed = 0

    logger.info("------------------------------------------------------------")
    for t in targets:
        batch_id = trigger_download(
            t["mls_id"],
            t["mls_id_str"],
            t["content_type"],
            t["content_sub_type"]
        )
        if batch_id:
            total_triggered += 1
            triggered_records.append({
                "mls_id": t["mls_id"],
                "mls_id_str": t["mls_id_str"],
                "content_type": t["content_type"],
                "content_sub_type": t["content_sub_type"],
                "batch_id": batch_id,
            })
        else:
            total_failed += 1

    save_triggered_downloads_log(triggered_records)

    logger.info("============================================================")
    logger.info(f"✅ STAGE 7 COMPLETE | Successful Triggers: {total_triggered} | Failures: {total_failed}")
    logger.info("============================================================")


if __name__ == "__main__":
    main()
