"""
Pipeline Stage 9: API Verification & Job Re-Enablement Engine.

1. Reads API batch IDs from 'temp-data/triggered_downloads_log_api.csv'.
2. Queries BigQuery 'stream-listing-prod.mls_download.{content_type}' by batch_id.
3. Checks 'kw_logger.data_team_logs' for ERROR severities matching target batch IDs.
4. Re-enables verified clean API download jobs via HTTP POST and triggers Download Manager Scheduler Sync.

Logs execution events to 'logs/pipeline_YYYY-MM-DD.log'.
"""

import os
import sys
import csv
import requests
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
from google.auth.exceptions import RefreshError

from pipeline_logger import setup_logger

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
logger = setup_logger("Stage9_VerifyAndEnableAPI")

# ==============================================================================
# ENVIRONMENT CONSTANTS
# ==============================================================================
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT", "data-shared-prod-44e4")
DOWNLOAD_PROD_PROJECT = os.getenv("DOWNLOAD_PROD_PROJECT", "stream-listing-prod")

MLS_ADMIN_PROD_URL = os.getenv("MLS_ADMIN_PROD_URL", "").strip().strip("'\"").rstrip("/")
PROD_BASE_URL = MLS_ADMIN_PROD_URL.split("/v1/mls-admin")[0] if "/v1/mls-admin" in MLS_ADMIN_PROD_URL else MLS_ADMIN_PROD_URL

API_BASE_URL = MLS_ADMIN_PROD_URL
MLS_ADMIN_API_KEY = os.getenv("MLS_ADMIN_API_KEY") or os.getenv("API_KEY", "")
UPDATED_BY_USER = os.getenv("UPDATED_BY_USER", "shrisha.vanga@kw.com")


# ==============================================================================
# API VERIFICATION & RE-ENABLEMENT FUNCTIONS
# ==============================================================================

def load_triggered_api_downloads_log() -> list[dict]:
    """Loads API batch trigger records from Stage 8 output log."""
    log_file = current_dir / "temp-data" / "triggered_downloads_log_api.csv"

    if not log_file.exists():
        logger.info("ℹ️ No 'triggered_downloads_log_api.csv' found. Skipping API verification.")
        return []

    trigger_records = []
    with open(log_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("batch_id"):
                trigger_records.append({
                    "mls_id": int(row["mls_id"].strip()),
                    "mls_id_str": row.get("mls_id_str", "").strip(),
                    "content_type": row["content_type"].strip(),
                    "batch_id": row["batch_id"].strip(),
                })

    return trigger_records


def verify_api_batch_records_in_bigquery(records: list[dict]) -> dict[str, int]:
    """Queries stream-listing-prod.mls_download.{content_type} by exact batch_id."""
    try:
        client = bigquery.Client(project=DOWNLOAD_PROD_PROJECT)
    except Exception as e:
        logger.error(f"❌ Failed to initialize BigQuery client: {e}")
        logger.error("👉 Try running: `gcloud auth application-default login --scopes=\"https://www.googleapis.com/auth/cloud-platform\"` ")
        return {}

    logger.info("============================================================")
    logger.info("🔍 CHECKING PRODUCTION DOWNLOAD TABLES BY BATCH ID (API)")
    logger.info("============================================================")

    batch_counts = {}

    for r in records:
        mls_id = r["mls_id"]
        c_type = r["content_type"]
        b_id = r["batch_id"]
        table_name = f"{DOWNLOAD_PROD_PROJECT}.mls_download.{c_type}"

        query = f"""
            SELECT COUNT(*) AS downloaded_count
            FROM `{table_name}`
            WHERE mls_id = {mls_id}
              AND CAST(batch_id AS STRING) = '{b_id}'
        """

        try:
            logger.info(f"📦 Querying `{table_name}` | MLS: {mls_id} | batch_id: {b_id}")
            query_job = client.query(query)
            results = list(query_job.result())
            count = results[0]["downloaded_count"] if results else 0

            batch_counts[b_id] = count
            if count > 0:
                logger.info(f"   ✅ SUCCESS: Found {count} row(s) for Batch ID {b_id} in `{table_name}`.")
            else:
                logger.warning(f"   ⚠️ PENDING/EMPTY: No rows found yet for Batch ID {b_id} in `{table_name}`.")

        except RefreshError:
            logger.error("❌ GCP Authentication Expired! Run: `gcloud auth application-default login --scopes=\"https://www.googleapis.com/auth/cloud-platform\"` ")
            sys.exit(1)
        except Exception as e:
            logger.error(f"❌ Error querying `{table_name}` for Batch ID {b_id}: {e}")
            batch_counts[b_id] = 0

    return batch_counts


def check_api_error_logs(records: list[dict]) -> list[str]:
    """Checks kw_logger.data_team_logs for ERROR severities matching target batch IDs."""
    batch_ids = [r["batch_id"] for r in records if r.get("batch_id")]
    if not batch_ids:
        return []

    try:
        client = bigquery.Client(project=BIGQUERY_PROJECT)
    except Exception:
        return []

    batch_ids_formatted = ", ".join(f"'{b}'" for b in batch_ids)
    regex_pattern = "|".join(batch_ids)

    query = f"""
        SELECT 
            COALESCE(
                CAST(JSON_VALUE(json_payload, '$.batch_id') AS STRING),
                REGEXP_EXTRACT(CAST(text_payload AS STRING), r'({regex_pattern})')
            ) AS batch_id,
            COUNT(*) AS error_count
        FROM `{BIGQUERY_PROJECT}.kw_logger.data_team_logs`
        WHERE processed_at >= DATETIME(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))
          AND severity = 'ERROR'
          AND (
            CAST(JSON_VALUE(json_payload, '$.batch_id') AS STRING) IN ({batch_ids_formatted})
            OR REGEXP_CONTAINS(CAST(text_payload AS STRING), r'({regex_pattern})')
          )
        GROUP BY 1
    """

    failing_batch_ids = []
    try:
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            if row["error_count"] > 0 and row["batch_id"]:
                failing_batch_ids.append(row["batch_id"])
                logger.error(f"   ❌ Batch ID {row['batch_id']}: Detected {row['error_count']} ERROR log(s).")
    except RefreshError:
        logger.error("❌ GCP Authentication Expired! Run: `gcloud auth application-default login --scopes=\"https://www.googleapis.com/auth/cloud-platform\"` ")
        sys.exit(1)
    except Exception as e:
        logger.warning(f"⚠️ Log error check encountered an exception: {e}")

    return failing_batch_ids


def trigger_scheduler_sync() -> bool:
    """Triggers Download Manager Scheduler Sync POST call on Production."""
    if not PROD_BASE_URL:
        return False

    sync_url = f"{PROD_BASE_URL}/v1/download-manager/scheduler/sync"
    logger.info("🔄 ACTION: TRIGGER PRODUCTION DOWNLOAD MANAGER SCHEDULER SYNC")

    headers = {
        "accept": "application/json",
        "api-key": MLS_ADMIN_API_KEY,
    }

    try:
        response = requests.post(sync_url, headers=headers, data="", timeout=15)
        if response.status_code in (200, 201, 204):
            logger.info("✅ SUCCESS: Download Manager Scheduler synchronized successfully.")
            return True
        else:
            logger.error(f"❌ SCHEDULER SYNC FAILED [{response.status_code}]: {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Scheduler Sync Exception: {e}")
        return False


def enable_api_download_jobs(mls_ids: list[int]) -> bool:
    """Enables MLS Download Jobs for clean target sources on Production API."""
    if not API_BASE_URL or not mls_ids:
        return False

    endpoint = f"{API_BASE_URL}/mls/download/jobs/enable"
    params = {"temp_update": "true"}

    headers = {
        "accept": "application/json",
        "api-key": MLS_ADMIN_API_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "sources": mls_ids,
        "updated_by": UPDATED_BY_USER
    }

    try:
        response = requests.post(endpoint, params=params, headers=headers, json=payload, timeout=15)
        if response.status_code in (200, 201):
            logger.info(f"✅ SUCCESS: Re-enabled API jobs for clean MLS IDs: {mls_ids}")
            return True
        else:
            logger.error(f"❌ FAILED TO ENABLE API JOBS [{response.status_code}]: {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ API Request Exception: {e}")
        return False


# ==============================================================================
# MAIN ROUTING ENGINE
# ==============================================================================

def main() -> None:
    logger.info("============================================================")
    logger.info("🚀 STAGE 9: VERIFYING DOWNLOADS & RE-ENABLING API JOBS")
    logger.info("============================================================")

    trigger_records = load_triggered_api_downloads_log()

    if not trigger_records:
        logger.info("No active API trigger records found to verify. Execution complete.")
        return

    batch_counts = verify_api_batch_records_in_bigquery(trigger_records)
    failing_batches = check_api_error_logs(trigger_records)

    verified_mls_ids = set()
    for r in trigger_records:
        m_id = r["mls_id"]
        b_id = r["batch_id"]
        if b_id not in failing_batches and batch_counts.get(b_id, 0) > 0:
            verified_mls_ids.add(m_id)

    clean_ids = sorted(list(verified_mls_ids))

    if clean_ids:
        logger.info(f"✅ CLEAN & VERIFIED API TARGETS ({len(clean_ids)}): {clean_ids}")
        if enable_api_download_jobs(clean_ids):
            trigger_scheduler_sync()
        logger.info("\n🎉 Stage 9 completed successfully! Scheduled API jobs are restored.")
    else:
        logger.warning("⚠️ No API targets passed verification. Download jobs remain disabled for safety.")


if __name__ == "__main__":
    main()
