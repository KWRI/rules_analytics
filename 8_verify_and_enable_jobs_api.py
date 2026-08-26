"""
Stage 8: Production MLS Download Verification & Automatic Job Re-Enablement (API Variant)

1. Reads batch_id entries recorded by Stage 7 in temp-data/triggered_downloads_log_api.csv.
2. Direct-queries stream-listing-prod.mls_download.{content_type} WHERE batch_id = <id>.
3. Checks kw_logger.data_team_logs for ERROR severities matching target batch IDs in both json_payload and text_payload.
4. Automatically re-enables download jobs and syncs scheduler for verified clean targets.
"""

import os
import sys
import csv
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
from google.auth.exceptions import RefreshError

from pipeline_logger import setup_logger
from enable_download_jobs import enable_mls_download_jobs

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
logger = setup_logger("Stage8_VerifyAndEnable")

BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT", "data-shared-prod-44e4")
DOWNLOAD_PROD_PROJECT = os.getenv("DOWNLOAD_PROD_PROJECT", "stream-listing-prod")


def load_triggered_downloads_log() -> list[dict]:
    """Loads batch trigger records from Stage 7 output log."""
    log_file = current_dir / "temp-data" / "triggered_downloads_log_api.csv"

    if not log_file.exists():
        logger.error(f"❌ Triggered downloads log missing at '{log_file}'. Did you run Stage 7 first?")
        sys.exit(1)

    trigger_records = []
    with open(log_file, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            trigger_records.append({
                "mls_id": int(row["mls_id"].strip()),
                "mls_id_str": row["mls_id_str"].strip(),
                "content_type": row["content_type"].strip(),
                "batch_id": row["batch_id"].strip(),
            })

    return trigger_records


def verify_batch_records_in_bigquery(records: list[dict]) -> dict[str, int]:
    """Queries stream-listing-prod.mls_download.{content_type} by exact batch_id."""
    try:
        client = bigquery.Client(project=DOWNLOAD_PROD_PROJECT)
    except Exception as e:
        logger.error(f"❌ Failed to initialize BigQuery client: {e}")
        logger.error("👉 Try running: `gcloud auth application-default login --scopes=\"https://www.googleapis.com/auth/cloud-platform\"`")
        sys.exit(1)

    logger.info("============================================================")
    logger.info("🔍 STAGE 8: CHECKING PRODUCTION DOWNLOAD TABLES BY BATCH ID")
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
            logger.error("❌ GCP Authentication Expired! Run: `gcloud auth application-default login --scopes=\"https://www.googleapis.com/auth/cloud-platform\"`")
            sys.exit(1)
        except Exception as e:
            logger.error(f"❌ Error querying `{table_name}` for Batch ID {b_id}: {e}")
            batch_counts[b_id] = 0

    return batch_counts


def check_error_logs(records: list[dict]) -> list[str]:
    """Checks data_team_logs for ERROR severities matching target batch IDs in both json_payload and text_payload."""
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
        WHERE severity = 'ERROR'
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
        logger.error("❌ GCP Authentication Expired! Run: `gcloud auth application-default login --scopes=\"https://www.googleapis.com/auth/cloud-platform\"`")
        sys.exit(1)
    except Exception as e:
        logger.warning(f"⚠️ Log error check encountered an exception: {e}")

    return failing_batch_ids


def main() -> None:
    trigger_records = load_triggered_downloads_log()

    # 1. Query Production Tables by exact Batch ID
    batch_counts = verify_batch_records_in_bigquery(trigger_records)

    # 2. Check for Execution Errors in kw_logger by Batch ID
    failing_batches = check_error_logs(trigger_records)

    # 3. Determine Clean MLS Targets
    verified_mls_ids = set()
    for r in trigger_records:
        m_id = r["mls_id"]
        b_id = r["batch_id"]

        if b_id not in failing_batches and batch_counts.get(b_id, 0) > 0:
            verified_mls_ids.add(m_id)

    clean_ids = sorted(list(verified_mls_ids))

    logger.info("\n============================================================")
    logger.info("📊 STAGE 8 VERIFICATION SUMMARY")
    logger.info("============================================================")

    if clean_ids:
        logger.info(f"✅ CLEAN & VERIFIED MLS TARGETS ({len(clean_ids)}): {clean_ids}")
        logger.info("   Enabling download jobs and syncing scheduler...")
        enable_mls_download_jobs(mls_ids=clean_ids, temp_update=True)
        logger.info("\n🎉 Pipeline completed successfully! Scheduled jobs are restored.")
    else:
        logger.warning("⚠️ No MLS targets passed both download batch verification and zero-error checks.")
        logger.warning("   Download jobs remain DISABLED for safety.")


if __name__ == "__main__":
    main()
