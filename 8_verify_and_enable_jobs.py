"""
Stage 8: Granular Per-MLS Verification & Selective Job Enablement

1. Evaluates BigQuery logs post-trigger timestamp individually for each MLS target in batch.
2. Contains a failsafe sleep gate to ensure BigQuery has had enough time to index logs.
3. For CLEAN MLS targets (0 ERROR logs):
   - Automatically re-enables download jobs and triggers scheduler sync for that specific MLS.
4. For FAILING MLS targets (>= 1 ERROR log):
   - Keeps download jobs DISABLED.
   - Generates individual investigation queries in 'temp-data/investigate_errors.sql'.
"""

import os
import re
import sys
import time
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery

from pipeline_logger import setup_logger
from enable_download_jobs import enable_mls_download_jobs

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
logger = setup_logger("Stage8_VerifyAndEnable")

BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT", "data-shared-prod-44e4")
MINIMUM_WAIT_MINUTES = 15


def load_verification_context() -> tuple[str, dict[str, int]]:
    """
    Loads trigger timestamp, verifies elapsed time, and builds a mapping
    of mls_id_str -> integer mls_id from temp-data/batch_mls_targets.csv.
    """
    timestamp_file = current_dir / "temp-data" / "download_trigger_timestamp.txt"
    csv_path = current_dir / "temp-data" / "batch_mls_targets.csv"

    if not timestamp_file.exists():
        logger.error("❌ Trigger timestamp file not found. Did you run Stage 7 first?")
        sys.exit(1)

    trigger_timestamp = timestamp_file.read_text(encoding="utf-8").strip()

    # --- FAIL-SAFE TIME CHECK ---
    try:
        # Normalize ISO strings (e.g., '2026-08-06T22:15:00.000Z' or '2026-08-06T22:15:00.000')
        clean_ts = trigger_timestamp.replace("Z", "").split(".")[0]
        trigger_time = datetime.fromisoformat(clean_ts).replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)

        elapsed_minutes = (now - trigger_time).total_seconds() / 60.0

        if elapsed_minutes < MINIMUM_WAIT_MINUTES:
            remaining_mins = int(MINIMUM_WAIT_MINUTES - elapsed_minutes) + 1
            logger.warning(f"⚠️ SAFETY GATE: Only {elapsed_minutes:.1f} minutes have passed since Stage 7 trigger.")
            logger.warning(f"Sleeping for the remaining {remaining_mins} minute(s) to guarantee BigQuery logs are indexed...")
            time.sleep(remaining_mins * 60)
            logger.info("✅ Minimum wait time satisfied. Proceeding to BigQuery query.")
    except Exception as e:
        logger.warning(f"Could not parse trigger timestamp for safety check, proceeding carefully: {e}")
    # --------------------------------

    df = pd.read_csv(csv_path)
    # Map string identifier -> integer MLS ID
    mls_mapping = (
        df[["mls_id_str", "mls_id"]]
        .drop_duplicates()
        .set_index("mls_id_str")["mls_id"]
        .to_dict()
    )

    return trigger_timestamp, mls_mapping


def generate_investigation_queries(trigger_timestamp: str, failed_mls_details: dict[str, list[dict]]) -> Path:
    """Generates targeted SQL queries for failed MLS sources and their failing callers."""
    sql_file_path = current_dir / "temp-data" / "investigate_errors.sql"

    query_blocks = []
    query_blocks.append("-- ============================================================")
    query_blocks.append("-- MANUAL INVESTIGATION QUERIES FOR FAILED MLS DOWNLOADS")
    query_blocks.append(f"-- TRIGGER TIMESTAMP: {trigger_timestamp}")
    query_blocks.append("-- ============================================================\n")

    idx = 1
    for mls_str, error_rows in failed_mls_details.items():
        callers = sorted(list(set(r["caller_name"] for r in error_rows if r.get("caller_name"))))
        for caller in callers:
            block = f"""-- ------------------------------------------------------------
-- [{idx}] TARGET MLS: {mls_str} | CALLER: {caller}
-- ------------------------------------------------------------
SELECT 
    text_payload, 
    severity, 
    caller_name, 
    logger_name, 
    json_payload, 
    processed_at, 
    created_at
FROM `{BIGQUERY_PROJECT}.kw_logger.data_team_logs`
WHERE resource_type = 'beam_pipeline'
  AND processed_at >= TIMESTAMP('{trigger_timestamp}')
  AND severity = 'ERROR'
  AND caller_name = '{caller}'
  AND text_payload LIKE '%{mls_str}%'
ORDER BY processed_at DESC;
"""
            query_blocks.append(block)
            idx += 1

    sql_file_path.write_text("\n".join(query_blocks), encoding="utf-8")
    return sql_file_path


def verify_logs_per_mls(trigger_timestamp: str, mls_mapping: dict[str, int]) -> tuple[list[str], list[str], dict[str, list[dict]]]:
    """Queries BigQuery logs and groups results by mls_id_str."""
    client = bigquery.Client(project=BIGQUERY_PROJECT)
    all_mls_strs = list(mls_mapping.keys())

    # Escape special regex characters in MLS string targets
    mls_pattern = "|".join(re.escape(m) for m in all_mls_strs)

    query = f"""
        SELECT 
            severity, 
            caller_name, 
            REGEXP_EXTRACT(text_payload, r'({mls_pattern})') AS mls_id_str,
            COALESCE(JSON_VALUE(text_payload, '$.exception_type'), 'N/A') AS exception_type, 
            COUNT(*) AS log_count
        FROM `{BIGQUERY_PROJECT}.kw_logger.data_team_logs`
        WHERE processed_at >= TIMESTAMP('{trigger_timestamp}')
          AND REGEXP_CONTAINS(text_payload, r'({mls_pattern})')
        GROUP BY 1, 2, 3, 4
        ORDER BY 3, 2, 1
    """

    logger.info("============================================================")
    logger.info("🔍 STAGE 8: GRANULAR PER-MLS LOG VERIFICATION")
    logger.info("============================================================")
    logger.info(f"Checking logs processed after : {trigger_timestamp}")
    logger.info(f"Target MLS Identifiers ({len(all_mls_strs)}) : {', '.join(all_mls_strs)}")
    logger.info("Executing BigQuery check...\n")

    clean_mls_strs = []
    failed_mls_strs = []
    failed_mls_details = {}

    try:
        query_job = client.query(query)
        results = [dict(row) for row in query_job.result()]

        if results:
            logger.info(f"{'SEVERITY':<10} | {'MLS_ID_STR':<15} | {'CALLER_NAME':<25} | {'EXCEPTION_TYPE':<25} | {'COUNT':<8}")
            logger.info("-" * 90)
            for row in results:
                mls_val = row.get("mls_id_str") or "UNKNOWN"
                logger.info(f"{row['severity']:<10} | {mls_val:<15} | {row['caller_name']:<25} | {row['exception_type']:<25} | {row['log_count']:<8}")

        # Evaluate log severity per individual MLS
        for mls_str in all_mls_strs:
            # Match logs belonging to this specific MLS
            mls_logs = [r for r in results if r.get("mls_id_str") == mls_str]
            error_logs = [r for r in mls_logs if r.get("severity") == "ERROR"]

            if error_logs:
                failed_mls_strs.append(mls_str)
                failed_mls_details[mls_str] = error_logs
            else:
                clean_mls_strs.append(mls_str)

        return clean_mls_strs, failed_mls_strs, failed_mls_details

    except Exception as e:
        logger.error(f"❌ BigQuery Execution Error: {e}", exc_info=True)
        return [], all_mls_strs, {}


def main() -> None:
    trigger_timestamp, mls_mapping = load_verification_context()

    clean_mls_strs, failed_mls_strs, failed_mls_details = verify_logs_per_mls(trigger_timestamp, mls_mapping)

    logger.info("\n============================================================")
    logger.info("📊 STAGE 8 VERIFICATION SUMMARY")
    logger.info("============================================================")

    # 1. PROCESS CLEAN MLS SOURCES
    if clean_mls_strs:
        clean_ids = [mls_mapping[m] for m in clean_mls_strs]
        logger.info(f"✅ CLEAN MLS TARGETS ({len(clean_mls_strs)}): {', '.join(clean_mls_strs)}")
        logger.info(f"   Corresponding Integer IDs: {clean_ids}")
        logger.info("   Enabling download jobs and syncing scheduler...")

        # Pass integer MLS IDs explicitly to enable function
        enable_mls_download_jobs(mls_ids=clean_ids, temp_update=True)
    else:
        logger.warning("⚠️ No clean MLS targets found in this batch.")

    # 2. PROCESS FAILING MLS SOURCES
    if failed_mls_strs:
        sql_path = generate_investigation_queries(trigger_timestamp, failed_mls_details)
        logger.error("------------------------------------------------------------")
        logger.error(f"❌ FAILED MLS TARGETS ({len(failed_mls_strs)}): {', '.join(failed_mls_strs)}")
        logger.error("   Jobs for these targets REMAIN DISABLED due to detected ERROR logs.")
        logger.error(f"📄 Generated Investigation SQL File: {sql_path}")
        logger.error("   Please run queries in BigQuery to investigate failure root cause.")
        logger.error("------------------------------------------------------------")
        sys.exit(1)
    else:
        logger.info("\n🎉 All MLS targets in batch passed verification successfully!")


if __name__ == "__main__":
    main()
