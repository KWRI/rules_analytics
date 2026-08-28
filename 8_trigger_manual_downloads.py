"""
Pipeline Stage 8: Multi-Protocol Ingestion Job Enabler & Trigger Engine (Parallelized & Robust).

Parses target batch files ('temp-data/batch_mls_targets_rets.csv' and 'temp-data/batch_mls_targets_api.csv')
and triggers ingestion downloads in parallel:
  - RETS Integration: Enables Jenkins jobs, triggers 'buildWithParameters' (LOAD_TYPE=incr),
                      and polls execution status until completion concurrently.
  - API Integration: Triggers 3-day manual reprocess downloads via Production Microservice API,
                     captures batch IDs, and logs details to 'temp-data/triggered_downloads_log_api.csv'.

Logs execution events to 'logs/pipeline_YYYY-MM-DD.log'.
"""

import os
import csv
import re
import time
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from dotenv import load_dotenv

from pipeline_logger import setup_logger

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
logger = setup_logger("Stage8_TriggerDownload")

# ==============================================================================
# ENVIRONMENT CONSTANTS
# ==============================================================================
# Jenkins Config (RETS)
JENKINS_URL = os.getenv("JENKINS_PROD_URL", "").strip().strip("'\"")
JENKINS_USER = os.getenv("JENKINS_USER", "").strip().strip("'\"")
JENKINS_TOKEN = os.getenv("JENKINS_PROD_TOKEN", "").strip().strip("'\"")

# API Microservice Config (API Production)
MLS_ADMIN_PROD_URL = os.getenv("MLS_ADMIN_PROD_URL", "").strip().strip("'\"").rstrip("/")
PROD_BASE_URL = MLS_ADMIN_PROD_URL.split("/v1/mls-admin")[0] if "/v1/mls-admin" in MLS_ADMIN_PROD_URL else MLS_ADMIN_PROD_URL

API_KEY = os.getenv("MLS_ADMIN_API_KEY") or os.getenv("API_KEY", "")

# Jenkins Polling Config
POLL_INTERVAL_SEC = 15
MAX_WAIT_MINUTES = 10
MAX_PARALLEL_WORKERS = 8  # Parallel threads for concurrent triggers

# API Download Defaults
DEFAULT_RELATIVE_PERIOD = "3"
DEFAULT_RELATIVE_PERIOD_UNIT = "day"


# ==============================================================================
# ROBUST NETWORK SESSION BUILDER
# ==============================================================================

def create_robust_session() -> requests.Session:
    """Creates a requests session configured with automatic retries for dropped connections."""
    session = requests.Session()
    retries = Retry(
        total=3,  # Max 3 retries for connection blips
        backoff_factor=2,  # Waits 2s, 4s, 8s between attempts
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# ==============================================================================
# CODE BLOCK 1: RETS INGESTION TRIGGER & MONITORING (JENKINS)
# ==============================================================================

def get_jenkins_crumb(session: requests.Session) -> dict:
    """Fetches a CSRF Crumb header if required by Jenkins."""
    base_url = JENKINS_URL.rstrip("/")
    try:
        response = session.get(f"{base_url}/crumbIssuer/api/json", timeout=10)
        if response.status_code == 200:
            data = response.json()
            return {data["crumbRequestField"]: data["crumb"]}
    except Exception:
        pass
    return {}


def trigger_and_monitor_jenkins_job(session: requests.Session, job_name: str, headers: dict, load_type: str = "incr") -> bool:
    """Enables, triggers, and polls a Jenkins job until completion (Thread Worker)."""
    base_url = JENKINS_URL.rstrip("/")

    # 1. Enable Job
    try:
        enable_url = f"{base_url}/job/{job_name}/enable"
        session.post(enable_url, headers=headers, timeout=15)
        logger.info(f"✅ Enabled Jenkins job: {job_name}")
    except Exception as e:
        logger.warning(f"⚠️ Could not verify enabled status for '{job_name}': {e}")

    # 2. Trigger Parameterized Build
    build_url = f"{base_url}/job/{job_name}/buildWithParameters"
    try:
        response = session.post(build_url, headers=headers, params={"LOAD_TYPE": load_type}, timeout=15)
        if response.status_code not in (200, 201, 202, 302):
            logger.error(f"❌ Failed to trigger build for '{job_name}' [{response.status_code}]: {response.text}")
            return False
        logger.info(f"🚀 Triggered build (LOAD_TYPE={load_type}) for '{job_name}'")
    except Exception as e:
        logger.error(f"❌ Network failure while triggering '{job_name}': {e}")
        return False

    # 3. Resolve Queue Item to get Build Number
    queue_url = response.headers.get("Location")
    build_number = None

    if queue_url:
        logger.info(f"⏳ Waiting for build '{job_name}' to start in queue...")
        for _ in range(12):  # Wait up to 60s for build assignment
            time.sleep(5)
            try:
                q_res = session.get(f"{queue_url.rstrip('/')}/api/json", timeout=10)
                if q_res.status_code == 200:
                    q_data = q_res.json()
                    executable = q_data.get("executable")
                    if executable and "number" in executable:
                        build_number = executable["number"]
                        break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as err:
                logger.warning(f"⚠️ Connection glitch polling queue for '{job_name}' (retrying): {err}")

    # Fallback: query job API directly if queue resolution timed out
    if not build_number:
        try:
            last_build_res = session.get(f"{base_url}/job/{job_name}/api/json", timeout=10)
            if last_build_res.status_code == 200:
                build_number = last_build_res.json().get("nextBuildNumber", 1) - 1
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch last build number for '{job_name}': {e}")

    if not build_number:
        logger.warning(f"⚠️ Could not determine build number for '{job_name}'. Trigger issued, skipping polling.")
        return True

    console_url = f"{base_url}/job/{job_name}/{build_number}/console"
    logger.info(f"📌 Build #{build_number} active for '{job_name}'. Live logs: {console_url}")

    # 4. Asynchronous Polling Loop
    start_time = time.time()
    max_wait_seconds = MAX_WAIT_MINUTES * 60

    while (time.time() - start_time) < max_wait_seconds:
        try:
            status_res = session.get(f"{base_url}/job/{job_name}/{build_number}/api/json", timeout=10)
            if status_res.status_code == 200:
                data = status_res.json()
                is_building = data.get("building", False)
                result = data.get("result")

                if not is_building:
                    if result == "SUCCESS":
                        logger.info(f"🎉 Build #{build_number} for '{job_name}' COMPLETED SUCCESSFULLY!")
                        return True
                    else:
                        logger.error(f"❌ Build #{build_number} for '{job_name}' FAILED! Result: {result}")
                        return False

                elapsed = int(time.time() - start_time)
                logger.info(f"⏳ Build #{build_number} ('{job_name}') in progress... (Elapsed: {elapsed}s)")
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as err:
            logger.warning(f"⚠️ Network blip polling status for '{job_name}': {err}. Retrying in {POLL_INTERVAL_SEC}s...")

        time.sleep(POLL_INTERVAL_SEC)

    logger.error(f"⌛ Timed out after {MAX_WAIT_MINUTES} minutes waiting for '{job_name}' build #{build_number}.")
    return False


def process_rets_triggers_parallel(rets_targets: set):
    """Handles target job matching and parallel execution for RETS feeds."""
    logger.info("------------------------------------------------------------")
    logger.info(f"⚙️ [RETS BLOCK] Processing {len(rets_targets)} unique target pair(s) via Jenkins (Parallel)...")
    logger.info("------------------------------------------------------------")

    if not JENKINS_URL or not JENKINS_TOKEN:
        logger.error("❌ Missing JENKINS_PROD_URL or JENKINS_PROD_TOKEN in environment configuration.")
        return

    session = create_robust_session()
    session.auth = (JENKINS_USER, JENKINS_TOKEN)
    headers = get_jenkins_crumb(session)

    list_res = session.get(f"{JENKINS_URL.rstrip('/')}/api/json?tree=jobs[name,url]", timeout=15)
    if list_res.status_code != 200:
        logger.error(f"❌ Failed to fetch Jenkins jobs. HTTP Status: {list_res.status_code}")
        return

    jenkins_jobs = list_res.json().get("jobs", [])
    matched_jobs_dict = {}

    for mls_id, mls_id_str in rets_targets:
        pattern = re.compile(rf"^{re.escape(str(mls_id))}-{re.escape(str(mls_id_str))}.*")
        for job in jenkins_jobs:
            if pattern.match(job["name"]):
                matched_jobs_dict[job["name"]] = job

    matched_jobs = list(matched_jobs_dict.values())
    logger.info(f"Found {len(matched_jobs)} matching Jenkins job(s) for RETS targets.")

    if not matched_jobs:
        return

    # Multithreaded execution across worker threads
    success_count = 0
    failure_count = 0

    with ThreadPoolExecutor(max_workers=min(len(matched_jobs), MAX_PARALLEL_WORKERS)) as executor:
        future_to_job = {
            executor.submit(trigger_and_monitor_jenkins_job, session, job["name"], headers, "incr"): job["name"]
            for job in matched_jobs
        }

        for future in as_completed(future_to_job):
            job_name = future_to_job[future]
            try:
                if future.result():
                    success_count += 1
                else:
                    failure_count += 1
            except Exception as exc:
                logger.error(f"❌ Exception in thread execution for '{job_name}': {exc}")
                failure_count += 1

    logger.info(f"✅ RETS Builds Complete | Successful: {success_count} | Failed: {failure_count}")


# ==============================================================================
# CODE BLOCK 2: API INGESTION MANUAL DOWNLOAD TRIGGER ENGINE
# ==============================================================================

def trigger_api_download(session: requests.Session, mls_id: int, mls_id_str: str, content_type: str, content_sub_type: str) -> dict | None:
    """Triggers 3-day download for an API target and returns the record if successful."""
    url = f"{PROD_BASE_URL}/v1/listing-mflow/reprocess/download/mls/{mls_id}"

    headers = {
        "accept": "application/json",
        "api-key": API_KEY,
    }

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
        response = session.post(url, headers=headers, params=params, data="", timeout=30)

        if response.status_code in (200, 201, 202):
            data = response.json() if response.text else {}
            batch_id = str(data.get("batch_id", "")).strip()
            logger.info(f"   ✅ SUCCESS [{response.status_code}]: Triggered Batch ID = {batch_id}")
            if batch_id:
                return {
                    "mls_id": mls_id,
                    "mls_id_str": mls_id_str,
                    "content_type": content_type,
                    "content_sub_type": content_sub_type,
                    "batch_id": batch_id,
                }
            return None

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
        logger.error(f"   ❌ Request Exception for MLS {mls_id}: {e}", exc_info=True)
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


def process_api_triggers_parallel(api_targets: list[dict]):
    """Processes 3-day manual reprocess downloads for API protocol targets concurrently."""
    logger.info("------------------------------------------------------------")
    logger.info(f"⚙️ [API BLOCK] Processing {len(api_targets)} target download payload(s) via API Service (Parallel)...")
    logger.info("------------------------------------------------------------")

    if not API_KEY:
        logger.error("❌ API_KEY / MLS_ADMIN_API_KEY missing from .env file.")
        return

    session = create_robust_session()
    triggered_records = []
    total_triggered = 0
    total_failed = 0

    with ThreadPoolExecutor(max_workers=min(len(api_targets), MAX_PARALLEL_WORKERS)) as executor:
        future_to_payload = {
            executor.submit(
                trigger_api_download,
                session,
                t["mls_id"],
                t["mls_id_str"],
                t["content_type"],
                t["content_sub_type"]
            ): t for t in api_targets
        }

        for future in as_completed(future_to_payload):
            rec = future.result()
            if rec:
                total_triggered += 1
                triggered_records.append(rec)
            else:
                total_failed += 1

    save_triggered_downloads_log(triggered_records)
    logger.info(f"✅ API Triggers Complete | Successful: {total_triggered} | Failed: {total_failed}")


# ==============================================================================
# MAIN ROUTING ENGINE
# ==============================================================================

def main():
    logger.info("============================================================")
    logger.info("🚀 STAGE 8: ENABLING INGESTION JOBS & TRIGGERING DOWNLOADS")
    logger.info("============================================================")

    temp_data_dir = current_dir / "temp-data"
    rets_csv_path = temp_data_dir / "batch_mls_targets_rets.csv"
    api_csv_path = temp_data_dir / "batch_mls_targets_api.csv"

    rets_targets = set()
    api_targets_dict = {}

    # 1. Parse RETS Targets
    if rets_csv_path.exists():
        with open(rets_csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                mls_id = row.get("mls_id", "").strip()
                mls_id_str = row.get("mls_id_str", "").strip()
                if mls_id and mls_id_str:
                    rets_targets.add((mls_id, mls_id_str))

    # 2. Parse API Targets
    if api_csv_path.exists():
        with open(api_csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row.get("mls_id"):
                    continue
                mls_id = int(row["mls_id"].strip())
                mls_id_str = row.get("mls_id_str", f"ID_{mls_id}").strip()
                content_type = row.get("content_type", "").strip()
                content_sub_type = row.get("content_sub_type", "").strip()

                key = (mls_id, content_type, content_sub_type)
                if key not in api_targets_dict:
                    api_targets_dict[key] = {
                        "mls_id": mls_id,
                        "mls_id_str": mls_id_str,
                        "content_type": content_type,
                        "content_sub_type": content_sub_type,
                    }

    api_targets = list(api_targets_dict.values())

    logger.info(f"RETS Target Feeds Discovered : {len(rets_targets)}")
    logger.info(f"API Target Feeds Discovered  : {len(api_targets)}")
    logger.info("============================================================")

    if rets_targets:
        process_rets_triggers_parallel(rets_targets)

    if api_targets:
        process_api_triggers_parallel(api_targets)

    if not rets_targets and not api_targets:
        logger.warning("No RETS or API targets discovered in batch CSVs. No downloads were triggered.")


if __name__ == "__main__":
    main()
