"""
Production Test Script: Trigger RETS Jenkins Job & Monitor Execution.

Targets MLS ID 533 (KY_AABR):
1. Connects to Production Jenkins API.
2. Locates matching job: '533-KY_AABR-*'.
3. Enables the job and triggers 'buildWithParameters' (LOAD_TYPE=incr).
4. Asynchronously polls Jenkins API until the build completes.
"""

import os
import re
import time
import requests
from dotenv import load_dotenv

from pipeline_logger import setup_logger

load_dotenv()
logger = setup_logger("Test_JenkinsTriggerProd")

JENKINS_URL = os.getenv("JENKINS_PROD_URL", "").strip().strip("'\"")
JENKINS_USER = os.getenv("JENKINS_USER", "").strip().strip("'\"")
JENKINS_TOKEN = os.getenv("JENKINS_PROD_TOKEN", "").strip().strip("'\"")

TARGET_MLS_ID = "533"
TARGET_MLS_ID_STR = "KY_AABR"
LOAD_TYPE = "incr"

# Polling configuration
POLL_INTERVAL_SEC = 15  # Frequency of status checks
MAX_WAIT_MINUTES = 10  # Maximum timeout


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


def trigger_and_monitor_jenkins_job(session: requests.Session, job_name: str, headers: dict) -> bool:
    """Enables, triggers, and polls a Jenkins job until completion."""
    base_url = JENKINS_URL.rstrip("/")

    # 1. Enable Job
    enable_url = f"{base_url}/job/{job_name}/enable"
    session.post(enable_url, headers=headers, timeout=15)
    logger.info(f"✅ Enabled Jenkins job: {job_name}")

    # 2. Trigger Parameterized Build
    build_url = f"{base_url}/job/{job_name}/buildWithParameters"
    response = session.post(build_url, headers=headers, params={"LOAD_TYPE": LOAD_TYPE}, timeout=15)

    if response.status_code not in (200, 201, 202, 302):
        logger.error(f"❌ Failed to trigger build [{response.status_code}]: {response.text}")
        return False

    logger.info(f"🚀 Triggered build (LOAD_TYPE={LOAD_TYPE}) for '{job_name}'")

    # 3. Resolve Queue Item to get Build Number
    queue_url = response.headers.get("Location")
    build_number = None

    if queue_url:
        logger.info("⏳ Waiting for build to start in queue...")
        for _ in range(12):  # Wait up to 60 seconds for build to leave queue
            time.sleep(5)
            q_res = session.get(f"{queue_url.rstrip('/')}/api/json", timeout=10)
            if q_res.status_code == 200:
                q_data = q_res.json()
                executable = q_data.get("executable")
                if executable and "number" in executable:
                    build_number = executable["number"]
                    break

    # Fallback: query last build directly if queue location resolution timed out
    if not build_number:
        last_build_res = session.get(f"{base_url}/job/{job_name}/api/json", timeout=10)
        if last_build_res.status_code == 200:
            build_number = last_build_res.json().get("nextBuildNumber", 1) - 1

    if not build_number:
        logger.warning("⚠️ Could not determine build number. Job triggered successfully, but polling skipped.")
        return True

    console_url = f"{base_url}/job/{job_name}/{build_number}/console"
    logger.info(f"📌 Build #{build_number} active. View live logs: {console_url}")

    # 4. Asynchronous Polling Loop
    start_time = time.time()
    max_wait_seconds = MAX_WAIT_MINUTES * 60

    while (time.time() - start_time) < max_wait_seconds:
        status_res = session.get(f"{base_url}/job/{job_name}/{build_number}/api/json", timeout=10)
        if status_res.status_code == 200:
            data = status_res.json()
            is_building = data.get("building", False)
            result = data.get("result")

            if not is_building:
                if result == "SUCCESS":
                    logger.info(f"🎉 Build #{build_number} COMPLETED SUCCESSFULLY! Result: {result}")
                    return True
                else:
                    logger.error(f"❌ Build #{build_number} FAILED! Result: {result}")
                    return False

            elapsed = int(time.time() - start_time)
            logger.info(f"⏳ Build #{build_number} in progress... (Elapsed: {elapsed}s)")

        time.sleep(POLL_INTERVAL_SEC)

    logger.error(f"⌛ Timed out after {MAX_WAIT_MINUTES} minutes waiting for build #{build_number}.")
    return False


def main():
    logger.info("============================================================")
    logger.info("🧪 RUNNING JENKINS PROD PARAMETERIZED TRIGGER & MONITOR TEST")
    logger.info("============================================================")

    session = requests.Session()
    session.auth = (JENKINS_USER, JENKINS_TOKEN)
    headers = get_jenkins_crumb(session)

    # Resolve job for target 533
    pattern = re.compile(rf"^{re.escape(TARGET_MLS_ID)}-{re.escape(TARGET_MLS_ID_STR)}.*")
    list_res = session.get(f"{JENKINS_URL.rstrip('/')}/api/json?tree=jobs[name,url]", timeout=15)

    if list_res.status_code == 200:
        for job in list_res.json().get("jobs", []):
            if pattern.match(job["name"]):
                trigger_and_monitor_jenkins_job(session, job["name"], headers)
                return

    logger.error(f"❌ Could not find job matching '{TARGET_MLS_ID}-{TARGET_MLS_ID_STR}*'")


if __name__ == "__main__":
    main()
