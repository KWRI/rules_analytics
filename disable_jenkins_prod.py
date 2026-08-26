import csv
import os
import re
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Production Configuration
JENKINS_URL = os.getenv("JENKINS_PROD_URL")
USER = os.getenv("JENKINS_USER")
TOKEN = os.getenv("JENKINS_PROD_TOKEN")

# DRY_RUN = True: Only fetches and prints matching jobs. Does NOT execute disable/enable.
# DRY_RUN = False: Will actively modify Production jobs.
DRY_RUN = False

# Build path to CSV file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "temp-data", "batch_mls_targets_rets.csv")


def get_all_jenkins_jobs():
    """Fetch all job names and URLs from the Production Jenkins API."""
    if not JENKINS_URL or not TOKEN:
        print("Error: Missing JENKINS_PROD_URL or JENKINS_PROD_TOKEN in .env")
        return []

    base_url = JENKINS_URL.rstrip('/')
    url = f"{base_url}/api/json?tree=jobs[name,url]"

    try:
        response = requests.get(url, auth=(USER, TOKEN), timeout=10)
        if response.status_code == 200:
            return response.json().get("jobs", [])
        elif response.status_code == 401:
            print("Authentication failed (401). Please check your JENKINS_PROD_TOKEN.")
            return []
        else:
            print(f"Failed to fetch jobs. HTTP Status: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error connecting to Production Jenkins: {e}")
        return []


def toggle_job_status(job_name, action="disable"):
    """Enable or disable a specific Production Jenkins job."""
    base_url = JENKINS_URL.rstrip('/')
    url = f"{base_url}/job/{job_name}/{action}"

    try:
        response = requests.post(url, auth=(USER, TOKEN), timeout=10)
        if response.status_code in [200, 201, 302]:
            print(f" Successfully performed '{action}' on PROD: {job_name}")
        else:
            print(f" Failed to '{action}' {job_name}. HTTP Status: {response.status_code}")
    except Exception as e:
        print(f"Error updating job {job_name}: {e}")


def main():
    mode = "TEST / DRY-RUN (No changes will be made)" if DRY_RUN else "LIVE EXECUTION"
    print(f"--- Running automation against Production Jenkins ---")
    print(f"Target: {JENKINS_URL}")
    print(f"Mode:   {mode}\n")

    if not os.path.exists(CSV_PATH):
        print(f"Error: Could not find CSV file at path: {CSV_PATH}")
        return

    # 1. Extract unique targets from CSV
    targets = set()
    with open(CSV_PATH, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mls_id = row.get("mls_id", "").strip()
            mls_id_str = row.get("mls_id_str", "").strip()
            if mls_id and mls_id_str:
                targets.add((mls_id, mls_id_str))

    print(f"Loaded {len(targets)} unique target pattern pair(s) from CSV.")

    # 2. Retrieve existing Production Jenkins jobs
    jenkins_jobs = get_all_jenkins_jobs()
    if not jenkins_jobs:
        print("No jobs fetched from Production Jenkins. Exiting.")
        return

    # 3. Match jobs based on pattern: mls_id-mls_id_str.*
    matched_jobs_dict = {}
    for mls_id, mls_id_str in targets:
        pattern = re.compile(rf"^{re.escape(mls_id)}-{re.escape(mls_id_str)}.*")
        for job in jenkins_jobs:
            if pattern.match(job["name"]):
                matched_jobs_dict[job["name"]] = job

    matched_jobs = list(matched_jobs_dict.values())

    # 4. Display Matched Results
    print(f"\nFound {len(matched_jobs)} unique matching Production Jenkins job(s):")
    for job in matched_jobs:
        print(f" - Name: {job['name']}\n   URL:  {job['url']}")

    # 5. Execute or Simulate
    ACTION = "disable"

    if matched_jobs:
        if DRY_RUN:
            print(f"\n[DRY-RUN COMPLETE] Found {len(matched_jobs)} job(s) ready to be disabled.")
            print("Set 'DRY_RUN = False' in the script when you are ready to execute on Production.")
        else:
            print(f"\nExecuting '{ACTION}' on Production matched jobs...")
            for job in matched_jobs:
                toggle_job_status(job["name"], action=ACTION)


if __name__ == "__main__":
    main()
