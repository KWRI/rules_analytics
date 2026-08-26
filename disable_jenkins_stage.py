import csv
import os
import re
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Stage Configuration
JENKINS_URL = os.getenv("JENKINS_STAGE_URL")
USER = os.getenv("JENKINS_USER")
TOKEN = os.getenv("JENKINS_STAGE_TOKEN")

# Build absolute path to CSV file relative to this script's location
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "temp-data", "batch_mls_targets_rets.csv")


def get_all_jenkins_jobs():
    """Fetch all job names and URLs from the Stage Jenkins API."""
    if not JENKINS_URL:
        print("Error: JENKINS_STAGE_URL is not defined in your .env file.")
        return []

    base_url = JENKINS_URL.rstrip('/')
    url = f"{base_url}/api/json?tree=jobs[name,url]"

    try:
        response = requests.get(url, auth=(USER, TOKEN), timeout=10)
        if response.status_code == 200:
            return response.json().get("jobs", [])
        else:
            print(f"Failed to fetch jobs. HTTP Status: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error connecting to Jenkins: {e}")
        return []


def toggle_job_status(job_name, action="disable"):
    """Enable or disable a specific Jenkins job."""
    base_url = JENKINS_URL.rstrip('/')
    url = f"{base_url}/job/{job_name}/{action}"

    try:
        response = requests.post(url, auth=(USER, TOKEN), timeout=10)
        if response.status_code in [200, 201, 302]:
            print(f"Successfully performed '{action}' on: {job_name}")
        else:
            print(f"Failed to '{action}' {job_name}. HTTP Status: {response.status_code}")
    except Exception as e:
        print(f"Error updating job {job_name}: {e}")


def main():
    print(f"--- Running automation against Stage Jenkins ({JENKINS_URL}) ---")

    if not os.path.exists(CSV_PATH):
        print(f"Error: Could not find CSV file at path: {CSV_PATH}")
        return

    # 1. Extract unique mls_id and mls_id_str targets from CSV using a set
    targets = set()
    with open(CSV_PATH, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mls_id = row.get("mls_id", "").strip()
            mls_id_str = row.get("mls_id_str", "").strip()
            if mls_id and mls_id_str:
                targets.add((mls_id, mls_id_str))

    print(f"Loaded {len(targets)} unique target pattern pair(s) from CSV.")

    # 2. Retrieve existing Stage Jenkins jobs
    jenkins_jobs = get_all_jenkins_jobs()
    if not jenkins_jobs:
        print("No jobs fetched from Jenkins. Exiting.")
        return

    # 3. Match jobs based on pattern: mls_id-mls_id_str.* (using dict to deduplicate)
    matched_jobs_dict = {}
    for mls_id, mls_id_str in targets:
        pattern = re.compile(rf"^{re.escape(mls_id)}-{re.escape(mls_id_str)}.*")
        for job in jenkins_jobs:
            if pattern.match(job["name"]):
                matched_jobs_dict[job["name"]] = job

    matched_jobs = list(matched_jobs_dict.values())

    # 4. Display Matched Results
    print(f"\nFound {len(matched_jobs)} unique matching Stage Jenkins job(s):")
    for job in matched_jobs:
        print(f" - Name: {job['name']}\n   URL:  {job['url']}")

    # 5. Execute Job Status Update
    ACTION = "disable"  # Set to "enable" or "disable"

    if matched_jobs:
        print(f"\nExecuting '{ACTION}' on matched jobs...")
        for job in matched_jobs:
            toggle_job_status(job["name"], action=ACTION)


if __name__ == "__main__":
    main()
