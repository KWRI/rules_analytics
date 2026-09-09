# 🚀 MLS Rule Migration Execution Guide

This repository (`rules_analytics`) contains the end-to-end automated pipeline required to standardize legacy rule naming conventions into `PascalCase` across staging and production infrastructure.

## 📋 Prerequisites & System Requirements

Before running the migration pipeline, ensure your system meets the following requirements:

### 1. Requirements & Tools

* **Python 3.10+**
* **Git** (configured with access to Keller Williams internal repositories)
* **SSH Client & Keys** with access to staging and production bastion hosts (`bas-stage-cs.data.kw.com` / `sdh.data.kw.com`)
* **Network Access**: Internal network or VPN access required to reach DB instances, microservices, and Jenkins hosts.

### 2. Sibling Repository Dependencies

This project relies on local sibling repositories referenced in `.env.bulk`. Ensure the following repositories are cloned adjacent to this project directory:

* `eim-slp-tools`
* `eim-utilities-pip`
* `eim-snowflake-id`
* `dm-consolidated-rules`

## 📦 Installation & Setup

1. **Clone the repository:**
```bash
git clone git@github.com:kwri/rules_analytics.git
cd rules_analytics
```

2. **Set up a Virtual Environment:**

Ensure you have Python 3.12 installed before creating your environment to guarantee compatibility with all pre-compiled binary dependencies (such as `psycopg2-binary` and `Django 6.x`).

```bash
# Windows (PowerShell / CMD):
py -3.12 -m venv .venv

# macOS / Linux:
python3.12 -m venv .venv

# Activate the environment:
# Windows (PowerShell):
.\.venv\Scripts\Activate.ps1

# Windows (Git Bash):
source .venv/Scripts/activate

# macOS / Linux:
source .venv/bin/activate
```

3. **Install Dependencies:**
```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## ⚙️ Environment Configuration

The pipeline requires two configuration files in the project root: `.env` and `.env.bulk`.

### `.env`

```env
# Database Credentials (Staging & Production Verification)
DB_NAME=mls_admin
DB_USER=your_username            # Mandatory: Used for audit trail attribution in API calls (updated_by)
DB_PASSWORD=your_password
DB_HOST=pg-sdk-stage-ext.data.kw.com
STAGE_DB_HOST=pg-sdk-stage-ext.data.kw.com
PROD_DB_HOST=pg-sdk-prod-ext.data.kw.com

# SSH / Bastion Tunnel Configuration
SSH_HOST=bas-stage-cs.data.kw.com
STAGE_SSH_HOST=bas-stage-cs.data.kw.com
SSH_USER=your_ssh_username
SSH_KEY_PATH=C:/Users/your.username/path/to/ssh_key.pem
SSH_KEY_PASSPHRASE=your_key_passphrase

# Remote Promotion Host (SDH Server)
REMOTE_SSH_HOST=sdh.data.kw.com
REMOTE_SSH_USER=your_ssh_username
REMOTE_SSH_KEY_PATH=C:/Users/your.username/path/to/ssh_key.pem
REMOTE_SSH_KEY_PASSPHRASE=your_key_passphrase
REMOTE_MANIFEST_PATH=/eim-mls-admin-ms/devtools/source_promotion/promotion_sources.txt

# Local Repository Paths
REPO_PATH=C:/Users/your.username/Documents/GitHub/dm-consolidated-rules

# Microservice Endpoints & Keys
MLS_ADMIN_STAGE_URL=https://stage-ext-ms.data.kw.com/v1/mls-admin
MLS_ADMIN_PROD_URL=https://prod-ext-ms.data.kw.com/v1/mls-admin
MLS_ADMIN_API_KEY=your_mls_admin_api_key

# Jenkins Integration (RETS Ingestion Jobs)
JENKINS_USER=your.email@kw.com
JENKINS_PROD_URL=https://jenkins-prod.data.kw.com/
JENKINS_PROD_TOKEN=your_jenkins_prod_token

# Google BigQuery (Landing Verification)
GCP_PROJECT_ID=stream-listing-prod
GOOGLE_APPLICATION_CREDENTIALS=C:/Users/your.username/path/to/gcp_creds.json
```

### `.env.bulk`

```env
MS_HOST=https://stage-ext-ms.data.kw.com/v1.0.3
MS_API_KEY=your_mls_admin_api_key
LOG_LEVEL=DEBUG

# Local Sibling Repository Paths
TOOLS_REPO_PATH=C:/Users/your.username/Documents/GitHub/eim-slp-tools
UTILS_REPO_PATH=C:/Users/your.username/Documents/GitHub/eim-utilities-pip
SNOWFLAKE_REPO_PATH=C:/Users/your.username/Documents/GitHub/eim-snowflake-id
```

## 🏃 Execution Methods

You can run the migration pipeline either automatically using the master orchestrator script or manually stage-by-stage.

### Method 1: Master Orchestrator (Recommended)

`run_pipeline.py` executes the entire pipeline sequentially (Stages 0–11), handling live Production verification checks, dry-run safety gates, no-op MLS fast-forwarding, and BigQuery wait timers automatically.

#### Interactive Mode

Runs with interactive `(y/N)` confirmation prompts before mutating Staging DB, disabling Production jobs, promoting, and purging temp files:

```bash
python run_pipeline.py --limit 1
```

#### Automated / CI Mode

Skips all interactive prompts and automatically sleeps for BigQuery log streaming:

```bash
python run_pipeline.py --limit 1 --auto-approve --wait-mins 5
```

#### Command Flags for `run_pipeline.py`

- `-l, --limit <int>`: Batch size per protocol variant (API / RETS) selected in Stage 0 (Default: `4`).
- `--auto-approve`: Bypasses manual `(y/N)` confirmation prompts.
- `--wait-mins <int>`: Minutes to pause after triggering downloads (Stage 8) before verifying BigQuery landing logs (Stage 9) (Default: `5`).

### Method 2: Manual / Sequential Execution

If you need to execute specific stages individually, run the scripts in the following numerical order:

```bash
# Stage 0: Batch Target Selection (API & RETS with live Production status filtering)
python 0_prepare_batch_targets_api.py --limit 1
python 0_prepare_batch_targets_rets.py --limit 1

# Stage 1: Backup Raw DB Records to Local Git Repository
python 1_sync_database_to_repo.py

# Stage 2: Compile PascalCase Standard Rules & Blueprint Matrix
python 2_standardize_rules.py

# Stage 3: Register PascalCase Rules via Staging Microservice API
python 3_inserting_new_rule.py

# Stage 4: Correlate Targets & Generate Mutation Mapping
python 4_generate_targeted_csv.py

# Stage 5: Staging Bulk DB Mutation (Dry-Run First, Then Live Update)
python 5_run_bulk_update.py --input-file temp-data/raw_targeted_rules.csv --debug
python 5_run_bulk_update.py --input-file temp-data/raw_targeted_rules.csv --do-update

# Stage 6: Disable Production Ingestion Jobs (Jenkins & API Service)
python 6_disable_ingestion_jobs.py

# Stage 7: Production Promotion Engine (Remote SDH SSH Runner)
python 7_run_bulk_promotion.py

# Stage 8: Trigger 3-Day Test Ingestion Reprocessing
python 8_trigger_manual_downloads.py

# --- Pause ~5 Minutes to Allow Production BigQuery Data Landing ---

# Stage 9: Verify BigQuery Landing Logs & Re-Enable Verified API Schedules
python 9_verify_and_enable_injestion_jobs_api.py

# Stage 10: Soft Delete Legacy Rules via Stage Microservice API
python 10_soft_delete_legacy_rules.py

# Stage 11: Sync Repository Archival, Update Ledger & Clean Temporary Files
python 11_update_consolidated_repo.py
```

## 📋 Script Execution Sequence Reference

- **`0_prepare_batch_targets_api.py`** **&** **`0_prepare_batch_targets_rets.py`**: Queries unmigrated legacy rules and builds batch target CSVs in `temp-data/`.
- **`1_sync_database_to_repo.py`**: Connects via SSH to back up raw DB rule records to disk.
- **`2_standardize_rules.py`**: Transforms names to `PascalCase`, resolves versioning collisions (V1/V2), and generates `migration_blueprint.csv`.
- **`3_inserting_new_rule.py`**: Registers newly standardized rules in Staging via `POST /v1/mls-admin/rules`.
- **`4_generate_targeted_csv.py`**: Traverses JSON property maps, correlates targets, and generates `raw_targeted_rules.csv`.
- **`5_run_bulk_update.py`**: Applies bulk schema updates directly to the Staging database via microservice API handoff (`bulk_map_tool`).
- **`6_disable_ingestion_jobs.py`**: Disables Production Jenkins jobs (RETS) and API ingestion schedules (passing `DB_USER` for audit attribution).
- **`7_run_bulk_promotion.py`**: Uploads deployment manifests via SFTP and runs production promotion scripts (`promote_source.py`).
- **`8_trigger_manual_downloads.py`**: Enables required ingestion schedules and triggers parallel 3-day reprocessing ranges in Production.
- **`9_verify_and_enable_injestion_jobs_api.py`**: Queries BigQuery landing tables (`mls_download`) by `batch_id`, validates download results, and restores schedules for clean MLS targets using `DB_USER`.
- **`10_soft_delete_legacy_rules.py`**: Checks if legacy rules are used by other unmigrated MLSs, soft-deleting only fully unlinked rules via API.
- **`11_update_consolidated_repo.py`**: Moves soft-deleted rules to `archived/`, records completed sources in `processed_mls_ledger.json`, purges old logs (>3 days), and cleans `temp-data/`.

## 📂 Key State Files in `temp-data/`

- **`processed_mls_ledger.json`**: Permanent ledger tracking all fully migrated and audited MLS IDs. Stage 0 reads this file to ensure completed MLS sources are never re-processed.
- **`skip_mls.txt`**: Numerically sorted manual exclusion list for inactive or restricted MLS sources (`mls_status_id != 2`). Managed automatically by pipeline utilities.
- **`raw_targeted_rules.csv`**: Specific rule mutations generated in Stage 4 for Stage 5 input.
- **`triggered_downloads_log_api.csv`**: Operational log generated in Stage 8 storing `batch_id` values used by BigQuery queries in Stage 9.

## 🛡️ Safety Protocols & Troubleshooting

### Dry-Run Verification

Always execute **Stage 5** with `--debug` prior to passing `--do-update` to verify affected row counts against expectations:

```bash
python 5_run_bulk_update.py --input-file temp-data/raw_targeted_rules.csv --debug
```

### Stage Failures & Recovery

- **Stage 6 (Jobs Disabled) Failure:** If pipeline execution halts after Stage 6, ingestion jobs will remain paused. Re-run `9_verify_and_enable_injestion_jobs_api.py` manually to re-enable verified ingestion schedules.
- **Stage 9 Verification Check:** If BigQuery validation fails or returns empty/pending rows, allow additional time for streaming buffer flushes and re-run Stage 9. Do not run Stage 10 until missing landing data is investigated.
