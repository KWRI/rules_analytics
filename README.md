````
# 🚀 MLS Rule Migration Execution Guide

This repository (`rules_analytics`) contains the end-to-end automated pipeline required to standardize legacy rule naming conventions into `PascalCase` across staging and production infrastructure.

## 📋 Prerequisites & System Requirements

Before running the migration pipeline, ensure your system meets the following requirements:

### 1. Requirements & Tools

- **Python 3.12+**
- **Git** (configured with access to Keller Williams internal repositories)
- **Google Cloud SDK** (required for BigQuery landing verification in Stage 9)
- **SSH Client & Keys** with access to staging and production bastion hosts (`bas-stage-cs.data.kw.com` / `sdh.data.kw.com`)
- **Network Access**: Internal network or VPN access required to reach DB instances, microservices, and Jenkins hosts.

### 2. Sibling Repository Dependencies & File System Pinning

This project relies on local sibling repositories referenced in `.env.bulk`. Ensure the following repositories are cloned adjacent to this project directory:

- `eim-slp-tools`
- `eim-utilities-pip`
- `eim-snowflake-id`
- `dm-consolidated-rules`

> ⚠️ **OneDrive File Pinning Requirement:** If your GitHub repositories reside inside a OneDrive-synced folder, ensure all repository folders are set to **"Always keep on this device"** in Windows File Explorer. "Cloud-only" placeholder files cause dynamic module import failures (`OSError: [Errno 22] Invalid argument`).

---

## 📦 Installation & Setup

### 1. Clone the Repository

```bashbash
   git clone git@github.com:kwri/rules_analytics.git
   cd rules_analytics

````

### 2. Set Up a Virtual Environment

   Ensure you have Python 3.12 installed before creating your environment to guarantee compatibility with pre-compiled binary dependencies.

   ```
   # Windows (PowerShell / CMD):
   py -3.12 -m venv .venv

   # macOS / Linux:
   python3.12 -m venv .venv

   # Activate the environment:
   # Windows (PowerShell):
   ..venv\Scripts\Activate.ps1

   # Windows (Git Bash):
   source .venv/Scripts/activate

   # macOS / Linux:
   source .venv/bin/activate

   ```
### 3. Install Dependencies

   ```
   python -m pip install --upgrade pip
   pip install -r requirements.txt

   ```
### 4. Initialize Google Cloud Application Default Credentials (ADC)

   Stage 9 requires authenticated access to BigQuery (`stream-listing-prod`). Initialize ADC via terminal:

   ```
   gcloud auth application-default login

   ```
   *Authenticate in the browser using your corporate account (**`<username>@kw.com`**). This generates local credentials at `%APPDATA%\gcloud\application_default_credentials.json`.*

## ⚙️ Environment Configuration

The pipeline requires two configuration files in the project root: `.env` and `.env.bulk`.

> 📌 **Path Formatting Rule:** All path configurations in `.env` and `.env.bulk` **must use forward slashes (`/`)**, even on Windows, to avoid character escaping errors.

### `.env`


```
# Database Credentials (Staging & Production Verification)
DB_NAME=mls_admin
DB_USER=your_username            # Mandatory: Used for audit trail attribution in API calls & central DB ledger
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

```

### `.env.bulk`


```
MS_HOST=https://stage-ext-ms.data.kw.com/v1.0.3
MS_API_KEY=your_mls_admin_api_key
LOG_LEVEL=DEBUG

# Local Sibling Repository Paths (Must use forward slashes)
TOOLS_REPO_PATH=C:/Users/your.username/Documents/GitHub/eim-slp-tools
UTILS_REPO_PATH=C:/Users/your.username/Documents/GitHub/eim-utilities-pip
SNOWFLAKE_REPO_PATH=C:/Users/your.username/Documents/GitHub/eim-snowflake-id
BULK_MAP_TOOL_PATH=C:/Users/your.username/Documents/GitHub/rules_analytics/bulk_map_tool.py

```

## 🏃 Execution Methods

You can run the migration pipeline either automatically using the master orchestrator script or manually stage-by-stage.

### Method 1: Master Orchestrator (Recommended)

`run_pipeline.py` executes pipeline preparation sequentially (Stages 0–4) and finishes with a mandatory Stage 5 `--debug` dry-run verification pass.

#### 1. Jira Ticket Target Mode (Parallel Developer Alignment)

To process specific MLS sources assigned to a Jira ticket (and avoid collisions with teammates), pass comma-separated IDs via `-a` / `--assigned-mls`:


```
python run_pipeline.py -a 502,162,483

```

*(Alternatively, place `502, 162, 483` inside a root `my_assigned_mls.txt` file and run `python run_pipeline.py`).*

#### 2. Automated Unassigned Batch Mode

Processes up to the top $N$ unmigrated MLS sources per protocol automatically:


```
python run_pipeline.py -l 4

```

#### 3. Automated / CI Non-Interactive Mode

Bypasses interactive prompts across Stage 0 and preparation steps:


```
python run_pipeline.py -a 502,162 --auto-approve

```

#### Command Flags for `run_pipeline.py`

- `-a, --assigned-mls <str>`: Comma-separated MLS IDs for Jira ticket targeting (e.g., `-a 502,162`).
- `-l, --limit <int>`: Batch limit per protocol variant (API / RETS) selected in Stage 0 (Default: `4`).
- `-s, --start-at <int>`: Stage number to resume execution from (0 to 11, Default: `0`).
- `--auto-approve`: Auto-approves interactive prompts across applicable pipeline stages.
- `--wait-mins <int>`: Minutes to wait for BigQuery logs after Stage 8 (Default: `5`).

### Method 2: Manual / Sequential Execution

If you need to execute specific stages individually or inspect CSV mutation schemas before committing database changes:


```
# Stage 0: Batch Target Selection (API & RETS with live Production status & DB Ledger locking)
python 0_prepare_batch_targets_api.py -a 502,162 --limit 4
python 0_prepare_batch_targets_rets.py -a 502,162 --limit 4

# Stage 1: Backup Raw DB Records to Local Git Repository
python 1_sync_database_to_repo.py

# Stage 2: Compile PascalCase Standard Rules & Blueprint Matrix
python 2_standardize_rules.py

# Stage 3: Register PascalCase Rules via Staging Microservice API
python 3_inserting_new_rule.py

# Stage 4: Correlate Targets & Generate Mutation Mapping
python 4_generate_targeted_csv.py

# --- MANUAL INTERVENTION GATE: Inspect & Edit temp-data/raw_targeted_rules.csv ---

# Stage 5: Staging Bulk DB Mutation (Dry-Run First, Then Live Update)
python 5_run_bulk_update.py --input-file temp-data/raw_targeted_rules.csv --debug
python 5_run_bulk_update.py --input-file temp-data/raw_targeted_rules.csv --do-update

# Stage 6: Disable Production Ingestion Jobs (Jenkins & API Service)
python 6_disable_ingestion_jobs.py

# Stage 7: Production Promotion Engine (Remote SDH SSH Runner) & Slack Draft
python 7_run_bulk_promotion.py

# Stage 8: Trigger 3-Day Test Ingestion Reprocessing in Parallel
python 8_trigger_manual_downloads.py

# --- Pause ~5 Minutes to Allow Production BigQuery Data Landing ---

# Stage 9: Verify BigQuery Landing Logs & Re-Enable Verified API Schedules
python 9_verify_and_enable_injestion_jobs_api.py

# Stage 10: Soft Delete Legacy Rules via Stage Microservice API
python 10_soft_delete_legacy_rules.py

# Stage 11: Sync Repository Archival, Finalize DB Ledger & Clean Temporary Files
python 11_update_consolidated_repo.py -y

```

## 🔍 Parameter Inspection Workflow (Manual Mapping Guard)

When mapping legacy rules to standardized microservice rules where parameter signatures may differ (e.g., `Has Pool` → `HasPool`), follow the manual inspection protocol:

1. **Run Stage 5 in Dry-Run Mode:**

   ```
   python 5_run_bulk_update.py --input-file temp-data/raw_targeted_rules.csv --debug

   ```
2. **Inspect & Adjust CSV:**

   Open `temp-data/raw_targeted_rules.csv`. Compare `rule_params` keys against expected microservice parameters (e.g., remapping `features_1` to `features_2`). Save your edits.
3. **Apply Live Update:**

   ```
   python 5_run_bulk_update.py --input-file temp-data/raw_targeted_rules.csv --do-update

   ```
4. **Execute Post-Mutation Stages:**

   Execute Stages 6 through 11 sequentially as documented in Method 2.

## 📋 Script Execution Sequence Reference

| Stage | Script | Purpose |
|---|---|---|
| 0 | `0_prepare_batch_targets_api.py` & `0_prepare_batch_targets_rets.py` | Queries active MLS sources (filtered by optional ticket assignments), locks targets as `IN_PROGRESS` in `public.processed_mls_ledger`, excludes entries in `discrepant_mls.txt`, executes reverse promotion (Prod → Stage), and generates batch files in `temp-data/`. |
| 1 | `1_sync_database_to_repo.py` | Connects via SSH to back up raw DB rule records concurrently to local repository disk storage (`ui-rules/active` and `ui-rules/archived`). |
| 2 | `2_standardize_rules.py` | Transforms rule names to `PascalCase`, resolves versioning collisions (`V1`/`V2`), and outputs `migration_blueprint.csv`. |
| 3 | `3_inserting_new_rule.py` | Registers newly standardized rules in Staging via `POST /v1/mls-admin/rules`. |
| 4 | `4_generate_targeted_csv.py` | Traverses JSON property maps, correlates targets, and generates `raw_targeted_rules.csv` and `promotion_sources.txt`. Fast-forwards no-op batches directly to `processed_mls_ledger.json`. |
| 5 | `5_run_bulk_update.py` | Applies bulk schema updates directly to the Staging database via microservice API handoff (`bulk_map_tool`). |
| 6 | `6_disable_ingestion_jobs.py` | Disables Production Jenkins jobs (RETS) and API ingestion schedules, filtering strictly against `promotion_sources.txt`. |
| 7 | `7_run_bulk_promotion.py` | Generates a formatted Slack notification draft via `draft_slack_message.py` (copied to clipboard), uploads deployment manifests via SFTP to `sdh.data.kw.com`, and executes remote promotion (`s_p`). |
| 8 | `8_trigger_manual_downloads.py` | Enables required ingestion schedules and triggers parallel 3-day reprocessing ranges in Production for promoted MLS sources. |
| 9 | `9_verify_and_enable_injestion_jobs_api.py` | Queries BigQuery landing tables (`mls_download.*`) via ADC by `batch_id`, validates row counts, and restores schedules for clean MLS targets. |
| 10 | `10_soft_delete_legacy_rules.py` | Checks if legacy rules are still used by unmigrated MLSs, soft-deleting only fully unlinked rules via Stage API. |
| 11 | `11_update_consolidated_repo.py` | Moves soft-deleted rules to `archived/`, updates `public.processed_mls_ledger` status to `COMPLETED`, updates local `processed_mls_ledger.json`, purges old logs (>3 days), and cleans `temp-data/`. |

## 📂 Key State & Exclusion Files

| File / Resource | Purpose |
|---|---|
| `public.processed_mls_ledger` (Database Table) | Central PostgreSQL ledger tracking locked (`IN_PROGRESS`) and finalized (`COMPLETED`) MLS IDs (`mls_id` and `mls_id_str`) across concurrent developer runs. |
| `my_assigned_mls.txt` | Optional root-level text file containing specific MLS IDs for Jira ticket targeting. |
| `discrepant_mls.txt` | Root-level exclusion list containing environmental discrepancy IDs and inline notes, dynamically loaded in `rules_utils.py` and excluded during Stage 0 target selection. |
| `temp-data/processed_mls_ledger.json` | Local ledger tracking all fully migrated MLS IDs for offline continuity and local pipeline resume checks. |
| `skip_mls.txt` | Numerically sorted manual exclusion list for inactive or restricted MLS sources (`mls_status_id != 2`). |
| `temp-data/promotion_sources.txt` | Manifest containing numeric MLS IDs that contain active mutations for forward promotion. |
| `temp-data/raw_targeted_rules.csv` | Specific rule mutations generated in Stage 4 for Stage 5 input. |
| `temp-data/triggered_downloads_log_api.csv` | Operational log generated in Stage 8 storing `batch_id` values used by BigQuery queries in Stage 9. |
| `temp-data/reverse_targeted_rules.csv` | Emergency rollback mutation file generated by `revert_mapping_to_old_rules.py` to restore legacy string formats if needed. |

## 🛡️ Safety Protocols & Rollback Utilities

### Emergency Rollback (`revert_mapping_to_old_rules.py`)

If a live bulk update applied via Stage 5 needs to be rolled back in Staging, convert the standardized `raw_targeted_rules.csv` back to legacy string formats:


```
# 1. Generate reverse mapping CSV
python revert_mapping_to_old_rules.py

# 2. Apply reverse update to Staging DB
python 5_run_bulk_update.py --input-file temp-data/reverse_targeted_rules.csv --do-update

```

### Signal Handling & Instant Aborts

All stages and orchestrator entrypoints are equipped with OS-level `SIGINT`/`SIGBREAK` signal handlers (`os._exit(1)`). Pressing `Ctrl+C` at any point immediately terminates all thread pools and socket connections without hanging.

### Troubleshooting Reference

Refer to the official internal operational documentation for troubleshooting common connection timeouts or microservice key issues:

- [ProdOps Rules Cleanup Project Guide](https://kwri.atlassian.net/wiki/spaces/ProdOps/pages/3225518104/Errors%2Bregarding%2BRules%2BCleanup%2BProject)
