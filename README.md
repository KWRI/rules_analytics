# 🚀 MLS Rule Migration & Standardization Framework

A modular, multi-stage database migration engine designed to standardize legacy rule naming conventions into `PascalCase` across staging and production infrastructure. The framework automates database extraction, case-insensitive translation compilation, microservice API registration, property schema tree transformation, bulk database mutation, production deployment, telemetry auditing, and intelligent ledger-backed Git repository archiving.

---

## 🏛️ System Architecture & Workflow Pipeline

The migration pipeline operates in 11 sequential stages (`0` to `10`), orchestrated automatically via `run_pipeline.py`. The stages communicate using file-based matrix assets in `temp-data/` and record system events into rotating logs (`logs/pipeline_YYYY-MM-DD.log`).

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                 PIPELINE FLOW DIAGRAM                                  │
└────────────────────────────────────────────────────────────────────────────────────────┘

[Stage 0: Batch Target Prep] ──► Generates test_mls.txt & test_rules.txt
         │
         ▼
[Stage 1: DB Ingress Sync]   ──► Syncs raw DB rule records to disk baseline
         │
         ▼
[Stage 2: Pascal Compiler]   ──► Standardizes names & writes migration_blueprint.csv
         │
         ▼
[Stage 3: Microservice Reg]  ──► POSTs new PascalCase rules via MLS Admin API
         │
         ▼
[Stage 4: Schema Extractor]  ──► Traverses JSON properties & outputs raw_targeted_rules.csv
         │
         ▼
[Stage 5: Bulk DB Staging]   ──► Mutates JSON configuration nodes in Staging DB
         │
         ▼
[Stage 6: Prod Promotion]    ──► Disables active jobs, SFTPs manifest & executes Prod updates
         │
         ▼
[Stage 7: Test Downloads]    ──► Triggers a time-bounded (3-day) data sync in Production
         │
         ▼
[Stage 8: Log Verification]  ──► Audits BigQuery logs; Enables clean jobs, isolates failing jobs
         │
         ▼
[Stage 9: Safe Soft Delete]  ──► DELETEs legacy rules via API (Active usage guard enabled)
         │
         ▼
[Stage 10: Repo Archival]    ──► Moves .py files to ui-rules/archived & updates ledger.json
```

---

## 📋 Pipeline Stages Summary

| Stage | Script Name | Core Responsibilities |
| :---: | :--- | :--- |
| **0** | `0_prepare_batch_targets.py` | Queries DB for unmigrated legacy rules. Groups by MLS ID and writes target files sorted in strict ascending numeric order. |
| **1** | `1_sync_database_to_repo.py` | Establishes an SSH tunnel to Staging PostgreSQL to stream and back up raw rule records to disk concurrently. |
| **2** | `2_standardize_rules.py` | Compiles legacy rule names into PascalCase, resolves naming collisions (e.g., `rule_1` vs `Rule_1` via `V1`/`V2` versioning), and outputs `temp-data/migration_blueprint.csv`. |
| **3** | `3_inserting_new_rule.py` | Registers newly standardized rules in Staging via `POST /v1/mls-admin/rules`. |
| **4** | `4_generate_targeted_csv.py` | Traverses property schema trees in DB to map old rules to new ones, outputting targeted CSVs and promotion manifests. |
| **5** | `5_run_bulk_update.py` | Executes bulk field and rule schema mutations in Staging DB using `eim-slp-tools` (supports `--debug` dry-run). |
| **6** | `6_run_bulk_promotion.py` | Disables Prod jobs, syncs scheduler, uploads manifest via SFTP, and invokes remote shell deployment commands. |
| **7** | `7_trigger_manual_downloads.py` | Triggers a narrow, 3-day reprocessing download range in Production to test the new mappings. |
| **8** | `8_verify_and_enable_jobs.py` | Checks BigQuery logs per individual MLS. Re-enables jobs for clean MLSs, keeps failing MLSs disabled, and generates debugging SQL queries. |
| **9** | `9_soft_delete_legacy_rules.py` | Verifies active usage across unmigrated MLSs, then soft-deletes fully unlinked rules via API. |
| **10** | `10_update_consolidated_repo.py` | Moves soft-deleted `.py` rules to `archived/`, updates `processed_mls_ledger.json`, and cleans `temp-data/`. |

---

## ⚙️ Prerequisites & Environment Setup

### 1. Dependencies

Ensure **Python 3.10+** is installed along with the required libraries:

```bash
pip install psycopg2-binary paramiko sshtunnel python-dotenv pandas requests cryptography pydantic pydantic-settings google-cloud-bigquery
```

> **Note:** Use `psycopg2-binary` instead of `psycopg2` for easier installation on macOS/Linux.

### 2. Configuration Files

#### Primary Configuration (`.env`)
Create a `.env` file in the project root:

```env
# Repository & User Identification
REPO_PATH="/path/to/dm-consolidated-rules"
CREATED_BY_USER="your_username"

# SSH Infrastructure Tunnel Configuration
SSH_HOST="stage-ssh.yourdomain.com"
SSH_USER="ssh_user"
SSH_KEY_PATH="~/.ssh/id_rsa"
SSH_KEY_PASSPHRASE="your_key_passphrase"

# Staging PostgreSQL Configuration
DB_HOST="staging-db.internal"
DB_NAME="mls_staging_db"
DB_USER="db_user"
DB_PASSWORD="db_password"

# MLS Admin Microservice API Configuration
MLS_ADMIN_STAGE_URL="https://stage-ext-ms.data.kw.com/v1/mls-admin"
MLS_ADMIN_API_KEY="your_admin_api_key"

# Production Promotion & Verification Configuration
PROD_MS_URL="https://prod-ext-ms.data.kw.com"
BIGQUERY_PROJECT="data-shared-prod-44e4"
REMOTE_SSH_HOST="prod-gateway.yourdomain.com"
REMOTE_SSH_USER="ssh_user"
REMOTE_SSH_KEY_PATH="~/.ssh/id_rsa"
REMOTE_MANIFEST_PATH="/opt/migrations/promotions/promotion_sources.txt"
```

#### Bulk Tool Configuration (`.env.bulk`)
Create `.env.bulk` for Stage 5 bulk update dependencies (isolates environment variables for safety):

```env
TOOLS_REPO_PATH="/path/to/eim-slp-tools"
UTILS_REPO_PATH="/path/to/eim-slp-utils"
SNOWFLAKE_REPO_PATH="/path/to/eim-snowflake-id"
```

---

## 🏃 Execution Guide

The primary way to execute this pipeline is via the master orchestrator, `run_pipeline.py`. It handles sequential execution, dry-run safety gates, and BigQuery latency waits automatically.

### 1. Interactive / Developer Mode
Runs the pipeline with a default batch limit of 5 MLSs. It will pause to ask for explicit `(y/N)` confirmation before mutating staging databases, promoting to production, and executing BigQuery log checks:

```bash
python run_pipeline.py
```

### 2. Automated CI/CD Mode
Use the `--auto-approve` flag to run the pipeline headlessly without human intervention:

```bash
# Process 20 MLS targets, auto-approve all gates, and wait 15 minutes for BigQuery logs
python run_pipeline.py --limit 20 --auto-approve --wait-mins 15
```

### Orchestrator Command Flags
* `-l, --limit <int>`: Number of MLS sources to process in this batch *(Default: `5`)*.
* `--auto-approve`: Skips all interactive `Y/N` prompts.
* `--wait-mins <int>`: Minutes to sleep after Stage 7 before querying BigQuery in Stage 8 *(Default: `15`)*.

---

## 🎯 Pipeline Safeguards & Fault Isolation

### Granular Fault Isolation (Stage 8)
When test downloads are triggered in Production, the pipeline evaluates logs per individual MLS. If one MLS vendor throws a mapping error:
* Only that vendor's download job remains **disabled**.
* Clean, successful MLS jobs in the batch are **automatically re-enabled**.
* The pipeline generates targeted debugging SQL queries in `temp-data/investigate_errors.sql` for quick investigation.

### Active Reference Safety Guard (Stages 9 & 10)
When multiple MLS sources share a single legacy rule (e.g., `Timezones_US/Eastern` used across 50 MLSs), migrating a subset of 5 MLSs must not delete or archive the shared rule:
* **Stage 9** queries active database process maps. If a legacy rule is still referenced by an unmigrated MLS, soft-deletion is skipped.
* **Stage 10** only archives `.py` files that have been successfully soft-deleted in the database, ensuring shared rules remain safely active.

### Automated Batch Tracking (Stages 0 & 10)
* **Stage 0** checks `temp-data/processed_mls_ledger.json` to automatically skip MLS sources processed in previous runs.
* **Stage 10** appends newly completed MLS IDs to the ledger upon successful batch completion.

---

## 🛠️ Manual / Ad-Hoc Target Overrides

To run a specific set of rules or MLSs without using automated Stage 0 selection:

1. Skip `0_prepare_batch_targets.py` or comment it out in the orchestrator.
2. Manually populate your target files in `temp-data/`:
   * `temp-data/test_rules.txt` — legacy rule names, one per line.
   * `temp-data/test_mls.txt` — target MLS IDs, one per line.
3. Execute the pipeline or standalone stage scripts directly:
   ```bash
   python enable_download_jobs.py -m 120 123  # Manually re-enable clean MLS IDs
   ```

---

## 📊 Logging Infrastructure

All operations write daily structured logs to `logs/pipeline_YYYY-MM-DD.log` while simultaneously displaying console output:

```text
[2026-08-06 22:43:41] [INFO] [Stage0_BatchPrepare] Target MLS batch size: 20
[2026-08-06 22:43:43] [INFO] [Stage0_BatchPrepare] 🎯 AUTOMATED BATCH SELECTION COMPLETE
[2026-08-06 22:43:43] [INFO] [Stage0_BatchPrepare] Target MLS IDs (20): 20, 24, 30, 31...
...
[2026-08-06 22:45:10] [INFO] [MasterPipeline] 🤖 Auto-approve mode active. Sleeping for 15 minutes...
[2026-08-06 23:00:10] [INFO] [Stage8_VerifyAndEnable] ✅ CLEAN MLS TARGETS (19): austin_tx, dallas_tx...
[2026-08-06 23:00:12] [ERROR] [Stage8_VerifyAndEnable] ❌ FAILED MLS TARGETS (1): houston_tx
```

*Note: Stage 5 additionally writes low-level mutation diffs directly to `bulk_map_tool.log`.*
