# 🚀 MLS Rule Migration & Standardization Framework

A modular, multi-stage database migration engine designed to standardize legacy rule naming conventions into **PascalCase** across staging and production infrastructure. The framework automates database extraction, case-insensitive translation compilation, microservice API registration, property schema tree transformation, bulk database mutation, production deployment, and intelligent ledger-backed git repository archiving.

---

## 🏛️ System Architecture & Workflow Pipeline

The migration pipeline operates in **9 sequential stages**, controlled via isolated execution scripts that communicate using file-based matrix assets in `temp-data/` and record system events into rotating logs (`logs/pipeline_YYYY-MM-DD.log`).

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
[Stage 6: Prod Promotion]    ──► SFTP/SSH stream execution to Production nodes
         │
         ▼
[Stage 7: Safe Soft Delete]  ──► DELETEs legacy rules via API (Active usage guard enabled)
         │
         ▼
[Stage 8: Repo Archival]     ──► Moves .py files to ui-rules/archived & updates ledger.json
```

---

## 📋 Pipeline Stages Summary

| Stage | Script Name | Core Responsibilities |
| :---: | :--- | :--- |
| **`0`** | `0_prepare_batch_targets.py` | Queries DB for unmigrated legacy rules. Groups by MLS ID and writes `test_mls.txt` and `test_rules.txt` sorted in strict ascending numeric order. |
| **`1`** | `1_sync_database_to_repo.py` | Establishes an SSH tunnel to Staging PostgreSQL to stream and back up raw rule records to disk concurrently. |
| **`2`** | `2_standardize_rules.py` | Compiles legacy rule names into PascalCase and outputs `temp-data/migration_blueprint.csv`. |
| **`3`** | `3_inserting_new_rule.py` | Registers newly standardized rules in Staging via `POST /v1/mls-admin/rules`. |
| **`4`** | `4_generate_targeted_csv.py` | Traverses property schema trees in DB to map old rules to new ones, outputting `raw_targeted_rules.csv` and `promotion_sources.txt`. |
| **`5`** | `5_run_bulk_update.py` | Executes bulk field and rule schema mutations in Staging DB using `eim-slp-tools` (supports `--debug` dry-run and `--do-update`). |
| **`6`** | `6_run_bulk_promotion.py` | Uploads `promotion_sources.txt` via SFTP and invokes interactive remote shell deployment commands on Production server. |
| **`7`** | `7_soft_delete_legacy_rules.py` | Verifies active usage across unmigrated MLSs over SSH tunnel, then soft-deletes fully unlinked rules via `DELETE /v1/mls-admin/rules`. |
| **`8`** | `8_update_consolidated_repo.py` | Moves soft-deleted `.py` rule files from `ui-rules/active/` to `ui-rules/archived/`, updates `processed_mls_ledger.json`, and cleans up `temp-data/`. |

---

## ⚙️ Prerequisites & Environment Setup

### 1. Dependencies

Ensure Python 3.10+ is installed along with the required libraries:

```bash
pip install psycopg2-binary paramiko sshtunnel python-dotenv pandas requests cryptography pydantic pydantic-settings
```

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

# Remote Production Server Promotion Configuration (Stage 6)
REMOTE_HOST="prod-gateway.yourdomain.com"
REMOTE_DIR="/opt/migrations/promotions"
```

#### Bulk Tool Configuration (`.env.bulk`)

Create `.env.bulk` for Stage 5 bulk update dependencies:

```env
TOOLS_REPO_PATH="/path/to/eim-slp-tools"
UTILS_REPO_PATH="/path/to/eim-slp-utils"
SNOWFLAKE_REPO_PATH="/path/to/eim-snowflake-id"
```

---

## 🏃 Execution Guide

### Automated Batch Workflow (Stages 0–8)

Run the stages sequentially from your workspace terminal:

```bash
# Stage 0: Generate batch targets (default limit: 5 MLSs)
python 0_prepare_batch_targets.py

# (Optional) Override batch limit at runtime:
# python 0_prepare_batch_targets.py --limit 2

# Stage 1: Backup raw database baseline
python 1_sync_database_to_repo.py

# Stage 2: Compile PascalCase blueprint
python 2_standardize_rules.py

# Stage 3: Register new rules in Staging via API
python 3_inserting_new_rule.py

# Stage 4: Extract targeted mapping CSV & promotion manifest
python 4_generate_targeted_csv.py

# Stage 5: Dry-run simulation (verify before mutating)
python 5_run_bulk_update.py --input-file raw_targeted_rules.csv --debug

# Stage 5: Live bulk mutation in Staging DB
python 5_run_bulk_update.py --input-file raw_targeted_rules.csv --do-update

# Stage 6: Promote changes to Production
python 6_run_bulk_promotion.py

# Stage 7: Soft-delete fully unlinked legacy rules via API
python 9_soft_delete_legacy_rules.py

# Stage 8: Archive repository files & update processed ledger
python 10_update_consolidated_repo.py
```

---

## 🎯 Batch Processing & Shared Rule Safeguards

### Automated Batch Tracking (Stages 0 & 8)

Stage 0 checks `temp-data/processed_mls_ledger.json` to automatically skip MLS sources processed in previous runs. Stage 8 appends newly completed MLS IDs to `processed_mls_ledger.json` upon successful batch completion.

### Active Reference Safety Guard (Stages 7 & 8)

When multiple MLS sources share a single legacy rule (e.g., `Timezones_US/Eastern` used across 50 MLSs), migrating a subset of 5 MLSs must not delete or archive the shared rule while the other 45 MLSs still depend on it.

Stage 7 queries active database process maps over SSH. If a legacy rule is still referenced by an unmigrated MLS, soft-deletion is skipped. Stage 8 checks database deletion status and only archives `.py` files that have been soft-deleted in the database — shared active rules remain safely in `ui-rules/active/`.

---

## 🛠️ Manual / Ad-Hoc Target Overrides

To run a specific set of rules or MLSs without using automated Stage 0 selection:

1. Skip `0_prepare_batch_targets.py`.
2. Manually populate your target files in `temp-data/`:
   - `temp-data/test_rules.txt` — legacy rule names, one per line
   - `temp-data/test_mls.txt` — target MLS IDs, one per line
3. Execute Stages 1 through 8 as normal.

---

## 📊 Logging Infrastructure

All operations write daily structured logs to `logs/pipeline_YYYY-MM-DD.log` while simultaneously displaying console output via `pipeline_logger.py`:

```
[2026-07-30 12:43:41] [INFO] [Stage0_BatchPrepare] Loaded 0 previously processed MLS ID(s) from ledger.
[2026-07-30 12:43:41] [INFO] [Stage0_BatchPrepare] Target MLS batch size: 20
[2026-07-30 12:43:43] [INFO] [Stage0_BatchPrepare] 🎯 AUTOMATED BATCH SELECTION COMPLETE
[2026-07-30 12:43:43] [INFO] [Stage0_BatchPrepare] Target MLS IDs (20): 20, 24, 30, 31, 32, 34, ...
[2026-07-30 12:43:43] [INFO] [Stage0_BatchPrepare] Target Legacy Rules (6): 34PropSubTypeHandler, Timezones_US/Arizona, ...
```

Stage 5 additionally writes low-level mutation diffs directly to `bulk_map_tool.log`.