# 🚀 Rules Analytics & Automation Pipeline

An automated, end-to-end framework for standardizing, registering, updating, and promoting property data mapping rules across Staging and Production environments.

---

## 📋 Table of Contents
- [Prerequisites & Sibling Repositories](#-prerequisites--sibling-repositories)
- [Local Machine Setup](#-local-machine-setup)
- [Directory Structure](#-directory-structure)
- [Batch Testing Configuration](#-batch-testing-configuration)
- [Execution Lifecycle (Stage-by-Stage)](#-execution-lifecycle-stage-by-stage)
- [Troubleshooting & Common Edge Cases](#-troubleshooting--common-edge-cases)
- [Best Practices & Safety Precautions](#-best-practices--safety-precautions)

---

## 📋 Prerequisites & Sibling Repositories

Before setting up the repository, ensure your environment meets the following software requirements:

* **Python 3.10+** (Python 3.11 recommended)
* **Git** installed and configured
* Active credentials/tokens for internal rule microservices and database environments

### Required Sibling Repositories

This project relies on underlying core libraries and bulk update engines. Ensure the following sibling repositories are cloned into the **same parent directory** alongside `rules_analytics`:

```text
parent_folder/
├── rules_analytics/           <-- (This repo)
├── dm-consolidated-rules/     <-- Consolidated rule definitions
├── eim-mapex/                 <-- Mapex schema mapping dependencies
├── eim-slp-tools/             <-- Execution tools & bulk update engine
├── eim-snowflake-id/          <-- Snowflake ID utilities
├── eim-utilities-pip/         <-- Internal pipeline utility package
└── mapping-slp-rules/         <-- Mapping rules configuration repository
```

To clone these dependencies into your workspace parent folder:

```powershell
git clone https://github.com/your-org/dm-consolidated-rules.git
git clone https://github.com/your-org/eim-mapex.git
git clone https://github.com/your-org/eim-slp-tools.git
git clone https://github.com/your-org/eim-snowflake-id.git
git clone https://github.com/your-org/eim-utilities-pip.git
git clone https://github.com/your-org/mapping-slp-rules.git
```

---

## 🛠️ Local Machine Setup

### 1. Clone the Repository
```powershell
git clone https://github.com/your-org/rules_analytics.git
cd rules_analytics
```

### 2. Set Up a Virtual Environment

#### On Windows (PowerShell):
```powershell
python -m venv .venv
.\.venv\Scripts\Activate
```

#### On macOS / Linux:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies
```powershell
pip install --upgrade pip
pip install -r requirements.txt
```

*(If dependent sibling packages need to be installed in editable mode, run `pip install -e ..\eim-utilities-pip` and `pip install -e ..\eim-slp-tools`).*

---

## 📂 Directory Structure

```text
rules_analytics/
├── .venv/                      # Python virtual environment
├── eim_snowflake_id/           # Snowflake ID utilities & configs
├── temp-data/                  # Internal runtime manifests & batch state (Git-ignored)
│   ├── migration_blueprint.csv # Output: Old rule -> PascalCase mapping registry
│   ├── promotion_sources.txt   # Output: Verified list of MLS targets for production
│   ├── raw_targeted_rules.csv  # Output: Generated CSV payload for staging update
│   ├── reverse_targeted_rules.csv # Output: Rollback / reverse target payload
│   ├── test_mls.txt            # Input: Target MLS IDs to process
│   └── test_rules.txt          # Input: Target rule names to process
├── .env                        # Local environment variables
├── .env.bulk                   # Bulk update tool configuration variables
├── .gitignore                  # Git exclusion rules
├── 1_sync_database_to_repo.py  # Stage 1: Backup & DB baseline pull
├── 2_standardize_rules.py     # Stage 2: PascalCase rule compiler
├── 3_inserting_new_rule.py    # Stage 3: Microservice rule registration
├── 4_generate_targeted_csv.py # Stage 4: Architectural configuration extractor
├── 5_run_bulk_update.py        # Stage 5: Staging bulk updates (Dry-Run & Live)
├── 6_run_bulk_promotion.py     # Stage 6: Production deployment engine
├── identify_redundant_logic.py # Utility: Redundancy detection analyzer
├── README.md                   # Repository documentation
├── requirements.txt            # Project dependencies
├── revert_mapping_to_old_rules.py # Utility: Rollback mapping tool
└── rules_utils.py              # Shared pipeline utility module for batch configurations
```

---

## 🧪 Batch Testing Configuration

By default, the pipeline operates in **Batch Testing Mode** when specific target files are provided in `temp-data/`. This prevents unintended updates to non-target rules or MLS sources.

Create the target files inside `temp-data/`:

### 1. Target Rules File (`temp-data/test_rules.txt`)
Add the exact rule names you wish to compile/standardize (one per line):
```text
Has Deck
Has Patio
Has Porch
```

### 2. Target MLS IDs File (`temp-data/test_mls.txt`)
Add the specific MLS IDs you want to target (one per line):
```text
162
434
```

> 💡 **Note:** If these test files are omitted or empty, the execution scripts will default to processing the full dataset across the entire database.

---

## 🔄 Execution Lifecycle (Stage-by-Stage)

Follow this 6-stage operational pipeline whenever migrating, updating, or promoting rules:

### Stage 1: Sync Database Baseline
Backs up all active and archived database rule definitions to local disk for diff tracking and rollback safety.
```powershell
python .\1_sync_database_to_repo.py
```
* **Expected Output:** `Total Unique Files Written to Disk: XXXX`

---

### Stage 2: Standardize Rules (PascalCase Compiler)
Reads `test_rules.txt`, compiles targeting rules into standard **PascalCase** filenames (e.g., `Has Deck` -> `HasDeck`), and creates the transformation blueprint `temp-data/migration_blueprint.csv`.
```powershell
python .\2_standardize_rules.py
```
* **Expected Output:** `Mode: BATCH TEST (N rules)` and confirmation of `migration_blueprint.csv` creation.

---

### Stage 3: Register Rules via Microservice
Calls the rule registration microservice API to register the newly compiled PascalCase rules into the system catalog.
```powershell
python .\3_inserting_new_rule.py
```
* **Expected Output:** Reports newly created rules vs. pre-existing skipped rules (`ℹ️ Skipped (Already exists)`).

---

### Stage 4: Generate Targeted Configuration Manifests
Scans JSON property mapping schemas for the target MLS sources specified in `test_mls.txt`, updates field path rule references to PascalCase, and generates output payloads.
```powershell
python .\4_generate_targeted_csv.py
```
* **Key Artifacts Generated:**
  * `temp-data/raw_targeted_rules.csv`: The complete payload mapping.
  * `temp-data/promotion_sources.txt`: The isolated list of affected MLS IDs.

---

### Stage 5: Apply Staging Updates

#### Step 5A: Execute Dry-Run Mode Validation (Mandatory)
Always perform a dry-run check before mutating the staging environment:
```powershell
python .\5_run_bulk_update.py --input-file raw_targeted_rules.csv --debug
```
* **Verification:** Check console logs for `🧪 [STAGING BULK UPDATE GUARD]` and review field mutation diffs.

#### Step 5B: Apply Live Staging Update
Once dry-run outputs are validated:
```powershell
python .\5_run_bulk_update.py --input-file raw_targeted_rules.csv --do-update
```
* **Expected Output:** `updated: N, error: 0, warning: 0`

---

### Stage 6: Promote Changes to Production
Promotes the verified staging mapping configurations for all active MLS targets listed in `promotion_sources.txt` to Production.
```powershell
python .\6_run_bulk_promotion.py
```
* **Verification:** Ensure log displays `🧪 [PROMOTION BATCH GUARD]` confirming active MLS targets before final execution.

---

## ⚡ Troubleshooting & Common Edge Cases

### 1. `ImportError: cannot import name 'RunStats' from 'utils'`
* **Cause:** Name collision between internal repo `utils` package and a generic `utils.py` script.
* **Fix:** Ensure the project utility module is named `rules_utils.py` and imported as `from rules_utils import load_target_test_mls`.

### 2. Rule Already Exists in Database (Stage 3)
* **Behavior:** Log will indicate `Skipped / Already Existing Rules: N`.
* **Action:** This is normal and expected behavior. Proceed directly to Stage 4.

### 3. PascalCase Naming Convention Reference
* `Has Deck` -> `HasDeck`
* `Schools (Bridge)` -> `SchoolsBridge`
* `470HOA` -> `470Hoa`
* `518ListStatusID?` -> `518ListStatusId`
* `586_parking_features?` -> `586ParkingFeatures`

---

## 🛡️ Best Practices & Safety Precautions

1. **Always verify sibling repositories are up to date.** Run `git pull` across all sibling directories before kicking off a bulk migration.
2. **Always run Stage 5 with `--debug` first.** Never execute `--do-update` without inspecting the dry-run output.
3. **Inspect `promotion_sources.txt` before Stage 6.** Verify that only intended MLS IDs are queued for deployment.
4. **Do not commit `temp-data/` artifacts.** Keep runtime manifests in `.gitignore` to avoid pollution of source control.
