"""
Master Migration Pipeline Orchestrator.

Sequentially executes Stages 0 through 8 with built-in error handling,
dry-run verification gates, and user confirmation prompts.

Usage:
    python run_pipeline.py              # Default run (batch limit: 5)
    python run_pipeline.py --limit 20   # Standard long flag format
    python run_pipeline.py -l 20        # Standard short flag format
"""

import sys
import argparse
import subprocess
from pathlib import Path
from pipeline_logger import setup_logger

logger = setup_logger("MasterPipeline")


def run_stage(script_name: str, args: list[str] = None) -> bool:
    """
    Executes a pipeline script as a subprocess.
    Returns True if successful (exit code 0), False otherwise.
    """
    cmd = [sys.executable, script_name] + (args or [])
    logger.info(f"\n============================================================")
    logger.info(f"🚀 EXECUTING: {' '.join(cmd)}")
    logger.info(f"============================================================")

    result = subprocess.run(cmd)

    if result.returncode != 0:
        logger.error(f"❌ STAGE FAILED: '{script_name}' exited with status code {result.returncode}.")
        return False

    logger.info(f"✅ STAGE PASSED: '{script_name}' completed successfully.\n")
    return True


def prompt_user_confirmation(prompt_text: str) -> bool:
    """Prompts the operator for explicit Y/N confirmation."""
    answer = input(f"\n⚠️  {prompt_text} (y/N): ").strip().lower()
    return answer == "y"


def main() -> None:
    parser = argparse.ArgumentParser(description="Master MLS Rule Migration Pipeline Orchestrator")

    # Strictly accepts --limit and -l
    parser.add_argument(
        "-l", "--limit",
        type=int,
        default=5,
        dest="limit",
        help="Batch limit for Stage 0 (default: 5)"
    )
    parser.add_argument(
        "--auto-approve",
        action="store_true",
        help="Skip interactive prompts (Use with caution!)"
    )
    args = parser.parse_args()

    logger.info("============================================================")
    logger.info("🏁 STARTING AUTOMATED MLS MIGRATION PIPELINE")
    logger.info("============================================================")

    # -------------------------------------------------------------------------
    # STAGE 0: Prepare Batch Targets
    # -------------------------------------------------------------------------
    if not run_stage("0_prepare_batch_targets.py", ["--limit", str(args.limit)]):
        sys.exit(1)

    # Check if target files were generated and non-empty
    test_mls_file = Path("temp-data/test_mls.txt")
    if not test_mls_file.exists() or not test_mls_file.read_text().strip():
        logger.info("🎉 No unmigrated rules/MLSs remaining! Pipeline finished early.")
        sys.exit(0)

    # -------------------------------------------------------------------------
    # STAGE 1: Database Baseline Ingress Sync
    # -------------------------------------------------------------------------
    if not run_stage("1_sync_database_to_repo.py"):
        sys.exit(1)

    # -------------------------------------------------------------------------
    # STAGE 2: PascalCase Standardizer & Blueprint Compiler
    # -------------------------------------------------------------------------
    if not run_stage("2_standardize_rules.py"):
        sys.exit(1)

    # -------------------------------------------------------------------------
    # STAGE 3: Microservice Rule Registration
    # -------------------------------------------------------------------------
    if not run_stage("3_inserting_new_rule.py"):
        sys.exit(1)

    # -------------------------------------------------------------------------
    # STAGE 4: Property Schema Extraction & Targeted Payload Generator
    # -------------------------------------------------------------------------
    if not run_stage("4_generate_targeted_csv.py"):
        sys.exit(1)

    # -------------------------------------------------------------------------
    # STAGE 5: Staging Bulk Database Mutation
    # Step 5A: Dry-Run Mode
    # Step 5B: Live Staging Mutation
    # -------------------------------------------------------------------------
    logger.info("Running Stage 5 in DRY-RUN (--debug) mode for safety verification...")
    if not run_stage("5_run_bulk_update.py", ["--input-file", "raw_targeted_rules.csv", "--debug"]):
        sys.exit(1)

    if not args.auto_approve:
        if not prompt_user_confirmation("Review dry-run output above. Proceed with LIVE Staging DB mutation?"):
            logger.warning("Pipeline aborted by user prior to Staging DB mutation.")
            sys.exit(0)

    if not run_stage("5_run_bulk_update.py", ["--input-file", "raw_targeted_rules.csv", "--do-update"]):
        sys.exit(1)

    # -------------------------------------------------------------------------
    # STAGE 6: Production Environment Promotion
    # -------------------------------------------------------------------------
    if not args.auto_approve:
        if not prompt_user_confirmation("Staging DB updated. Proceed with PRODUCTION promotion?"):
            logger.warning("Pipeline paused before Production deployment.")
            sys.exit(0)

    if not run_stage("6_run_bulk_promotion.py"):
        sys.exit(1)

    # -------------------------------------------------------------------------
    # STAGE 7: Safe Soft Delete via Microservice API
    # -------------------------------------------------------------------------
    if not run_stage("7_soft_delete_legacy_rules.py"):
        sys.exit(1)

    # -------------------------------------------------------------------------
    # STAGE 8: Consolidated Repository Sync, Archival & Ledger Update
    # -------------------------------------------------------------------------
    if not run_stage("8_update_consolidated_repo.py"):
        sys.exit(1)

    logger.info("============================================================")
    logger.info("🎉 PIPELINE BATCH MIGRATION COMPLETE SUCCESSFULLY!")
    logger.info("============================================================")


if __name__ == "__main__":
    main()
