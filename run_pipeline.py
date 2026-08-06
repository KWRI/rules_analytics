"""
Master Migration Pipeline Orchestrator.

Sequentially executes Stages 0 through 10 with built-in error handling,
dry-run verification gates, timing pause prompts, and user confirmation steps.

Pipeline Flow:
    Stage 0:  Prepare Batch Targets
    Stage 1:  Database Baseline Ingress Sync
    Stage 2:  PascalCase Standardizer & Blueprint Compiler
    Stage 3:  Microservice Rule Registration
    Stage 4:  Property Schema Extraction & Targeted Payload Generator
    Stage 5:  Staging Bulk Database Mutation (Dry-Run -> Live)
    Stage 6:  Production Environment Promotion (Job Disable + Promo + Scheduler Sync)
    Stage 7:  Trigger Test Downloads (3-Day Range)
    --- WAIT / PAUSE GATE (Configurable delay for Download Processing) ---
    Stage 8:  BigQuery Log Verification & Selective Job Enablement
    Stage 9:  Safe Soft Delete via Microservice API
    Stage 10: Consolidated Repository Sync, Archival & Ledger Update

Usage:
    python run_pipeline.py                       # Default run (batch limit: 5, manual prompts)
    python run_pipeline.py --limit 20            # Standard long flag format
    python run_pipeline.py --auto-approve        # Fully automated CI/CD mode
    python run_pipeline.py --auto-approve --wait-mins 15 # Automated with specific wait time
"""

import sys
import time
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
    # Ensure Windows console handles UTF-8 emojis cleanly
    if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

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
    parser.add_argument(
        "--wait-mins",
        type=int,
        default=15,
        help="Minutes to wait for BigQuery logs after Stage 7 (default: 15)"
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
    # STAGE 7: Trigger Manual Test Downloads (Production)
    # -------------------------------------------------------------------------
    if not run_stage("7_trigger_manual_downloads.py"):
        sys.exit(1)

    # -------------------------------------------------------------------------
    # INTERACTIVE PAUSE GATE: WAIT FOR DOWNLOAD PROCESSING
    # -------------------------------------------------------------------------
    logger.info("============================================================")
    logger.info("⏳ PRODUCTION DOWNLOADS TRIGGERED IN STAGE 7")
    logger.info(f"   Downloads typically take up to {args.wait_mins} minutes to process in BigQuery.")
    logger.info("============================================================")

    if args.auto_approve:
        wait_seconds = args.wait_mins * 60
        logger.info(f"🤖 Auto-approve mode active. Automatically sleeping for {args.wait_mins} minutes...")

        for remaining in range(wait_seconds, 0, -60):
            logger.info(f"⏳ Time remaining: {remaining // 60} minute(s)...")
            time.sleep(60)

        logger.info("✅ Wait complete. Proceeding to Stage 8 log verification.")
    else:
        if not prompt_user_confirmation(f"Have you allowed ~{args.wait_mins} minutes for downloads to complete? Ready to verify BigQuery logs (Stage 8)?"):
            logger.warning("Pipeline paused by user prior to Stage 8 log verification.")
            logger.info("💡 You can manually run 'python 8_verify_and_enable_jobs.py' when ready.")
            sys.exit(0)

    # -------------------------------------------------------------------------
    # STAGE 8: BigQuery Log Verification & Selective Job Enablement
    # -------------------------------------------------------------------------
    if not run_stage("8_verify_and_enable_jobs.py"):
        logger.error("⛔ Stage 8 detected errors in BigQuery logs or failed execution. Pipeline stopped.")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # STAGE 9: Safe Soft Delete via Microservice API
    # -------------------------------------------------------------------------
    if not run_stage("9_soft_delete_legacy_rules.py"):
        sys.exit(1)

    # -------------------------------------------------------------------------
    # STAGE 10: Consolidated Repository Sync, Archival & Ledger Update
    # -------------------------------------------------------------------------
    if not run_stage("10_update_consolidated_repo.py"):
        sys.exit(1)

    logger.info("============================================================")
    logger.info("🎉 PIPELINE BATCH MIGRATION COMPLETE SUCCESSFULLY!")
    logger.info("============================================================")


if __name__ == "__main__":
    main()
