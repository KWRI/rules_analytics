"""
Master Migration Pipeline Orchestrator.

Sequentially executes Stages 0 through 11 with built-in error handling,
dry-run verification gates, timing pause prompts, and user confirmation steps.

Pipeline Flow:
    Stage 0   : Target Selection & Reverse Promotion (API & RETS -> Prod to Stage Sync)
    Stage 1   : Core Database Ingress Backup
    Stage 2   : Rule Standardization & Blueprint Compilation
    Stage 3   : Microservice Rule Registration
    Stage 4   : Targeted Mutation CSV & Manifest Generation
    Stage 5   : Staging Bulk Update
    Stage 6   : Disable Ingestion Jobs
    Stage 7   : Draft Slack Notification & Production Promotion
    Stage 8   : Re-enable Jobs & Trigger Manual Downloads
    Stage 9   : Verify Ingestion & Re-enable API Jobs
    Stage 10  : Safe Soft Delete Legacy Rules
    Stage 11  : Repository Archival & Ledger Update
"""

import sys
import time
import argparse
import subprocess
from pathlib import Path
from pipeline_logger import setup_logger

logger = setup_logger("MasterPipeline")


def run_stage(script_name: str, args: list[str] = None) -> bool:
    """Executes a pipeline script as a subprocess."""
    cmd = [sys.executable, script_name] + (args or [])
    logger.info("============================================================")
    logger.info(f"🚀 EXECUTING: {' '.join(cmd)}")
    logger.info("============================================================")

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
    if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    parser = argparse.ArgumentParser(description="Master MLS Rule Migration Pipeline Orchestrator")

    parser.add_argument(
        "-l", "--limit",
        type=int,
        default=4,
        dest="limit",
        help="Batch limit for Stage 0 (default: 4)"
    )
    parser.add_argument(
        "-s", "--start-at",
        type=int,
        default=0,
        dest="start_at",
        help="Stage number to start execution from (0 to 11, default: 0)"
    )
    parser.add_argument(
        "--auto-approve",
        action="store_true",
        help="Skip interactive prompts for non-interactive execution"
    )
    parser.add_argument(
        "--wait-mins",
        type=int,
        default=5,
        help="Minutes to wait for BigQuery logs after Stage 8 (default: 5)"
    )
    args = parser.parse_args()

    start_at = args.start_at

    logger.info("============================================================")
    logger.info("🏁 STARTING AUTOMATED MLS MIGRATION PIPELINE")
    if start_at > 0:
        logger.info(f"   ⏩ Resuming execution from Stage {start_at}")
    logger.info("============================================================")

    # ------------------------------------------------------------
    # STAGE 0: Prepare Batch Targets (API & RETS)
    # ------------------------------------------------------------
    if start_at <= 0:
        stage0_args = ["--limit", str(args.limit)]
        if args.auto_approve:
            stage0_args.append("-y")

        if not run_stage("0_prepare_batch_targets_api.py", stage0_args):
            sys.exit(1)

        if not run_stage("0_prepare_batch_targets_rets.py", stage0_args):
            sys.exit(1)

        api_batch = Path("temp-data/batch_mls_targets_api.csv")
        rets_batch = Path("temp-data/batch_mls_targets_rets.csv")

        if (not api_batch.exists() or not api_batch.read_text().strip()) and \
                (not rets_batch.exists() or not rets_batch.read_text().strip()):
            logger.info("🎉 No unmigrated rules/MLSs remaining! Pipeline finished early.")
            sys.exit(0)

    # ------------------------------------------------------------
    # STAGES 1 TO 4: Ingress, Standardization & Target Generation
    # ------------------------------------------------------------
    if start_at <= 1:
        if not run_stage("1_sync_database_to_repo.py"):
            sys.exit(1)

    if start_at <= 2:
        if not run_stage("2_standardize_rules.py"):
            sys.exit(1)

    if start_at <= 3:
        if not run_stage("3_inserting_new_rule.py"):
            sys.exit(1)

    if start_at <= 4:
        if not run_stage("4_generate_targeted_csv.py"):
            sys.exit(1)

        targeted_csv = Path("temp-data/raw_targeted_rules.csv")
        has_mutations = False
        if targeted_csv.exists():
            lines = [line.strip() for line in targeted_csv.read_text(encoding="utf-8").splitlines() if line.strip()]
            if len(lines) > 1:
                has_mutations = True

        if not has_mutations:
            logger.info("ℹ️ All target MLS rules are already standardized (no-op batch). Updating ledger directly.")
            run_stage("11_update_consolidated_repo.py", ["-y"])
            logger.info("🎉 No DB mutations needed. Ledger updated cleanly.")
            sys.exit(0)

    # ------------------------------------------------------------
    # STAGE 5: Staging Transformation & Bulk Updates
    # ------------------------------------------------------------
    if start_at <= 5:
        logger.info("Running Stage 5 in DRY-RUN (--debug) mode for safety verification...")
        if not run_stage("5_run_bulk_update.py", ["--input-file", "temp-data/raw_targeted_rules.csv", "--debug"]):
            sys.exit(1)

        if not args.auto_approve:
            if not prompt_user_confirmation("Review dry-run output above. Proceed with LIVE Staging DB mutation?"):
                logger.warning("Pipeline aborted by user prior to Staging DB mutation.")
                sys.exit(0)

        if not run_stage("5_run_bulk_update.py", ["--input-file", "temp-data/raw_targeted_rules.csv", "--do-update"]):
            sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 6: Disable Ingestion Jobs Prior to Production Promotion
    # ------------------------------------------------------------
    if start_at <= 6:
        if not args.auto_approve and start_at < 6:
            if not prompt_user_confirmation(
                    "Stage 5 complete. Do you want to proceed with DISABLING Production Ingestion Jobs (Stage 6)?"):
                logger.warning("Pipeline paused by user prior to disabling Production jobs.")
                sys.exit(0)

        if not run_stage("6_disable_ingestion_jobs.py"):
            sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 7: Production Environment Bulk Promotion & Slack Draft
    # ------------------------------------------------------------
    if start_at <= 7:
        if not args.auto_approve and start_at < 7:
            if not prompt_user_confirmation(
                    "Production jobs disabled. Do you want to proceed with PRODUCTION PROMOTION & Slack Draft (Stage 7)?"):
                logger.warning("Pipeline paused by user prior to Production promotion.")
                sys.exit(0)

        stage7_args = ["-y"] if args.auto_approve else []
        if not run_stage("7_run_bulk_promotion.py", stage7_args):
            sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 8: Re-Enable Jobs & Trigger Downloads
    # ------------------------------------------------------------
    if start_at <= 8:
        if not args.auto_approve:
            if not prompt_user_confirmation("Stage 7 Forward Promotion complete. Ready to RE-ENABLE Production Ingestion Jobs & Trigger Downloads (Stage 8)?"):
                logger.warning("Pipeline paused by user prior to Stage 8 manual download trigger.")
                sys.exit(0)

        if not run_stage("8_trigger_manual_downloads.py"):
            sys.exit(1)

        # Interactive Wait Gate
        logger.info("============================================================")
        logger.info("⏳ PRODUCTION DOWNLOADS TRIGGERED IN STAGE 8")
        logger.info(f"   Downloads typically take up to {args.wait_mins} minutes to process in BigQuery.")
        logger.info("============================================================")

        if args.auto_approve:
            wait_seconds = args.wait_mins * 60
            logger.info(f"🤖 Auto-approve mode active. Automatically sleeping for {args.wait_mins} minutes...")

            for remaining in range(wait_seconds, 0, -60):
                logger.info(f"⏳ Time remaining: {remaining // 60} minute(s)...")
                time.sleep(60)

            logger.info("✅ Wait complete. Proceeding to Stage 9 log verification.")
        else:
            if not prompt_user_confirmation(
                    f"Have you allowed ~{args.wait_mins} minutes for downloads to complete? Ready to verify BigQuery logs (Stage 9)?"):
                logger.warning("Pipeline paused by user prior to Stage 9 log verification.")
                logger.info("💡 You can manually run 'python run_pipeline.py --start-at 9' when ready.")
                sys.exit(0)

    # ------------------------------------------------------------
    # STAGE 9: Log Verification & Selective Job Enablement
    # ------------------------------------------------------------
    if start_at <= 9:
        if not run_stage("9_verify_and_enable_injestion_jobs_api.py"):
            logger.error("⛔ Stage 9 detected errors in BigQuery logs or failed execution. Pipeline stopped.")
            sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 10: Safe Soft Delete via Microservice API
    # ------------------------------------------------------------
    if start_at <= 10:
        stage10_args = ["-y"] if args.auto_approve else []
        if not run_stage("10_soft_delete_legacy_rules.py", stage10_args):
            sys.exit(1)

    # ------------------------------------------------------------
    # STAGE 11: Consolidated Repo Sync, Archival & Ledger Update
    # ------------------------------------------------------------
    if start_at <= 11:
        stage11_args = ["-y"] if args.auto_approve else []
        if not run_stage("11_update_consolidated_repo.py", stage11_args):
            sys.exit(1)

    logger.info("============================================================")
    logger.info("🎉 PIPELINE BATCH MIGRATION COMPLETE SUCCESSFULLY!")
    logger.info("============================================================")


if __name__ == "__main__":
    main()
