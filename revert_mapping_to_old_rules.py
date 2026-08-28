"""
Text-Based Reverse Mapping Converter (Rollback Utility).

Reads 'temp-data/migration_blueprint.csv' to dynamically map newly standardized PascalCase
rule names back to their exact legacy string formats (e.g., 'ConvertObjectMapperToList' ->
'Convert ObjectMapper to List'). Performs string substitutions across 'raw_targeted_rules.csv'
and outputs the payload to 'reverse_targeted_rules.csv'.

Logs execution details to 'logs/pipeline_YYYY-MM-DD.log'.
"""

import csv
import sys
from pathlib import Path

from pipeline_logger import setup_logger

current_dir = Path(__file__).resolve().parent
temp_data_dir = current_dir / "temp-data"

blueprint_path = temp_data_dir / "migration_blueprint.csv"
raw_rules_path = temp_data_dir / "raw_targeted_rules.csv"
output_reverse_path = temp_data_dir / "reverse_targeted_rules.csv"

logger = setup_logger("RevertMappings")


def generate_reverse_targeted_rules() -> None:
    logger.info("============================================================")
    logger.info("🔄 REVERSE TARGETED RULES GENERATOR INITIALIZED")
    logger.info("============================================================")

    # --- Validation Checks ---
    if not blueprint_path.exists():
        logger.error(f"❌ Migration blueprint missing at: {blueprint_path}")
        logger.error("   Ensure Stage 2 generated 'migration_blueprint.csv' first.")
        sys.exit(1)

    if not raw_rules_path.exists():
        logger.error(f"❌ Raw targeted rules file missing at: {raw_rules_path}")
        logger.error("   Ensure Stage 4 generated 'raw_targeted_rules.csv' first.")
        sys.exit(1)

    logger.info("[1/2] Loading reversal mappings from migration blueprint...")

    # Load reverse mappings: clean_new_name -> old_name
    reverse_mappings = {}
    with open(blueprint_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            old_name = row.get("old_name", "").strip()
            new_name = row.get("new_name", "").strip()

            if not old_name or not new_name:
                continue

            clean_new_name = new_name[:-3] if new_name.endswith(".py") else new_name

            # Only track mapping if the rule name actually changed
            if old_name != clean_new_name:
                reverse_mappings[clean_new_name] = old_name

    if not reverse_mappings:
        logger.warning("⚠️ No modified rule mappings found in blueprint to revert.")
        return

    logger.info(f"   * Loaded {len(reverse_mappings)} reverse rule mapping definition(s).")
    logger.info("[2/2] Reading raw rules and executing dynamic text substitutions...")

    # Read raw rules file content
    with open(raw_rules_path, mode="r", encoding="utf-8") as f:
        file_content = f.read()

    total_replacements = 0

    # Sort keys by length descending to prevent substring substitution overlap
    sorted_new_names = sorted(reverse_mappings.keys(), key=len, reverse=True)

    for new_name in sorted_new_names:
        old_name = reverse_mappings[new_name]
        occurrence_count = file_content.count(new_name)
        if occurrence_count > 0:
            logger.info(f"   * Reverting '{new_name}' -> '{old_name}' ({occurrence_count} instance(s))")
            file_content = file_content.replace(new_name, old_name)
            total_replacements += occurrence_count

    # Output to reverse target file
    with open(output_reverse_path, mode="w", encoding="utf-8") as f:
        f.write(file_content)

    logger.info("============================================================")
    logger.info("🎉 SUCCESS: REVERSE MAPPING FILE GENERATED")
    logger.info("============================================================")
    logger.info(f"   Source File Managed : {raw_rules_path.name} (Untouched)")
    logger.info(f"   Target File Created : {output_reverse_path.name}")
    logger.info(f"   Total Replacements  : {total_replacements}")
    logger.info("============================================================")


if __name__ == "__main__":
    generate_reverse_targeted_rules()
