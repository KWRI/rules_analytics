"""
Text-Based Reverse Mapping Converter (Rollback Utility).

Reads 'temp-data/migration_blueprint.csv' to dynamically map newly standardized PascalCase
rule names back to their exact legacy string formats (e.g., 'ConvertObjectMapperToList' ->
'Convert ObjectMapper to List'). Performs structured CSV field substitutions across 'raw_targeted_rules.csv'
and outputs the payload to 'reverse_targeted_rules.csv'.

Logs execution details to 'logs/pipeline_YYYY-MM-DD.log'.
"""

import csv
import json
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
            # Support both "target_new_name" (Stage 2 default) and "new_name" fallback
            new_name = (row.get("target_new_name") or row.get("new_name") or "").strip()

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
    logger.info("[2/2] Reading raw rules and executing structured CSV rule function reversals...")

    total_replacements = 0
    reverted_rows = []

    with open(raw_rules_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        for row in reader:
            rule_func_raw = row.get("rule_func", "")

            if rule_func_raw:
                try:
                    # Parse JSON array (e.g. '["PascalCaseRule"]')
                    funcs = json.loads(rule_func_raw)
                    if isinstance(funcs, list):
                        updated_funcs = []
                        for func in funcs:
                            if func in reverse_mappings:
                                old_func = reverse_mappings[func]
                                updated_funcs.append(old_func)
                                total_replacements += 1
                                logger.info(f"   * Reverting '{func}' -> '{old_func}' in MLS {row.get('mls_id')}")
                            else:
                                updated_funcs.append(func)

                        row["rule_func"] = json.dumps(updated_funcs)
                    elif isinstance(funcs, str) and funcs in reverse_mappings:
                        old_func = reverse_mappings[funcs]
                        row["rule_func"] = json.dumps([old_func])
                        total_replacements += 1

                except (json.JSONDecodeError, TypeError):
                    # Fallback string replace strictly on rule_func column if not valid JSON
                    for new_name, old_name in reverse_mappings.items():
                        if new_name in rule_func_raw:
                            rule_func_raw = rule_func_raw.replace(new_name, old_name)
                            total_replacements += 1
                    row["rule_func"] = rule_func_raw

            reverted_rows.append(row)

    # Output to reverse target file safely
    with open(output_reverse_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(reverted_rows)

    logger.info("============================================================")
    logger.info("🎉 SUCCESS: REVERSE MAPPING FILE GENERATED")
    logger.info("============================================================")
    logger.info(f"   Source File Managed : {raw_rules_path.name} (Untouched)")
    logger.info(f"   Target File Created : {output_reverse_path.name}")
    logger.info(f"   Total Function Reversals : {total_replacements}")
    logger.info("============================================================")


if __name__ == "__main__":
    generate_reverse_targeted_rules()
