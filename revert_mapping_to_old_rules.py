"""
Text-Based Reverse Mapping Converter.

Reads 'temp-data/migration_blueprint.csv' to dynamically map newly standardized PascalCase
rule names back to their exact legacy string formats (e.g., 'ConvertObjectMapperToList' ->
'Convert ObjectMapper to List'). Performs string substitutions across 'raw_targeted_rules.csv'
and outputs the payload to 'reverse_targeted_rules.csv'.
"""

import csv
from pathlib import Path

# Target local project directories
current_dir = Path(__file__).resolve().parent
temp_data_dir = current_dir / "temp-data"

blueprint_path = temp_data_dir / "migration_blueprint.csv"
raw_rules_path = temp_data_dir / "raw_targeted_rules.csv"
output_reverse_path = temp_data_dir / "reverse_targeted_rules.csv"


def generate_reverse_targeted_rules():
    print("\n" + "=" * 60)
    print("REVERSE TARGETED RULES GENERATOR INITIALIZED")
    print("=" * 60)

    # --- Validation Checks ---
    if not blueprint_path.exists():
        print(f"[ERROR] Migration blueprint file missing at: {blueprint_path}")
        print("   Please ensure Stage 2 generated migration_blueprint.csv first.")
        return

    if not raw_rules_path.exists():
        print(f"[ERROR] Raw targeted rules file missing at: {raw_rules_path}")
        print("   Please ensure Stage 4 generated raw_targeted_rules.csv first.")
        return

    print("[1/2] Loading reversal mappings from migration blueprint...")

    # Load reverse mappings: new_name -> old_name
    reverse_mappings = {}
    with open(blueprint_path, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # Skip Header Row
        for row in reader:
            if len(row) >= 2:
                old_name, new_name = row[0].strip(), row[1].strip()
                clean_new_name = new_name[:-3] if new_name.endswith(".py") else new_name

                # Only map if the name actually changed
                if old_name != clean_new_name:
                    reverse_mappings[clean_new_name] = old_name

    if not reverse_mappings:
        print("[WARNING] No modified rule mappings found in blueprint to revert.")
        return

    print(f"   * Loaded {len(reverse_mappings)} reverse rule mapping definition(s).")
    print("[2/2] Reading raw rules and executing dynamic text substitutions...")

    # Read the entire file content as a single text block
    with open(raw_rules_path, mode="r", encoding="utf-8") as f:
        file_content = f.read()

    total_replacements = 0

    # Perform direct textual replacements for every standardized rule found
    for new_name, old_name in reverse_mappings.items():
        occurrence_count = file_content.count(new_name)
        if occurrence_count > 0:
            print(f"   * Reverting '{new_name}' -> '{old_name}' ({occurrence_count} instance(s))")
            file_content = file_content.replace(new_name, old_name)
            total_replacements += occurrence_count

    # Write out to separate reverse target file
    with open(output_reverse_path, mode="w", encoding="utf-8") as f:
        f.write(file_content)

    print("\n" + "=" * 60)
    print("SUCCESS: REVERSE FILE GENERATED")
    print("=" * 60)
    print(f" Source File Managed : {raw_rules_path.name} (100% Untouched)")
    print(f" Target File Created : {output_reverse_path.name}")
    print(f" Total Replacements  : {total_replacements}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    generate_reverse_targeted_rules()
