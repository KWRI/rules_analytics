"""
Update Consolidated Repo Script.

This module reads the migration blueprint (temp-data/migration_blueprint.csv)
and moves soft-deleted legacy rule files from the active repository
(defined by REPO_PATH in .env) into the archived directory.

"""

import os
import sys
import shutil
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def sanitize_filename(raw_name: str) -> str:
    """Matches Script 1's filename sanitization logic."""
    clean = raw_name.replace("/", "__").replace("\\", "__")
    clean = os.path.sub(r'[\x00-\x1f:*?"<>|]', '', clean).strip() if hasattr(os, 'sub') else clean

    if not clean:
        clean = "unnamed_rule"

    if not clean.lower().endswith(".py"):
        clean = f"{clean}.py"

    return clean


def load_old_rule_names(blueprint_path: Path) -> list[str]:
    """Parses the migration blueprint and returns legacy rule names."""
    if not blueprint_path.exists():
        print(f"❌ Error: Blueprint file not found at '{blueprint_path}'")
        sys.exit(1)

    df = pd.read_csv(blueprint_path) if blueprint_path.suffix == ".csv" else pd.read_excel(blueprint_path)

    column_name = "old_name"
    if column_name not in df.columns:
        print(f"❌ Error: Required column '{column_name}' missing from blueprint.")
        sys.exit(1)

    return df[column_name].dropna().astype(str).unique().tolist()


def archive_legacy_rules(blueprint_path: Path, repo_base_path: Path) -> bool:
    """Moves legacy rules from active/ to archived/ using REPO_PATH."""
    old_rule_names = load_old_rule_names(blueprint_path)
    print(f"📄 Loaded {len(old_rule_names)} legacy rule name(s) from blueprint.")

    ui_rules_root = repo_base_path / "ui-rules"
    active_dir = ui_rules_root / "active"
    archived_dir = ui_rules_root / "archived"

    if not active_dir.exists():
        print(f"❌ Error: Active directory not found at '{active_dir}'")
        return False

    archived_dir.mkdir(parents=True, exist_ok=True)

    archived_count = 0
    missing_count = 0

    for rule_name in old_rule_names:
        found = False

        # Determine the target filename matching Script 1 (.py extension)
        target_py_filename = rule_name if rule_name.endswith(".py") else f"{rule_name}.py"

        # Look for exact or collisions in active/
        for item in active_dir.glob("*.py"):
            # Check if file matches the rule name (or collision variant like rule_name_collision_1.py)
            if item.stem == rule_name or item.stem.startswith(f"{rule_name}_collision_"):
                destination = archived_dir / item.name
                shutil.move(str(item), str(destination))
                print(f"  📦 Archived: {item.name}  ==>  ui-rules/archived/{item.name}")
                archived_count += 1
                found = True

        if not found:
            print(f"  ⚠️ Warning: Rule file for '{rule_name}' was not found in '{active_dir}'")
            missing_count += 1

    print(f"\n📊 Summary:")
    print(f"   • Archived: {archived_count} rule file(s)")
    if missing_count > 0:
        print(f"   • Missing: {missing_count} rule file(s)")

    return True


def cleanup_temp_directories(temp_dirs: list[Path]) -> None:
    """Removes temporary working directories after sync finishes."""
    print("\n🧹 Cleaning up temporary working files...")
    for temp_dir in temp_dirs:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
            print(f"  🗑️ Removed folder: '{temp_dir}'")
        else:
            print(f"  ℹ️ Folder '{temp_dir}' is already clean.")


def main() -> None:
    BLUEPRINT_FILE = Path("temp-data/migration_blueprint.csv")

    # Get REPO_PATH from .env just like Script 1
    repo_base_raw = os.getenv("REPO_PATH", "")
    repo_base = repo_base_raw.strip().strip("'\"")

    if not repo_base:
        print("❌ Error: REPO_PATH not set in environment.")
        return

    REPO_PATH = Path(repo_base)
    TEMP_DIRS_TO_REMOVE = [Path("temp-data")]

    print("--- 📂 dm-consolidated-rules Archive Sync Tool ---")
    print(f"📍 Target REPO_PATH: {REPO_PATH}\n")

    success = archive_legacy_rules(BLUEPRINT_FILE, REPO_PATH)

    if not success:
        print("❌ Sync halted due to error.")
        return

    confirm = input("\nDo you want to delete temporary working files ('temp-data/')? (y/N): ")
    if confirm.lower() == "y":
        cleanup_temp_directories(TEMP_DIRS_TO_REMOVE)
        print("\n✨ Clean complete! Workspace repository updated.")
    else:
        print("ℹ️ Cleanup skipped. Working files retained in 'temp-data/'.")


if __name__ == "__main__":
    main()
