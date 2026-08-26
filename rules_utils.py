"""
Pipeline Utilities Module.

Provides shared configuration loaders, directory checkers, and target file parsers
for rule-level and MLS-level batch operations across API and RETS protocols.
"""

from pathlib import Path


def load_target_rules(temp_data_dir: Path) -> set[str]:
    """
    Reads rule names from 'api_rules.txt' and 'rets_rules.txt' inside temp-data/.
    Returns a unified set of rule names. Safely skips missing files or commented lines.
    """
    target_rules = set()
    candidate_files = [
        temp_data_dir / "api_rules.txt",
        temp_data_dir / "rets_rules.txt",
    ]

    for rule_file in candidate_files:
        if rule_file.exists():
            lines = rule_file.read_text(encoding="utf-8").splitlines()
            for line in lines:
                cleaned = line.strip().strip("'\"")
                if cleaned and not cleaned.startswith("#"):
                    target_rules.add(cleaned)

    return target_rules


def load_target_mls(temp_data_dir: Path) -> set[int]:
    """
    Reads numeric MLS IDs from 'api_mls.txt' and 'rets_mls.txt' inside temp-data/.
    Returns a unified set of integer MLS IDs (e.g., {189, 533}).
    Safely skips missing files, non-numeric values, or commented lines.
    """
    target_ids = set()
    candidate_files = [
        temp_data_dir / "api_mls.txt",
        temp_data_dir / "rets_mls.txt",
    ]

    for mls_file in candidate_files:
        if mls_file.exists():
            lines = mls_file.read_text(encoding="utf-8").splitlines()
            for line in lines:
                cleaned = line.strip().strip("'\"")
                if cleaned and not cleaned.startswith("#"):
                    try:
                        target_ids.add(int(cleaned))
                    except ValueError:
                        print(f"⚠️ Warning: Skipping non-numeric MLS ID '{cleaned}' in {mls_file.name}")

    return target_ids
