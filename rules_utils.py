"""
Pipeline Utilities Module.

Provides shared configuration loaders, directory checkers, and test file parsers
for rule-level and MLS-level batch operations.
"""

from pathlib import Path


def load_target_test_rules(temp_data_dir: Path) -> set[str]:
    """Reads temp-data/test_rules.txt if present.

    Returns a set of rule names to target.
    Returns an empty set if missing, empty, or commented out.
    """
    test_file = temp_data_dir / "test_rules.txt"
    if not test_file.exists():
        return set()

    lines = test_file.read_text(encoding="utf-8").splitlines()
    return {
        line.strip().strip("'\"")
        for line in lines
        if line.strip() and not line.strip().startswith("#")
    }


def load_target_test_mls(temp_data_dir: Path) -> set[int]:
    """Reads temp-data/test_mls.txt if present.

    Returns a set of numeric MLS IDs (e.g., {652, 656}).
    Returns an empty set if missing, empty, or commented out.
    """
    test_file = temp_data_dir / "test_mls.txt"
    if not test_file.exists():
        return set()

    lines = test_file.read_text(encoding="utf-8").splitlines()
    target_ids = set()

    for line in lines:
        cleaned = line.strip().strip("'\"")
        if cleaned and not cleaned.startswith("#"):
            try:
                target_ids.add(int(cleaned))
            except ValueError:
                print(f"⚠️ Warning: Skipping non-numeric MLS ID '{cleaned}' in test_mls.txt")

    return target_ids
