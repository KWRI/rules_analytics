"""
Pipeline Stage 2: High-Speed Case-Insensitive Translation Compiler.

Parses extracted staging records and compiles rule identifiers into a standardized
PascalCase naming format. Concurrently writes transformed rule files to target
directories and outputs a workspace mapping ledger (migration_blueprint.csv).

Supports target batch testing mode via 'temp-data/test_rules.txt' using rules_utils.py.
Logs execution events to 'logs/pipeline_YYYY-MM-DD.log'.
"""

import os
import io
import csv
import re
import shutil
import stat
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import psycopg2
import paramiko
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from cryptography.utils import CryptographyDeprecationWarning
from cryptography.hazmat.primitives import serialization

from rules_utils import load_target_test_rules
from pipeline_logger import setup_logger

warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
warnings.filterwarnings("ignore", message=".*TripleDES.*")

load_dotenv()
logger = setup_logger("Stage2_StandardizeRules")


def ensure_git_ignores_csv_artifacts(target_base_dir: Path) -> None:
    """Ensures local .gitignore rules exist to prevent CSV artifacts from leaking into git."""
    repo_root = target_base_dir.parent
    gitignore_path = repo_root / ".gitignore"
    required_ignores = ["*.csv", "standard-rule-names/temp-data/"]

    existing_rules = set()
    if gitignore_path.exists():
        existing_rules = {
            line.strip()
            for line in gitignore_path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")
        }

    missing_ignores = [rule for rule in required_ignores if rule not in existing_rules]

    if missing_ignores:
        with open(gitignore_path, "a", encoding="utf-8") as f:
            if not gitignore_path.exists() or gitignore_path.stat().st_size == 0:
                f.write("# Consolidated Rules Repository Artifact Shields\n")
            else:
                f.write("\n\n# Added dynamically by Stage 2 pipeline compilation framework\n")
            for rule in missing_ignores:
                f.write(f"{rule}\n")


def to_pascal_case(name: str) -> str:
    """Transforms raw rule names into PascalCase filenames while preserving camelCase/PascalCase words."""
    base_name = name.removesuffix(".py")
    base_name = re.sub(r"[\(\)\[\]\{\}]", "", base_name)
    base_name = re.sub(r"[/|\\]|__", " ", base_name)

    words = re.split(r"[\s_-]+", base_name)

    pascal_words = []
    for word in words:
        if not word:
            continue
        # Preserve internal camelCase/PascalCase words (e.g., ObjectMapper, 148Subdivision)
        if re.search(r"[a-z][A-Z]", word) or re.search(r"\d+[A-Z]", word):
            pascal_words.append(word[0].upper() + word[1:])
        else:
            # Capitalize the first letter found in the word without lowercasing subsequent letters
            match = re.search(r"[a-zA-Z]", word)
            if match:
                idx = match.start()
                word = word[:idx] + word[idx].upper() + word[idx+1:]
            pascal_words.append(word)

    pascal = "".join(pascal_words)
    return f"{pascal if pascal else 'UnnamedRule'}.py"


def safely_delete_dir(dir_path: Path) -> None:
    """Removes target directory recursively, clearing read-only flags if necessary."""
    if not dir_path.exists():
        return

    def remove_readonly(func, path, _):
        try:
            os.chmod(path, stat.S_IWRITE)
            func(path)
        except Exception:
            pass

    try:
        shutil.rmtree(dir_path, onexc=remove_readonly if hasattr(shutil, "onexc") else None)
        time.sleep(0.1)
    except Exception:
        shutil.rmtree(dir_path, ignore_errors=True)


def write_standard_file(target_path: Path, content: str | None) -> None:
    """Worker task to output individual rule file payload."""
    payload = content if content else "# No code content defined\n"
    target_path.write_text(payload, encoding="utf-8")


def run_standardization():
    repo_path_raw = os.getenv("REPO_PATH", "")
    repo_path = repo_path_raw.strip().strip("'\"")
    if not repo_path:
        logger.error("REPO_PATH missing from environment configuration.")
        return

    project_dir = Path(__file__).resolve().parent
    temp_data_dir = project_dir / "temp-data"
    temp_data_dir.mkdir(parents=True, exist_ok=True)

    # Check for target batch test file or single rule env var
    target_test_rules = load_target_test_rules(temp_data_dir)
    env_test_rule = os.getenv("TEST_RULE_NAME", "").strip().strip("'\"") or None

    if env_test_rule:
        target_test_rules.add(env_test_rule)

    target_base_dir = Path(repo_path) / "standard-rule-names"
    target_active_dir = target_base_dir / "active"
    target_archived_dir = target_base_dir / "archived"

    temp_files_cache = {}
    if temp_data_dir.exists():
        for file in temp_data_dir.glob("*.csv"):
            try:
                temp_files_cache[file.name] = file.read_text(encoding="utf-8")
            except Exception:
                pass

    safely_delete_dir(target_base_dir)
    target_base_dir.mkdir(parents=True, exist_ok=True)
    target_active_dir.mkdir(parents=True, exist_ok=True)
    target_archived_dir.mkdir(parents=True, exist_ok=True)

    for fname, fcontent in temp_files_cache.items():
        if fname != "migration_blueprint.csv":
            (temp_data_dir / fname).write_text(fcontent, encoding="utf-8")

    csv_output_path = temp_data_dir / "migration_blueprint.csv"
    ensure_git_ignores_csv_artifacts(target_base_dir)

    ssh_host = os.getenv("SSH_HOST", "").strip().strip("'\"")
    ssh_user = os.getenv("SSH_USER", "").strip().strip("'\"")
    ssh_key_path = os.getenv("SSH_KEY_PATH", "").strip().strip("'\"")
    ssh_passphrase = os.getenv("SSH_KEY_PASSPHRASE", "").strip().strip("'\"")

    db_host = os.getenv("DB_HOST", "").strip().strip("'\"")
    db_name = os.getenv("DB_NAME", "").strip().strip("'\"")
    db_user = os.getenv("DB_USER", "").strip().strip("'\"")
    db_pass = os.getenv("DB_PASSWORD", "").strip().strip("'\"")

    db_raw_records = {}

    try:
        with open(ssh_key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=ssh_passphrase.encode() if ssh_passphrase else None,
            )
        pem_data = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
        mypkey = paramiko.RSAKey.from_private_key(io.StringIO(pem_data.decode()))

        logger.info("Connecting to database via SSH tunnel to extract records...")
        with SSHTunnelForwarder(
                (ssh_host, 22),
                ssh_username=ssh_user,
                ssh_pkey=mypkey,
                remote_bind_address=(db_host, 5432),
        ) as tunnel:
            with psycopg2.connect(
                    dbname=db_name,
                    user=db_user,
                    password=db_pass,
                    host="127.0.0.1",
                    port=tunnel.local_bind_port,
            ) as conn:
                with conn.cursor() as cur:
                    if target_test_rules:
                        logger.info(f"🧪 [BATCH TEST MODE] Targeting {len(target_test_rules)} rule(s)...")
                        select_query = """
                            SELECT id, name, content, description, params, "order", created_at, deleted_at 
                            FROM public.process_rule
                            WHERE name = ANY(%s);
                        """
                        cur.execute(select_query, (list(target_test_rules),))
                    else:
                        logger.info("🚀 [FULL PRODUCTION MODE] Pulling all process_rule records...")
                        select_query = """
                            SELECT id, name, content, description, params, "order", created_at, deleted_at 
                            FROM public.process_rule;
                        """
                        cur.execute(select_query)

                    rows = cur.fetchall()
                    if not rows and target_test_rules:
                        logger.warning("No records found matching target test rule names.")

                    for row in rows:
                        _, old_name, content, _, _, _, created_at, deleted_at = row[:8]
                        if not old_name:
                            continue

                        is_deleted = deleted_at is not None
                        pascal_name = to_pascal_case(old_name)
                        lowercase_stem = Path(pascal_name).stem.lower()

                        if lowercase_stem not in db_raw_records:
                            db_raw_records[lowercase_stem] = []

                        db_raw_records[lowercase_stem].append({
                            "old_name": old_name,
                            "pascal_name": pascal_name,
                            "content": content,
                            "is_deleted": is_deleted,
                            "created_at": created_at,
                        })

    except Exception as err:
        logger.error(f"Database connection or extraction failure: {err}", exc_info=True)
        return

    csv_rows = []
    active_copied_count = 0
    archived_copied_count = 0
    futures = []

    with ThreadPoolExecutor(max_workers=16) as executor:
        for lowercase_stem, rows in db_raw_records.items():
            sorted_rows = sorted(rows, key=lambda x: (not x["is_deleted"], x["created_at"]))

            if len(sorted_rows) == 1:
                item = sorted_rows[0]
                old_name = item["old_name"]
                new_name = Path(item["pascal_name"]).stem

                target_folder = target_archived_dir if item["is_deleted"] else target_active_dir
                futures.append(
                    executor.submit(
                        write_standard_file, target_folder / f"{new_name}.py", item["content"]
                    )
                )

                if item["is_deleted"]:
                    archived_copied_count += 1
                else:
                    active_copied_count += 1

                csv_rows.append([old_name, new_name])
            else:
                for index, item in enumerate(sorted_rows, start=1):
                    old_name = item["old_name"]
                    new_name = f"{Path(item['pascal_name']).stem}V{index}"

                    target_folder = target_archived_dir if item["is_deleted"] else target_active_dir
                    futures.append(
                        executor.submit(
                            write_standard_file,
                            target_folder / f"{new_name}.py", item["content"]
                        )
                    )

                    if item["is_deleted"]:
                        archived_copied_count += 1
                    else:
                        active_copied_count += 1

                    csv_rows.append([old_name, new_name])

        for future in as_completed(futures):
            future.result()

    for attempt in range(5):
        try:
            with open(csv_output_path, mode="w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(["old_name", "target_new_name"])
                writer.writerows(csv_rows)
            break
        except PermissionError as e:
            if attempt == 4:
                logger.error("Critical Error: Unable to write CSV due to persistent file lock.")
                raise e
            time.sleep(1.0)

    logger.info("============================================================")
    logger.info("🚀 PHASE 2 COMPLETE: HIGH-SPEED CASE-INSENSITIVE COMPILER")
    logger.info("============================================================")
    logger.info(f"Mode                                     : {'BATCH TEST (' + str(len(target_test_rules)) + ' rules)' if target_test_rules else 'FULL PRODUCTION'}")
    logger.info(f"Standardized Active Rules Written        : {active_copied_count}")
    logger.info(f"Standardized Archived Rules Written      : {archived_copied_count}")
    logger.info(f"Unified (Active + Deleted) Rows in CSV   : {len(csv_rows)}")
    logger.info(f"Isolated Blueprint CSV Path              : {csv_output_path}")
    logger.info("============================================================")


if __name__ == "__main__":
    run_standardization()
