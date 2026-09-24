"""
Pipeline Utility Toolkit.

Provides reusable database helper functions to query central ledger locking status
(`public.processed_mls_ledger`), claim target MLS batches, finalize status, and load local target rules.
"""

import os
import json
import csv
import psycopg2
from pathlib import Path

DEFAULT_USER = (
    os.getenv("DB_USER") or
    os.getenv("USERNAME") or
    os.getenv("USER") or
    "migration_pipeline_bot"
).strip().strip("'\"")


def load_discrepant_mls(root_dir: Path) -> set[int]:
    """Reads unique integer MLS IDs from root-level discrepant_mls.txt file."""
    discrepant_file = root_dir / "discrepant_mls.txt"
    discrepant_ids = set()

    if not discrepant_file.exists():
        return discrepant_ids

    try:
        with open(discrepant_file, mode="r", encoding="utf-8") as f:
            for line in f:
                cleaned = line.split("#")[0].strip().strip("'\"")
                if cleaned.isdigit():
                    discrepant_ids.add(int(cleaned))
    except Exception:
        pass

    return discrepant_ids


def load_processed_ledger(temp_data_dir: Path) -> set[int]:
    """Loads all completed integer MLS primary key IDs from processed_mls_ledger.json."""
    ledger_file = temp_data_dir / "processed_mls_ledger.json"
    completed_mls = set()

    if ledger_file.exists():
        try:
            data = json.loads(ledger_file.read_text(encoding="utf-8"))
            completed_mls = set(int(x) for x in data.get("completed_mls_ids", []))
        except Exception:
            pass

    return completed_mls


def get_db_locked_mls_ids(conn) -> set[int]:
    """Queries public.processed_mls_ledger to return MLS primary key IDs currently marked IN_PROGRESS or COMPLETED."""
    locked_ids = set()
    query = """
        SELECT mls_id 
        FROM public.processed_mls_ledger 
        WHERE status IN ('IN_PROGRESS', 'COMPLETED');
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            for (m_id,) in cur.fetchall():
                if m_id is not None:
                    locked_ids.add(int(m_id))
    except Exception:
        conn.rollback()

    return locked_ids


def claim_mls_batch_in_db(conn, mls_targets: list[tuple[int, str]] | list[int], current_user: str = DEFAULT_USER) -> None:
    """Locks a batch of MLS targets as 'IN_PROGRESS' in public.processed_mls_ledger."""
    if not mls_targets:
        return

    normalized_targets = []
    for item in mls_targets:
        if isinstance(item, tuple):
            normalized_targets.append((int(item[0]), str(item[1])))
        else:
            normalized_targets.append((int(item), str(item)))

    query = """
        INSERT INTO public.processed_mls_ledger (mls_id, mls_id_str, status, created_by, updated_by, created_at, updated_at)
        VALUES (%s, %s, 'IN_PROGRESS', %s, %s, NOW(), NOW())
        ON CONFLICT (mls_id) 
        DO UPDATE SET status = 'IN_PROGRESS', mls_id_str = EXCLUDED.mls_id_str, updated_by = EXCLUDED.updated_by, updated_at = NOW();
    """
    try:
        with conn.cursor() as cur:
            for m_id, m_id_str in normalized_targets:
                cur.execute(query, (m_id, m_id_str, current_user, current_user))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def finalize_mls_batch_in_db(conn, mls_targets: list[tuple[int, str]] | list[int], current_user: str = DEFAULT_USER) -> None:
    """Finalizes target MLS primary key IDs as 'COMPLETED' in public.processed_mls_ledger."""
    if not mls_targets:
        return

    normalized_targets = []
    for item in mls_targets:
        if isinstance(item, tuple):
            normalized_targets.append((int(item[0]), str(item[1])))
        else:
            normalized_targets.append((int(item), str(item)))

    query = """
        INSERT INTO public.processed_mls_ledger (mls_id, mls_id_str, status, created_by, updated_by, created_at, updated_at)
        VALUES (%s, %s, 'COMPLETED', %s, %s, NOW(), NOW())
        ON CONFLICT (mls_id) 
        DO UPDATE SET status = 'COMPLETED', mls_id_str = EXCLUDED.mls_id_str, updated_by = EXCLUDED.updated_by, updated_at = NOW();
    """
    try:
        with conn.cursor() as cur:
            for m_id, m_id_str in normalized_targets:
                cur.execute(query, (m_id, m_id_str, current_user, current_user))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def load_skip_mls(temp_data_dir: Path) -> set[int]:
    """Reads unique integer MLS primary key IDs from skip_mls.txt if present."""
    skip_file = temp_data_dir / "skip_mls.txt"
    skip_ids = set()

    if not skip_file.exists():
        return skip_ids

    try:
        with open(skip_file, mode="r", encoding="utf-8") as f:
            for line in f:
                cleaned = line.split("#")[0].strip().strip("'\"")
                if cleaned.isdigit():
                    skip_ids.add(int(cleaned))
    except Exception:
        pass

    return skip_ids


def load_target_mls(temp_data_dir: Path) -> set[int]:
    """
    Loads target numeric MLS primary key IDs across api_mls.txt, rets_mls.txt, promotion_sources.txt,
    or batch target CSV files.
    """
    target_mls = set()

    txt_candidates = [
        temp_data_dir / "api_mls.txt",
        temp_data_dir / "rets_mls.txt",
        temp_data_dir / "promotion_sources.txt",
    ]
    for txt_file in txt_candidates:
        if txt_file.exists():
            for line in txt_file.read_text(encoding="utf-8").splitlines():
                cleaned = line.split("#")[0].strip().strip("'\"")
                if cleaned.isdigit():
                    target_mls.add(int(cleaned))

    csv_candidates = [
        temp_data_dir / "batch_mls_targets_api.csv",
        temp_data_dir / "batch_mls_targets_rets.csv",
        temp_data_dir / "batch_mls_targets.csv",
    ]
    for csv_file in csv_candidates:
        if csv_file.exists():
            try:
                with open(csv_file, mode="r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        m_id = row.get("mls_id", "").strip()
                        if m_id.isdigit():
                            target_mls.add(int(m_id))
            except Exception:
                pass

    return target_mls


def load_target_rules(temp_data_dir: Path) -> set[str]:
    """Loads target rule names from api_rules.txt or rets_rules.txt."""
    target_rules = set()

    txt_candidates = [
        temp_data_dir / "api_rules.txt",
        temp_data_dir / "rets_rules.txt",
    ]
    for txt_file in txt_candidates:
        if txt_file.exists():
            for line in txt_file.read_text(encoding="utf-8").splitlines():
                cleaned = line.split("#")[0].strip().strip("'\"")
                if cleaned:
                    target_rules.add(cleaned)

    return target_rules
