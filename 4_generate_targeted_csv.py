"""
Pipeline Stage 4: Architectural Property Configuration Extraction Engine.

This script ingests the local 'temp-data/migration_blueprint.csv' map and
streams live vendor property definitions from the database. It recursively traverses deeply nested
JSON property and mapping schema trees to target active rule mutations. It then writes a
comprehensive database layout configuration map to 'temp-data/raw_targeted_rules.csv' and aggregates
a numerically sorted, unique integer list of affected IDs to 'temp-data/promotion_sources.txt'.

Supports target MLS batch mode via 'temp-data/api_mls.txt' and/or 'temp-data/rets_mls.txt'
(via rules_utils.py).
Logs execution events to 'logs/pipeline_YYYY-MM-DD.log'.
"""

import os
import io
import json
import csv
import time
import sys
import signal
import warnings
from pathlib import Path
import psycopg2
import paramiko
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
from cryptography.utils import CryptographyDeprecationWarning
from cryptography.hazmat.primitives import serialization


# --- OS-LEVEL INSTANT TERMINAL EXIT ---
def force_terminal_exit(sig, frame):
    print("\n⛔ [TERMINAL ABORT] Killing process tree immediately...")
    os._exit(1)


signal.signal(signal.SIGINT, force_terminal_exit)
if hasattr(signal, "SIGBREAK"):
    signal.signal(signal.SIGBREAK, force_terminal_exit)

from rules_utils import load_target_mls, load_processed_ledger
from pipeline_logger import setup_logger

warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)
warnings.filterwarnings("ignore", message=".*TripleDES.*")

current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")
logger = setup_logger("Stage4_ExtractCSV")


def update_ledger_directly(temp_data_dir: Path, no_op_mls_ids: set[int]) -> None:
    """Writes no-op (already standardized) MLS IDs directly into processed_mls_ledger.json."""
    if not no_op_mls_ids:
        return

    ledger_file = temp_data_dir / "processed_mls_ledger.json"
    completed_mls = load_processed_ledger(temp_data_dir)

    completed_mls.update(no_op_mls_ids)
    ledger_file.parent.mkdir(parents=True, exist_ok=True)
    ledger_file.write_text(
        json.dumps({"completed_mls_ids": sorted(list(completed_mls))}, indent=2),
        encoding="utf-8"
    )

    logger.info(
        f"✅ [DIRECT LEDGER UPDATE] Recorded {len(no_op_mls_ids)} no-op MLS ID(s) "
        f"({', '.join(str(m) for m in sorted(list(no_op_mls_ids)))}) directly into '{ledger_file.name}'."
    )


def extract_rules_recursive(schema_node, target_rules_set, current_path=""):
    """Recursively traverses the properties JSON tree to locate rule mapping configurations."""
    found_mappings = []
    if not isinstance(schema_node, dict):
        return found_mappings

    if "rule" in schema_node and isinstance(schema_node["rule"], dict):
        rule_funcs = schema_node["rule"].get("func", [])
        if isinstance(rule_funcs, str):
            rule_funcs = [rule_funcs]
        clean_funcs = [str(f) for f in rule_funcs]

        if any(r in target_rules_set for r in clean_funcs):
            found_mappings.append({
                "structural_path": current_path,
                "field_value": schema_node
            })

    if "properties" in schema_node and isinstance(schema_node["properties"], dict):
        for key, child_node in schema_node["properties"].items():
            next_path = f"{current_path}.properties.{key}" if current_path else key
            found_mappings.extend(
                extract_rules_recursive(child_node, target_rules_set, next_path)
            )
        return found_mappings

    if "items" in schema_node and isinstance(schema_node["items"], dict):
        next_path = f"{current_path}.items" if current_path else "items"
        found_mappings.extend(
            extract_rules_recursive(schema_node["items"], target_rules_set, next_path)
        )
        return found_mappings

    for key, child_node in schema_node.items():
        if key in ["rule", "mapper", "transform", "enhance", "properties", "items"]:
            continue
        if isinstance(child_node, dict):
            next_path = f"{current_path}.{key}" if current_path else key
            found_mappings.extend(
                extract_rules_recursive(child_node, target_rules_set, next_path)
            )

    return found_mappings


def run_production_extractor():
    repo_base_raw = os.getenv("REPO_PATH", "") or os.getenv("UI_RULES_DIR", "")
    repo_path = repo_base_raw.strip().strip("'\"")
    if not repo_path:
        repo_path = str(current_dir.parent / "dm-consolidated-rules")

    ui_rules_root = Path(repo_path) / "ui-rules"
    if not ui_rules_root.exists() and (Path(repo_path) / "active").exists():
        active_rules_dir = Path(repo_path) / "active"
    else:
        active_rules_dir = ui_rules_root / "active"

    temp_data_dir = current_dir / "temp-data"
    temp_data_dir.mkdir(parents=True, exist_ok=True)

    blueprint_path = temp_data_dir / "migration_blueprint.csv"
    output_csv_path = temp_data_dir / "raw_targeted_rules.csv"
    txt_output_path = temp_data_dir / "promotion_sources.txt"

    if not blueprint_path.exists():
        logger.error("Stage 2 assets missing. Please execute Stage 2 standardization first.")
        return

    target_batch_mls = load_target_mls(temp_data_dir)
    if target_batch_mls:
        logger.info(
            f"🎯 [MLS BATCH TARGET] Targeting {len(target_batch_mls)} specified MLS ID(s): {sorted(list(target_batch_mls))}")
    else:
        logger.info("🚀 [FULL MODE] Executing extraction across ALL active MLS configurations...")

    blueprint_mapping = {}
    skipped_identical_count = 0

    with open(blueprint_path, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) >= 2:
                old_name, proposed_new_name = row[0].strip(), row[1].strip()
                clean_new_name = proposed_new_name[:-3] if proposed_new_name.endswith(".py") else proposed_new_name

                if old_name == clean_new_name:
                    logger.info(
                        f"ℹ️ Skipping rule '{old_name}' from CSV extraction because old name matches proposed new name.")
                    skipped_identical_count += 1
                    continue

                blueprint_mapping[old_name] = clean_new_name

    headers = [
        "vendor_name", "mls_id_str", "mls_id", "mls_name", "download_protocol",
        "content_type", "content_sub_type", "is_enabled", "field_path",
        "mapper_func", "mapper_params", "transform_func", "transform_params",
        "rule_func", "rule_params", "enhance_func", "enhance_params"
    ]

    if not blueprint_mapping:
        if skipped_identical_count > 0:
            logger.info(
                f"Skipped all {skipped_identical_count} candidate rule(s) because old and proposed new names are identical. No CSV generated.")
            if target_batch_mls:
                update_ledger_directly(temp_data_dir, target_batch_mls)
        else:
            logger.info("No active rule mutations mapped in the blueprint to extract.")

        # Write empty header row so bulk_map_tool can safely load CSV headers
        with open(output_csv_path, mode="w", newline="", encoding="utf-8") as f:
            csv.writer(f, quoting=csv.QUOTE_MINIMAL).writerow(headers)

        # Ensure promotion_sources.txt is empty if no mutations exist
        txt_output_path.write_text("", encoding="utf-8")
        return

    target_rules_set = set(blueprint_mapping.keys())
    logger.info(f"🎯 Target Rules Loaded from Blueprint: Processing {len(target_rules_set)} active rule mutation(s).")

    with open(output_csv_path, mode="w", newline="", encoding="utf-8") as f:
        csv.writer(f, quoting=csv.QUOTE_MINIMAL).writerow(headers)

    ssh_host = (os.getenv("STAGE_SSH_HOST") or os.getenv("REMOTE_SSH_HOST") or os.getenv("SSH_HOST", "")).strip("'\"")
    ssh_user = (os.getenv("SSH_USER") or os.getenv("REMOTE_SSH_USER", "")).strip("'\"")
    ssh_key_path = (os.getenv("SSH_KEY_PATH") or os.getenv("REMOTE_SSH_KEY_PATH", "")).strip("'\"")
    ssh_passphrase = (os.getenv("SSH_KEY_PASSPHRASE") or os.getenv("REMOTE_SSH_KEY_PASSPHRASE", "")).strip("'\"")

    db_host = (os.getenv("STAGE_DB_HOST") or os.getenv("DB_HOST", "")).strip("'\"")
    db_name = (os.getenv("DB_NAME", "mls_admin")).strip("'\"")
    db_user = (os.getenv("DB_USER", "")).strip("'\"")
    db_pass = (os.getenv("DB_PASSWORD", "")).strip("'\"")

    captured_mls_ids = set()
    total_match_count = 0

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

        logger.info(f"Establishing SSH tunnel to connect to database '{db_name}'...")
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
                cursor_name = f"master_bulk_sweep_stream_cursor_{int(time.time())}"
                with conn.cursor(name=cursor_name) as streaming_cur:
                    streaming_cur.itersize = 200

                    query = """
                            SELECT DISTINCT v.name               AS vendor_name, \
                                            m.id_str             AS mls_id_str, \
                                            m.id                 AS mls_id, \
                                            m.name               AS mls_name, \
                                            dp.name              AS download_protocol, \
                                            ctm.content_type     AS content_type, \
                                            ctm.content_sub_type AS content_sub_type, \
                                            'true'               AS is_enabled, \
                                            pm.properties        AS raw_properties, \
                                            pm.mapping_fields    AS raw_mapping_fields
                            FROM mls m
                                     JOIN mls_resource mr ON m.id = mr.mls_id
                                     JOIN map_rule_association mra ON mr.process_map_id = mra.process_map_id
                                     JOIN process_rule pr ON mra.process_rule_id = pr.id
                                     LEFT JOIN process_map pm ON pm.id = mra.process_map_id
                                     JOIN vendor v ON v.id = m.vendor_id
                                     JOIN content_type_map ctm ON ctm.id = mr.content_type_map_id
                                     JOIN download_mls dm ON dm.mls_id = m.id
                                     JOIN download_config dc ON dc.id = dm.download_config_id
                                     JOIN download_protocol dp ON dp.id = dc.download_protocol_id
                            WHERE m.mls_status_id = 2
                              AND pr.deleted_at IS NULL
                              AND pr.name = ANY (%s); \
                            """
                    streaming_cur.execute(query, (list(target_rules_set),))

                    with open(output_csv_path, mode="a", newline="", encoding="utf-8") as csv_file:
                        writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)

                        for row in streaming_cur:
                            (
                                vendor_name, mls_id_str, mls_id, mls_name,
                                download_protocol, content_type,
                                content_sub_type,
                                is_enabled, raw_properties, raw_mapping_fields
                            ) = row

                            if target_batch_mls and mls_id is not None:
                                try:
                                    if int(mls_id) not in target_batch_mls:
                                        continue
                                except ValueError:
                                    continue

                            if isinstance(raw_properties, str):
                                try:
                                    raw_properties = json.loads(raw_properties)
                                except json.JSONDecodeError:
                                    pass

                            if isinstance(raw_mapping_fields, str):
                                try:
                                    raw_mapping_fields = json.loads(raw_mapping_fields)
                                except json.JSONDecodeError:
                                    pass

                            if not raw_properties or not isinstance(raw_properties, dict):
                                continue

                            api_compliant_paths = set()
                            if isinstance(raw_mapping_fields, dict):
                                for category in ["required", "non_required"]:
                                    category_dict = raw_mapping_fields.get(category, {})
                                    if isinstance(category_dict, dict):
                                        for full_path in category_dict.keys():
                                            api_compliant_paths.add(str(full_path).strip())

                            discovered_nodes = extract_rules_recursive(raw_properties, target_rules_set)

                            for match in discovered_nodes:
                                struct_path = match["structural_path"]
                                node = match["field_value"]

                                if api_compliant_paths and struct_path not in api_compliant_paths:
                                    continue

                                rule_obj = node.get("rule", {})
                                rule_funcs = rule_obj.get("func", [])
                                if isinstance(rule_funcs, str):
                                    rule_funcs = [rule_funcs]

                                transformed_rule_funcs = []
                                for func in rule_funcs:
                                    if func in blueprint_mapping:
                                        transformed_rule_funcs.append(blueprint_mapping[func])
                                    else:
                                        transformed_rule_funcs.append(func)

                                def format_func_column(val):
                                    if val is None or val == "" or val == []:
                                        return ""
                                    if isinstance(val, list):
                                        return json.dumps([str(v) for v in val if v])
                                    return json.dumps([str(val)])

                                def format_param_column(val_data):
                                    if val_data is None or val_data == "" or val_data == []:
                                        return ""
                                    return json.dumps(val_data)

                                writer.writerow([
                                    vendor_name,
                                    mls_id_str,
                                    mls_id,
                                    mls_name,
                                    download_protocol,
                                    content_type,
                                    content_sub_type,
                                    is_enabled,
                                    struct_path,
                                    format_func_column(node.get("mapper", {}).get("func")),
                                    format_param_column(node.get("mapper", {}).get("func_data")),
                                    format_func_column(node.get("transform", {}).get("func")),
                                    format_param_column(node.get("transform", {}).get("func_data")),
                                    format_func_column(transformed_rule_funcs),
                                    format_param_column(rule_obj.get("func_data")),
                                    format_func_column(node.get("enhance", {}).get("func")),
                                    format_param_column(node.get("enhance", {}).get("func_data")),
                                ])
                                total_match_count += 1

                                if mls_id is not None:
                                    try:
                                        captured_mls_ids.add(int(mls_id))
                                    except ValueError:
                                        pass

                    # STRICT MANIFEST GENERATION: Write ONLY sources that actually required mutations
                    with open(txt_output_path, mode="w", encoding="utf-8") as txt_f:
                        for unique_id in sorted(list(captured_mls_ids)):
                            txt_f.write(f"{unique_id}\n")

                if target_batch_mls:
                    no_op_mls_ids = target_batch_mls.difference(captured_mls_ids)
                    if no_op_mls_ids:
                        logger.info(
                            f"ℹ️ Found {len(no_op_mls_ids)} MLS source(s) requiring 0 rule mutations: {sorted(list(no_op_mls_ids))}")
                        update_ledger_directly(temp_data_dir, no_op_mls_ids)

                logger.info("============================================================")
                logger.info("🚀 STAGE 4 COMPLETE: ARCHITECTURAL EXTRACTION ENGINE SUCCESSFUL")
                logger.info("============================================================")
                logger.info(f"Total Production Configurations Mutated : {total_match_count}")
                logger.info(f"Master File Ingress Destination (CSV)   : {output_csv_path}")
                logger.info(f"Unified Isolated Promotion Manifest     : {txt_output_path}")
                logger.info("============================================================")

    except Exception as e:
        logger.error(f"Critical Failure: {e}", exc_info=True)


if __name__ == "__main__":
    run_production_extractor()
