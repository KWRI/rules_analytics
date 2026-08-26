"""
Pipeline Stage 5: Staging Workspace Operator (Configuration Bulk Modification Framework).

This script executes bulk structural field and rule migrations across targeted database tables.
It supports safe evaluation passes via a validation simulation mode (`--debug`) to output verbose
dry-run traces without mutating records. When executed with live parameters (`--do-update`), it mutates
the nested configuration nodes in the Staging database to reflect the new PascalCase specifications.

Supports target MLS batch notice via 'temp-data/api_mls.txt' and/or 'temp-data/rets_mls.txt'
(via rules_utils.py).
Logs execution events to 'logs/pipeline_YYYY-MM-DD.log'.
"""

import sys
import os
import importlib.util
from pathlib import Path
from dotenv import load_dotenv

from rules_utils import load_target_mls
from pipeline_logger import setup_logger

logger = setup_logger("Stage5_BulkUpdate")

# 1. Clear out active environment memory just to be safe
forbidden_keys = [
    "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST",
    "SSH_HOST", "SSH_USER", "SSH_KEY_PATH", "SSH_KEY_PASSPHRASE", "REPO_PATH"
]
for key in list(os.environ.keys()):
    if key in forbidden_keys:
        del os.environ[key]

# 2. Force load ONLY the bulk tool environment variables into memory
current_dir = os.path.dirname(os.path.abspath(__file__))
bulk_env_path = os.path.join(current_dir, ".env.bulk")
if os.path.exists(bulk_env_path):
    load_dotenv(dotenv_path=bulk_env_path, override=True)

# 3. Base paths loaded dynamically from .env.bulk
tools_repo = os.getenv("TOOLS_REPO_PATH")
utils_repo = os.getenv("UTILS_REPO_PATH")
snowflake_repo = os.getenv("SNOWFLAKE_REPO_PATH")

if not tools_repo or not utils_repo:
    logger.error("TOOLS_REPO_PATH or UTILS_REPO_PATH missing from .env.bulk configuration.")
    sys.exit(1)

# 4. Core path injections
sys.path.insert(0, os.path.join(tools_repo, "bulk_map_tool"))
sys.path.insert(0, utils_repo)
sys.path.insert(0, tools_repo)

if snowflake_repo and os.path.exists(snowflake_repo):
    sys.path.insert(0, snowflake_repo)

# 5. Pydantic V2 to V1 Bridge + ANTI-.ENV DISCOVERY LAYER:
import pydantic
import pydantic_settings


class CleanSettings(pydantic_settings.BaseSettings):
    model_config = pydantic_settings.SettingsConfigDict(
        env_file=None,  # Hard block against reading any physical .env file
        extra="ignore"  # Safely ignore any stray keys instead of crashing
    )


pydantic.BaseSettings = CleanSettings
sys.modules['pydantic'].BaseSettings = CleanSettings

# 6. NAMESPACE SEPARATION INTERCEPT (Snowflake constants)
try:
    sf_inner = None
    if snowflake_repo and os.path.exists(snowflake_repo):
        for item in os.listdir(snowflake_repo):
            if "snowflake" in item.lower():
                sf_inner = os.path.join(snowflake_repo, item)
                break

    if sf_inner:
        sf_constants_path = os.path.join(sf_inner, "constants.py")
        if os.path.exists(sf_constants_path):
            for target_mod in ["eim_snowflake_id.constants",
                               "eim_snowfllake_id.constants"]:
                spec = importlib.util.spec_from_file_location(target_mod,
                                                              sf_constants_path)
                mod = importlib.util.module_from_spec(spec)
                sys.modules[target_mod] = mod
                spec.loader.exec_module(mod)
except Exception as e:
    logger.warning(f"Snowflake namespace intercept skipped: {e}")

if __name__ == "__main__":
    try:
        # Check if MLS batch target is active in temp-data/
        temp_data_dir = Path(current_dir) / "temp-data"
        target_batch_mls = load_target_mls(temp_data_dir)
        if target_batch_mls:
            logger.info(f"🎯 [STAGING BULK UPDATE GUARD] Active MLS targets detected: {sorted(list(target_batch_mls))}")

        import bulk_map_tool

        passthrough_args = sys.argv[1:]

        if passthrough_args:
            # DYNAMIC RESOLUTION: Auto-resolve simple filenames to the local workspace temp-data path
            if "--input-file" in passthrough_args:
                idx = passthrough_args.index("--input-file") + 1
                if idx < len(passthrough_args):
                    input_val = passthrough_args[idx]

                    # If it's just a raw filename, point directly to your rules_analytics temp-data folder
                    if not input_val.startswith("C:") and "temp-data" not in input_val:
                        resolved_path = Path(current_dir) / "temp-data" / input_val
                        passthrough_args[idx] = str(resolved_path)

            logger.info(f"Handing off execution to bulk_map_tool with args: {' '.join(passthrough_args)}")

            # Reconstruct sys.argv dynamically and handoff directly to core tool main engine execution
            sys.argv = ["bulk_map_tool.py"] + passthrough_args
            bulk_map_tool.main()

        else:
            logger.error("No runtime arguments provided.")
            sys.exit(1)

    except ImportError as e:
        logger.error(f"Missing Internal System Library Dependency: {e}", exc_info=True)
    except Exception as err:
        logger.error(f"Execution failure during runtime mapping sequence: {err}", exc_info=True)
