"""
Pipeline Logging Framework.

Provides a unified logger that outputs formatted messages to both
the console and persistent daily log files in the 'logs/' directory.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime


def setup_logger(script_name: str) -> logging.Logger:
    """Configures and returns a logger instance for a given pipeline script."""
    LOGS_DIR = Path("logs")
    LOGS_DIR.mkdir(exist_ok=True)

    # Log file named by date (e.g., logs/pipeline_2026-07-30.log)
    today_str = datetime.now().strftime("%Y-%m-%d")
    log_file_path = LOGS_DIR / f"pipeline_{today_str}.log"

    logger = logging.getLogger(script_name)
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if logger is initialized multiple times
    if logger.hasHandlers():
        return logger

    # Formatting: [2026-07-30 14:22:01] [INFO] [script_name] Message
    formatter = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 1. File Handler (Persistent log)
    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 2. Stream Handler (Console output)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
