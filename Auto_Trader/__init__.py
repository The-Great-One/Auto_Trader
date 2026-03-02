import logging
import os
import traceback
from logging.handlers import RotatingFileHandler

from .Build_Master import create_master as create_master
from .kite_ticker import run_ticker as run_ticker
from .rt_compute import Apply_Rules as Apply_Rules
from .updater import Updater as Updater
from .utils import is_Market_Open as is_Market_Open

logger = logging.getLogger("Auto_Trade_Logger")
logger.setLevel(logging.INFO)

if not logger.handlers:
    formatter = logging.Formatter(
        "[%(asctime)s] {%(filename)s %(funcName)s:%(lineno)d %(threadName)s} %(levelname)s - %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    os.makedirs("log", exist_ok=True)

    output_log_path = "log/output.log"
    error_log_path = "log/error.log"
    max_bytes = 10 * 1024 * 1024
    backup_count = 10

    info_file_handler = RotatingFileHandler(
        output_log_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        delay=True,
    )
    info_file_handler.setLevel(logging.INFO)
    info_file_handler.setFormatter(formatter)
    logger.addHandler(info_file_handler)

    error_file_handler = RotatingFileHandler(
        error_log_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        delay=True,
    )
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(formatter)
    logger.addHandler(error_file_handler)

logger.propagate = False

__all__ = [
    "Apply_Rules",
    "Updater",
    "create_master",
    "is_Market_Open",
    "logger",
    "logging",
    "run_ticker",
    "traceback",
]
