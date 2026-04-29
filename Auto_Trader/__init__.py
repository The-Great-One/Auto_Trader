import logging
import os
import tempfile
import traceback
from logging.handlers import RotatingFileHandler

# AT_RESEARCH_MODE=1 skips all Kite/broker imports so backtesting scripts can
# run on machines without a live Kite session (e.g. dedicated backtesting servers).
_AT_RESEARCH_MODE = os.getenv("AT_RESEARCH_MODE", "0").strip() in {"1", "true", "yes"}

if not _AT_RESEARCH_MODE:
    from .Build_Master import create_master as create_master
    from .kite_ticker import run_ticker as run_ticker
    from .rt_compute import Apply_Rules as Apply_Rules
    from .updater import Updater as Updater
    from .utils import is_Market_Open as is_Market_Open
else:
    create_master = None  # type: ignore[assignment]
    run_ticker = None  # type: ignore[assignment]
    Apply_Rules = None  # type: ignore[assignment]
    Updater = None  # type: ignore[assignment]
    is_Market_Open = None  # type: ignore[assignment]

logger = logging.getLogger("Auto_Trade_Logger")
logger.setLevel(logging.INFO)


def _resolve_log_path(filename: str) -> str:
    preferred_dir = os.path.abspath("log")
    preferred_path = os.path.join(preferred_dir, filename)
    os.makedirs(preferred_dir, exist_ok=True)

    if os.path.exists(preferred_path):
        if os.access(preferred_path, os.W_OK):
            return preferred_path
    elif os.access(preferred_dir, os.W_OK):
        return preferred_path

    fallback_dir = os.path.join(
        os.getenv("AT_LOG_FALLBACK_DIR", tempfile.gettempdir()),
        "Auto_Trader",
    )
    os.makedirs(fallback_dir, exist_ok=True)
    return os.path.join(fallback_dir, filename)


if not logger.handlers:
    formatter = logging.Formatter(
        "[%(asctime)s] {%(filename)s %(funcName)s:%(lineno)d %(threadName)s} %(levelname)s - %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    max_bytes = 10 * 1024 * 1024
    backup_count = 10
    disable_file_logging = os.getenv("AT_DISABLE_FILE_LOGGING", "0").strip().lower() in {
        "1",
        "true",
        "yes",
    }

    if not disable_file_logging:
        output_log_path = _resolve_log_path("output.log")
        error_log_path = _resolve_log_path("error.log")

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
