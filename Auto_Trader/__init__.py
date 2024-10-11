import os

import sys
import time
from datetime import datetime, timedelta
from functools import *
from multiprocessing import Process, Queue
import subprocess
import logging
from logging.handlers import *
import shutil
import talib
import traceback

import pandas as pd
import requests
from datetime import datetime, timedelta
from kiteconnect import KiteConnect
import pandas as pd
import json
import numpy as np
import pandas_market_calendars as mcal
from functools import lru_cache
from retry import retry
from zoneinfo import ZoneInfo

from .kite_ticker import run_ticker
from .Build_Master import create_master
from .rt_compute import Apply_Rules
from .utils import *
from .updater import Updater
from .my_secrets import *

import os
import logging
from logging.handlers import RotatingFileHandler

# Create a Logger for the package
logger = logging.getLogger("Auto_Trade_Logger")

# Format for Saving
formatter = logging.Formatter("[%(asctime)s] {%(filename)s %(funcName)s:%(lineno)d %(threadName)s} %(levelname)s - %(message)s")

# Set the Log Level
logger.setLevel(logging.INFO)

# Console Handler for Warnings and higher
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Create Folders for Logs
os.makedirs("log", exist_ok=True)

# Actual file paths and settings
OUTPUT_LOG_PATH = "log/output.log"
ERROR_LOG_PATH = "log/error.log"
MAXBYTES = 10 * 1024 * 1024  # 10 MB log file size
BACKUPCOUNT = 10  # Keep 10 backup log files

# Setting up RotatingFileHandler for info logs
INFO_filehandler = RotatingFileHandler(
    OUTPUT_LOG_PATH, 
    maxBytes=MAXBYTES, 
    backupCount=BACKUPCOUNT, 
    delay=True
)
INFO_filehandler.setLevel(logging.INFO)
INFO_filehandler.setFormatter(formatter)
logger.addHandler(INFO_filehandler)

# Setting up RotatingFileHandler for error logs
ERROR_filehandler = RotatingFileHandler(
    ERROR_LOG_PATH, 
    maxBytes=MAXBYTES,
    backupCount=BACKUPCOUNT, 
    delay=True
)
ERROR_filehandler.setLevel(logging.ERROR)
ERROR_filehandler.setFormatter(formatter)
logger.addHandler(ERROR_filehandler)

# Disable propagation to prevent duplicate logs from being passed to the root logger
logger.propagate = False