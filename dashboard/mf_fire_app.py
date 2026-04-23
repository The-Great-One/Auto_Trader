#!/usr/bin/env python3
"""MF FIRE Planner Streamlit wrapper — runs the core MF app."""
from __future__ import annotations

import runpy
from pathlib import Path

TARGET = Path(__file__).resolve().parent / "mf_app_core.py"
runpy.run_path(str(TARGET), run_name="__main__")