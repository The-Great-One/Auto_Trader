#!/usr/bin/env python3
"""MF FIRE Planner Streamlit wrapper — runs the core MF app."""
from __future__ import annotations

import os
import runpy
from pathlib import Path

import streamlit as st


def _authorized() -> bool:
    password = os.getenv("MF_FIRE_PASSWORD") or os.getenv("DASH_AUTH_PASSWORD") or ""
    if not password.strip():
        return True
    if st.session_state.get("mf_fire_auth_ok"):
        return True
    st.set_page_config(page_title="MF FIRE Planner", layout="wide")
    st.title("MF FIRE Planner")
    st.caption("Password required")
    with st.form("mf_fire_auth"):
        entered = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Unlock")
    if submitted and entered == password:
        st.session_state["mf_fire_auth_ok"] = True
        st.rerun()
    if submitted:
        st.error("Wrong password")
    return False


if _authorized():
    TARGET = Path(__file__).resolve().parent / "mf_app_core.py"
    runpy.run_path(str(TARGET), run_name="__main__")
