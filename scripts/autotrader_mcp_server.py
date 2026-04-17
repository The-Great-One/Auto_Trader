#!/usr/bin/env python3
"""Read-only MCP server for Auto_Trader reports and lab outputs.

First cut focuses on safe visibility tools only. It exposes streamable HTTP
transport by default so external MCP clients can inspect the latest Auto_Trader
state without being able to mutate trading code or runtime config.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

ROOT = Path(__file__).resolve().parents[1]
REPORTS = Path(os.getenv("AT_MCP_REPORTS_DIR", str(ROOT / "reports")))
REPORTS.mkdir(parents=True, exist_ok=True)

mcp = FastMCP(
    name="Auto_Trader MCP",
    instructions="Read-only MCP server for Auto_Trader reports, paper shadow outputs, and lab results.",
    host=os.getenv("AT_MCP_HOST", "127.0.0.1"),
    port=int(os.getenv("AT_MCP_PORT", "8765")),
    streamable_http_path=os.getenv("AT_MCP_PATH", "/mcp"),
    log_level=os.getenv("AT_MCP_LOG_LEVEL", "INFO"),
)


def _latest(pattern: str) -> Path | None:
    matches = sorted(REPORTS.glob(pattern))
    return matches[-1] if matches else None



def _read_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None



def _safe_report_path(name: str) -> Path:
    candidate = (REPORTS / Path(name).name).resolve()
    reports_root = REPORTS.resolve()
    if reports_root not in candidate.parents and candidate != reports_root:
        raise ValueError("report path escapes reports directory")
    return candidate



def _report_summary(path: Path) -> dict[str, Any]:
    return {
        "name": path.name,
        "path": str(path),
        "size_bytes": path.stat().st_size,
        "modified_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
    }


@mcp.tool()
def list_recent_reports(limit: int = 20) -> dict[str, Any]:
    """List recent report files from the reports directory."""
    limit = max(1, min(int(limit), 100))
    files = sorted(REPORTS.glob("*"), key=lambda p: p.stat().st_mtime, reverse=True)
    return {
        "reports_dir": str(REPORTS),
        "count": min(len(files), limit),
        "reports": [_report_summary(path) for path in files[:limit]],
    }


@mcp.tool()
def get_report_text(report_name: str, max_chars: int = 12000) -> dict[str, Any]:
    """Read a report file from reports/ by file name only."""
    path = _safe_report_path(report_name)
    if not path.exists():
        raise ValueError(f"Report not found: {report_name}")
    text = path.read_text(errors="ignore")
    return {
        **_report_summary(path),
        "truncated": len(text) > max_chars,
        "content": text[:max_chars],
    }


@mcp.tool()
def get_report_json(report_name: str) -> dict[str, Any]:
    """Read a JSON report from reports/ by file name only."""
    path = _safe_report_path(report_name)
    if not path.exists():
        raise ValueError(f"Report not found: {report_name}")
    payload = _read_json(path)
    if payload is None:
        raise ValueError(f"Report is not valid JSON: {report_name}")
    return {
        **_report_summary(path),
        "payload": payload,
    }


@mcp.tool()
def get_latest_scorecard() -> dict[str, Any]:
    """Get the latest daily scorecard JSON report."""
    path = _latest("daily_scorecard_*.json")
    return {"file": str(path) if path else None, "payload": _read_json(path)}


@mcp.tool()
def get_latest_daily_ops() -> dict[str, Any]:
    """Get the latest daily ops supervisor JSON report."""
    path = _latest("daily_ops_supervisor_*.json")
    return {"file": str(path) if path else None, "payload": _read_json(path)}


@mcp.tool()
def get_latest_options_supervisor() -> dict[str, Any]:
    """Get the latest options research supervisor JSON report."""
    path = _latest("options_research_supervisor_*.json")
    return {"file": str(path) if path else None, "payload": _read_json(path)}


@mcp.tool()
def get_latest_improvement_audit() -> dict[str, Any]:
    """Get the latest daily improvement audit JSON report."""
    path = _latest("daily_improvement_audit_*.json")
    return {"file": str(path) if path else None, "payload": _read_json(path)}


@mcp.tool()
def get_latest_equity_lab() -> dict[str, Any]:
    """Get the latest equity strategy lab JSON report."""
    path = _latest("strategy_lab_*.json")
    return {"file": str(path) if path else None, "payload": _read_json(path)}


@mcp.tool()
def get_latest_options_lab() -> dict[str, Any]:
    """Get the latest options strategy lab JSON report."""
    path = _latest("options_strategy_lab_*.json")
    return {"file": str(path) if path else None, "payload": _read_json(path)}


@mcp.tool()
def get_latest_paper_shadow() -> dict[str, Any]:
    """Get the latest equity paper shadow payload."""
    path = REPORTS / "paper_shadow_latest.json"
    return {"file": str(path), "payload": _read_json(path)}


@mcp.tool()
def get_latest_options_paper_shadow() -> dict[str, Any]:
    """Get the latest options paper shadow payload."""
    path = REPORTS / "paper_shadow_options_latest.json"
    return {"file": str(path), "payload": _read_json(path)}


@mcp.tool()
def get_status_snapshot() -> dict[str, Any]:
    """Small combined snapshot of current reports for dashboards/agents."""
    return {
        "generated_at": datetime.now().isoformat(),
        "latest_scorecard": get_latest_scorecard(),
        "latest_daily_ops": get_latest_daily_ops(),
        "latest_options_supervisor": get_latest_options_supervisor(),
        "latest_improvement_audit": get_latest_improvement_audit(),
        "latest_equity_lab": get_latest_equity_lab(),
        "latest_options_lab": get_latest_options_lab(),
        "latest_paper_shadow": get_latest_paper_shadow(),
        "latest_options_paper_shadow": get_latest_options_paper_shadow(),
    }


if __name__ == "__main__":
    transport = os.getenv("AT_MCP_TRANSPORT", "streamable-http")
    if transport == "stdio":
        mcp.run(transport="stdio")
    else:
        mount_path = os.getenv("AT_MCP_PATH", "/mcp")
        mcp.run(transport=transport, mount_path=mount_path)
