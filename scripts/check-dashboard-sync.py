#!/usr/bin/env python
"""Verify the inline Grafana dashboard JSON in docker-compose.yml matches
the standalone copy in monitoring/grafana/dashboards/api-perf.json.

Background: the dashboard JSON exists in two places — inlined as a Docker
Config (configs.grafana_api_perf_dashboard.content) and as a standalone
file. Grafana reads the inlined version. If they drift, edits to the
standalone file silently never reach Grafana.

Exit 0  when both copies parse to the same JSON (or either is missing).
Exit 1  when drift is detected.

Run manually:
    python scripts/check-dashboard-sync.py

Or wire into git pre-commit (see .githooks/pre-commit).
"""
import json
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("warning: PyYAML not installed; skipping dashboard sync check",
          file=sys.stderr)
    sys.exit(0)

REPO = Path(__file__).resolve().parent.parent
COMPOSE = REPO / "docker-compose.yml"
STANDALONE = REPO / "monitoring" / "grafana" / "dashboards" / "api-perf.json"

if not COMPOSE.exists() or not STANDALONE.exists():
    sys.exit(0)

with COMPOSE.open(encoding="utf-8") as f:
    compose = yaml.safe_load(f) or {}

inline_str = (compose.get("configs", {})
                     .get("grafana_api_perf_dashboard", {})
                     .get("content"))

if not inline_str:
    # Inline block missing entirely — nothing to compare against.
    sys.exit(0)

try:
    inline_json = json.loads(inline_str)
    standalone_json = json.loads(STANDALONE.read_text(encoding="utf-8"))
except json.JSONDecodeError as e:
    print(f"error: invalid JSON in dashboard source: {e}", file=sys.stderr)
    sys.exit(1)

if inline_json == standalone_json:
    sys.exit(0)

print("error: Grafana dashboard JSON drift detected between:",
      file=sys.stderr)
print(f"  - {STANDALONE.relative_to(REPO).as_posix()}", file=sys.stderr)
print("  - configs.grafana_api_perf_dashboard.content in docker-compose.yml",
      file=sys.stderr)
print("", file=sys.stderr)
print("Pick one as source of truth and copy its content to the other,",
      file=sys.stderr)
print("then recommit. Grafana reads the inlined copy in docker-compose.yml.",
      file=sys.stderr)
sys.exit(1)
