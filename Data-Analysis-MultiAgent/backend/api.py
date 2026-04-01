"""Programmatic entry point for running the analysis pipeline from the command line.

Usage:
    python -m backend.api <path_to_csv>
"""
import sys
import os
import json
import logging

# Ensure the project root is on the path so "backend.*" imports resolve
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.core.graph import run_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m backend.api <path_to_csv>")
        sys.exit(1)

    file_path = sys.argv[1]
    if not os.path.isfile(file_path):
        print(f"Error: File not found: {file_path}")
        sys.exit(1)

    logger.info("Starting analysis for: %s", file_path)
    state = run_pipeline(file_path)

    result = {
        "stats_summary": state.stats_summary,
        "insights": state.insights,
        "charts_generated": list(state.charts.keys()),
        "completed_agents": state.completed_agents,
        "errors": state.errors,
    }

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
