#!/usr/bin/env python3
"""
Agent 4 — Post-Contest Research

Runs after games complete to analyze projection accuracy, ownership accuracy,
sim pool quality, and contest results. Surfaces actionable improvements.

Called by refresh_all.py --postgame (after actuals are loaded).
"""
import os
import subprocess
import time
from pathlib import Path

from agents.logger import RunLogger

REPO_ROOT = Path(__file__).parent.parent


def _run_script(script: str, label: str, logger: RunLogger) -> bool:
    """Run a single script, record result, return success bool."""
    print(f"\n{'='*55}")
    print(f"  {label}")
    print(f"{'='*55}")
    start = time.time()
    env = {**os.environ, 'PYTHONIOENCODING': 'utf-8'}
    result = subprocess.run(
        ['py', '-3.12'] + script.split(),
        capture_output=False,
        cwd=str(REPO_ROOT),
        env=env,
    )
    elapsed = time.time() - start
    success = result.returncode == 0
    logger.record(label, success, elapsed)
    return success


def run(logger: RunLogger) -> RunLogger:
    """Run post-contest research pipeline. Returns logger with results."""
    from datetime import date, timedelta

    print(f"\n{'='*55}")
    print(f"  Agent 4 — Post-Contest Research")
    print(f"{'='*55}")

    # Ensure actuals are loaded
    _run_script('load_actuals.py', 'Load Actual DK Points', logger)

    # Run research analysis with 7-day range for stronger backtesting sample
    today = date.today()
    range_start = str(today - timedelta(days=7))
    range_end = str(today)
    _run_script(f'research_accuracy.py --range {range_start} {range_end}',
                'Research — Accuracy Analysis (7-day)', logger)

    return logger
