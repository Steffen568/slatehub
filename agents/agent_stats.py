#!/usr/bin/env python3
"""
Agent 1 — Parallel stats runner.
Runs all 5 stats scripts simultaneously using ThreadPoolExecutor.
Each script is independent; failures are reported but don't stop others.
"""
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from agents.logger import RunLogger

REPO_ROOT = Path(__file__).parent.parent

STATS_SCRIPTS = [
    ('load_rosters.py',              'Rosters'),
    ('load_stats.py',                'Batter & Pitcher Stats'),
    ('load_arsenal.py',              'Pitch Arsenal'),
    ('load_bat_tracking.py',         'Bat Tracking'),
    ('load_savant_splits.py',        'Savant Splits'),
    ('load_batter_pitch_splits.py',  'Batter vs Pitch Type Splits'),
]


def _run_single(script: str, label: str) -> dict:
    """Run a single stats script, capture output, return result dict."""
    start = time.time()
    env = {**os.environ, 'PYTHONIOENCODING': 'utf-8'}
    result = subprocess.run(
        ['py', '-3.12', str(REPO_ROOT / script)],
        capture_output=True,
        text=True,
        encoding='utf-8',
        cwd=str(REPO_ROOT),
        env=env,
    )
    elapsed = time.time() - start
    return {
        'label': label,
        'script': script,
        'success': result.returncode == 0,
        'elapsed': elapsed,
        'stdout': result.stdout,
        'stderr': result.stderr,
    }


def run(logger: RunLogger) -> RunLogger:
    """Run all stats scripts in parallel. Returns logger with results."""
    print(f"\n{'='*55}")
    print(f"  Agent 1 \u2014 Stats (parallel, {len(STATS_SCRIPTS)} scripts)")
    print(f"{'='*55}")

    futures = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        for script, label in STATS_SCRIPTS:
            future = executor.submit(_run_single, script, label)
            futures[future] = (script, label)

        for future in as_completed(futures):
            res = future.result()
            label = res['label']
            script = res['script']

            # Print buffered output prefixed with label so output is readable
            if res['stdout']:
                for line in res['stdout'].splitlines():
                    print(f"  [{label}] {line}")
            if not res['success'] and res['stderr']:
                for line in res['stderr'].splitlines():
                    print(f"  [{label}][ERR] {line}")

            logger.record(label, res['success'], res['elapsed'])

            if not res['success']:
                last_lines = (
                    '\n'.join(res['stderr'].splitlines()[-3:])
                    if res['stderr'] else '(no stderr)'
                )
                logger.add_lesson(
                    title=f"{script} failed during --stats run",
                    what_happened=last_lines,
                    rule="Check that py -3.12 and all dependencies are installed. Check API availability.",
                )

    return logger
