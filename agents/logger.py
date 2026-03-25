#!/usr/bin/env python3
"""
Shared logging utility for the SlateHub agent pipeline.
Thread-safe — safe to use from concurrent agent threads.
"""
import sys
import threading
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

LESSONS_FILE = Path(__file__).parent.parent / 'tasks' / 'lessons.md'


class RunLogger:
    def __init__(self, mode: str):
        self.mode = mode
        self.start_time = datetime.now()
        self.records: list = []
        self._lock = threading.Lock()

    def record(self, label: str, success: bool, elapsed: float, notes: str = ""):
        """Thread-safe record of a script result."""
        with self._lock:
            self.records.append({
                'label': label,
                'success': success,
                'elapsed': elapsed,
                'notes': notes,
            })

    def add_lesson(self, title: str, what_happened: str, rule: str):
        """Append a lesson to tasks/lessons.md immediately. Skips duplicates."""
        lesson_text = (
            f"\n### {title}\n"
            f"**What happened:** {what_happened}\n"
            f"**Rule:** {rule}\n"
        )
        with self._lock:
            try:
                if LESSONS_FILE.exists():
                    existing = LESSONS_FILE.read_text(encoding='utf-8')
                    if f"### {title}" in existing:
                        return  # Already exists — skip duplicate
                with open(LESSONS_FILE, 'a', encoding='utf-8') as f:
                    f.write(lesson_text)
                print(f"  [lessons.md] Added lesson: {title}")
            except Exception as e:
                print(f"  WARNING: Could not write to lessons.md: {e}")
                print(f"  Lesson content:\n{lesson_text}")

    def print_summary(self):
        ts = datetime.now().strftime('%H:%M:%S')
        width = 55
        print(f"\n{'='*width}")
        print(f" DONE — {self.mode} [{ts}]")
        print(f"{'='*width}")
        with self._lock:
            records = list(self.records)
        for r in records:
            icon = '✓' if r['success'] else '✗'
            label = r['label']
            elapsed_str = f"{r['elapsed']:.1f}s"
            if r['success']:
                print(f" {icon} {label:<28} {elapsed_str:>6}")
            else:
                note = f"  ({r['notes']})" if r['notes'] else ''
                print(f" {icon} {label:<28} FAILED{note}")
        print()
        if self.all_passed():
            print("All phases OK.")
        else:
            failed_count = sum(1 for r in records if not r['success'])
            print(f"{failed_count} phase{'s' if failed_count > 1 else ''} FAILED — check output above.")
        print()

    def write_lessons_to_file(self):
        """No-op — lessons are written immediately by add_lesson()."""
        pass

    def all_passed(self) -> bool:
        with self._lock:
            return all(r['success'] for r in self.records)

    def any_failed(self) -> bool:
        with self._lock:
            return any(not r['success'] for r in self.records)

    def get_failures(self) -> list:
        with self._lock:
            return [r for r in self.records if not r['success']]
