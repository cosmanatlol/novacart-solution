"""
Cleanup script for the NovaCart pipeline lab.

Removes all pipeline-generated output so the next run starts from a clean
slate. Run this after run_everything.py or any individual pipeline run.

Deletes (equivalent to: rm -rf data/bronze data/silver data/gold
data/quarantine state logs dashboard):
    data/bronze       data/silver       data/gold       data/quarantine
    state             logs              dashboard

Source/landing data (data/landing/*) is NOT touched — regenerate that with
scripts/generate_sample_data.py if needed.

Usage:
    python scripts/cleanup.py
"""
from __future__ import annotations
import shutil
from pathlib import Path

ROOT = Path(__file__).parent.parent

TARGETS = [
    ROOT / "data" / "bronze",
    ROOT / "data" / "silver",
    ROOT / "data" / "gold",
    ROOT / "data" / "quarantine",
    ROOT / "state",
    ROOT / "logs",
    ROOT / "dashboard",
]


def main() -> None:
    for path in TARGETS:
        if path.exists():
            shutil.rmtree(path)
            print(f"  removed {path.relative_to(ROOT)}")
        else:
            print(f"  skip    {path.relative_to(ROOT)} (not present)")

    print("\nCleanup complete. Landing data left intact.")


if __name__ == "__main__":
    main()
