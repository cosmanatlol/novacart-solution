"""
Cross-platform end-to-end runner. Works on macOS, Linux, and Windows. Installs dependencies.
Usage: python scripts/run_everything.py
"""
from __future__ import annotations
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent


def run(cmd: list[str], desc: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {desc}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, cwd=ROOT)
    if result.returncode != 0:
        print(f"\n[FAIL] Step failed: {desc}", file=sys.stderr)
        sys.exit(result.returncode)


def main():
    py = sys.executable

    run([py, "-m", "pip", "install", "-r", "requirements.txt"],
        "Step 1: Install requirements")

    run([py, "scripts/generate_sample_data.py"],
        "Step 2: Generate sample data")

    run([py, "-m", "src.pipeline", "--date", "2025-11-10", "--backfill", "3"],
        "Step 3: Run pipeline (Nov 7–10)")

    run([py, "scripts/build_dashboard.py"],
        "Step 4: Build dashboard")

    run([py, "-m", "pytest", "-v"],
        "Step 5: Run test suite")

    print("\n✓ All steps complete. Check data/ for pipeline output "
          "and dashboard/ for statistics and graphs.")


if __name__ == "__main__":
    main()
