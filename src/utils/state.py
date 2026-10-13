"""Watermark and run-state manager. Persists to JSON files in state/."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path


class StateManager:
    def __init__(self, state_dir: Path):
        self._dir = state_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._watermark_file = self._dir / "watermarks.json"
        self._runs_file = self._dir / "runs.jsonl"

    # ── watermarks ──────────────────────────────────────────────────────────
    def get_watermark(self, source: str) -> str | None:
        if not self._watermark_file.exists():
            return None
        data = json.loads(self._watermark_file.read_text())
        return data.get(source)

    def set_watermark(self, source: str, value: str) -> None:
        data: dict = {}
        if self._watermark_file.exists():
            data = json.loads(self._watermark_file.read_text())
        data[source] = value
        self._watermark_file.write_text(json.dumps(data, indent=2))

    # ── run history ─────────────────────────────────────────────────────────
    def record_run(self, metadata: dict) -> None:
        with self._runs_file.open("a") as f:
            f.write(json.dumps(metadata) + "\n")
