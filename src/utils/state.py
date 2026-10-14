"""Watermark and run-state manager. Persists to JSON files in state/."""
from __future__ import annotations
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from src.utils.exceptions import StateError


class StateManager:
    def __init__(self, state_dir: Path):
        self._dir = state_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._watermark_file = self._dir / "watermarks.json"
        self._runs_file = self._dir / "runs.jsonl"

    # ── watermarks ──────────────────────────────────────────────────────────
    def _read_watermarks(self) -> dict:
        """Load watermarks.json; fail loudly if it is corrupt (never reset to epoch)."""
        if not self._watermark_file.exists():
            return {}
        try:
            data = json.loads(self._watermark_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise StateError(
                f"Corrupt watermark file {self._watermark_file}: {exc}"
            ) from exc
        if not isinstance(data, dict):
            raise StateError(
                f"Corrupt watermark file {self._watermark_file}: expected a JSON object"
            )
        return data

    def get_watermark(self, source: str) -> str | None:
        return self._read_watermarks().get(source)

    def set_watermark(self, source: str, value: str) -> None:
        data = self._read_watermarks()
        data[source] = value
        fd, tmp_name = tempfile.mkstemp(
            dir=self._dir, prefix="watermarks.", suffix=".tmp"
        )
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(json.dumps(data, indent=2))
            os.replace(tmp_path, self._watermark_file)
        finally:
            tmp_path.unlink(missing_ok=True)

    # ── run history ─────────────────────────────────────────────────────────
    def record_run(self, metadata: dict) -> None:
        with self._runs_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(metadata) + "\n")
