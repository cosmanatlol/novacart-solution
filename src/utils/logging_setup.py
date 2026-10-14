"""Structured JSON logging setup."""
from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

# Values here (run_id, stage) are added to EVERY log line automatically.
_context: dict = {}


def set_context(**kwargs) -> None:
    _context.update(kwargs)


def clear_context() -> None:
    _context.clear()


def get_logger(name: str, log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    # Reset handlers so every call writes to the CURRENT log_dir (no duplicates).
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    fh = logging.FileHandler(log_dir / "pipeline.jsonl")
    fh.setFormatter(_JsonFormatter())
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    logger.addHandler(sh)
    return logger


def log_event(logger: logging.Logger, level: str, event: str, **kwargs):
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **_context,
        **kwargs,
    }
    getattr(logger, level.lower())(json.dumps(payload))


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return record.getMessage()