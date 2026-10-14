"""Tests for StateManager in src/utils/state.py."""
from __future__ import annotations
import json
from pathlib import Path
import pytest

from src.utils.exceptions import StateError
from src.utils.state import StateManager


@pytest.fixture()
def state_dir(tmp_path: Path) -> Path:
    return tmp_path / "state"


@pytest.fixture()
def state(state_dir: Path) -> StateManager:
    return StateManager(state_dir)


# 1. Fresh directory returns None.
def test_fresh_directory_returns_none(state: StateManager):
    assert state.get_watermark("any_key") is None
    assert state.get_watermark("products_updated_at") is None


# 2. Set/get round trip.
def test_set_get_round_trip(state: StateManager):
    state.set_watermark("products_updated_at", "2025-01-01T00:00:00")
    assert state.get_watermark("products_updated_at") == "2025-01-01T00:00:00"


# 3. Multiple keys coexist.
def test_multiple_keys_coexist(state: StateManager):
    state.set_watermark("products_updated_at", "2025-01-01T00:00:00")
    state.set_watermark("orders_date", "2025-01-02")
    state.set_watermark("customers_hash", "abc123xyz")

    assert state.get_watermark("products_updated_at") == "2025-01-01T00:00:00"
    assert state.get_watermark("orders_date") == "2025-01-02"
    assert state.get_watermark("customers_hash") == "abc123xyz"


# 4. Corrupt watermarks.json raises StateError (both get and set), and the file is left untouched.
@pytest.mark.parametrize(
    "corrupt_content",
    [
        b"{invalid json content",
        b"[1, 2, 3]",  # Valid JSON, but not a JSON object (dict)
        b"\"just a string\"",
        b"\xff\xfe\x00\x00corrupt utf8",
    ],
)
def test_corrupt_watermarks_file_raises_state_error_and_leaves_file_untouched(
    state: StateManager, state_dir: Path, corrupt_content: bytes
):
    watermark_file = state_dir / "watermarks.json"
    watermark_file.write_bytes(corrupt_content)

    # get_watermark raises StateError with file path
    with pytest.raises(StateError) as exc_info_get:
        state.get_watermark("any_key")
    assert str(watermark_file) in str(exc_info_get.value)
    assert watermark_file.read_bytes() == corrupt_content

    # set_watermark raises StateError with file path
    with pytest.raises(StateError) as exc_info_set:
        state.set_watermark("any_key", "value")
    assert str(watermark_file) in str(exc_info_set.value)
    assert watermark_file.read_bytes() == corrupt_content


# 5. record_run appends exactly one JSON line per call to runs.jsonl.
def test_record_run_appends_one_json_line_per_call(state: StateManager, state_dir: Path):
    run1 = {"run_id": "run-001", "status": "SUCCESS", "records_processed": 100}
    run2 = {"run_id": "run-002", "status": "FAILED", "error": "Disk full"}

    state.record_run(run1)
    state.record_run(run2)

    runs_file = state_dir / "runs.jsonl"
    assert runs_file.exists()

    lines = runs_file.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0]) == run1
    assert json.loads(lines[1]) == run2


# 6. A failure before os.replace leaves no partial/changed watermarks.json and no stray .tmp files.
def test_failure_before_replace_leaves_clean_state(
    state: StateManager, state_dir: Path, monkeypatch: pytest.MonkeyPatch
):
    # Set an initial valid watermark
    state.set_watermark("products_updated_at", "2025-01-01T00:00:00")
    watermark_file = state_dir / "watermarks.json"
    initial_bytes = watermark_file.read_bytes()

    def mock_replace(src, dst):
        raise OSError("Simulated crash right before replace")

    monkeypatch.setattr("src.utils.state.os.replace", mock_replace)

    with pytest.raises(OSError, match="Simulated crash right before replace"):
        state.set_watermark("products_updated_at", "2025-01-02T00:00:00")

    # Verify watermarks.json is unchanged
    assert watermark_file.read_bytes() == initial_bytes

    # Verify no .tmp files remain in state directory
    tmp_files = list(state_dir.glob("*.tmp"))
    assert tmp_files == []


def test_failure_on_first_write_leaves_no_watermarks_file_and_no_stray_tmp(
    state: StateManager, state_dir: Path, monkeypatch: pytest.MonkeyPatch
):
    watermark_file = state_dir / "watermarks.json"
    assert not watermark_file.exists()

    def mock_replace(src, dst):
        raise OSError("Simulated crash right before replace on initial write")

    monkeypatch.setattr("src.utils.state.os.replace", mock_replace)

    with pytest.raises(OSError, match="Simulated crash right before replace on initial write"):
        state.set_watermark("products_updated_at", "2025-01-01T00:00:00")

    assert not watermark_file.exists()
    assert list(state_dir.glob("*.tmp")) == []
