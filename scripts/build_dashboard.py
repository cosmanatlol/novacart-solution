"""
Build a simple dashboard (statistics + graphs) from existing pipeline output.

READ-ONLY with respect to the pipeline: it reads state/runs.jsonl,
logs/pipeline.jsonl and data/quarantine/**/*.parquet, and writes only into
dashboard/ (created if missing).

Outputs:
    dashboard/summary.md   readable statistics tables
    dashboard/summary.json same statistics, machine-readable
    dashboard/*.png        row_funnel, quarantine_rate, quarantine_reasons,
                           stage_duration, customer_changes

Usage:
    python scripts/build_dashboard.py
"""
from __future__ import annotations
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless: no display needed
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.utils.config import Config  # noqa: E402

OUT_DIR = ROOT / "dashboard"

# source -> (ingest stage, silver stage, gold stage)
SOURCES: dict[str, tuple[str, str, str]] = {
    "orders":    ("ingest_orders",    "silver_orders",    "fact_orders"),
    "customers": ("ingest_customers", "silver_customers", "dim_customer"),
    "products":  ("ingest_products",  "silver_products",  "dim_product"),
}


# ── Loading ──────────────────────────────────────────────────────────────────

def read_jsonl(path: Path) -> list[dict]:
    """Read a JSONL file; skip blank or corrupt lines with a warning."""
    if not path.exists():
        return []
    rows: list[dict] = []
    with path.open(encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                print(f"  warning: {path.name} line {n} is not valid JSON, skipped",
                      file=sys.stderr)
                continue
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def categorise_reason(text: object) -> str:
    """Turn a multi-line Pydantic error into a short, groupable label."""
    lines = [ln.strip() for ln in str(text).split("\n") if ln.strip()]
    if len(lines) < 3:
        return lines[0][:80] if lines else "unknown"
    field, detail = lines[1], lines[2]
    detail = detail.split(" [type=")[0]
    detail = re.sub(r"^Value error, ", "", detail)
    detail = re.sub(r"\s*'[^']*'\s*", " ", detail)        # drop row-specific values
    detail = re.split(r": |, got |\s*\{", detail)[0].strip()
    return detail if field in detail else f"{field}: {detail}"


def load_quarantine_reasons(quarantine_dir: Path) -> dict[str, Counter]:
    """Count reason labels per source across every quarantine file on disk."""
    result: dict[str, Counter] = {}
    for source in SOURCES:
        counts: Counter = Counter()
        folder = quarantine_dir / source
        files = sorted(folder.rglob("*.parquet")) if folder.exists() else []
        for f in files:
            try:
                df = pd.read_parquet(f, columns=["_error_reason"])
            except (OSError, ValueError) as exc:
                print(f"  warning: could not read {f.name}: {exc}", file=sys.stderr)
                continue
            counts.update(categorise_reason(r) for r in df["_error_reason"])
        result[source] = counts
    return result


# ── Computation ──────────────────────────────────────────────────────────────

def pct(part: int | None, whole: int | None) -> float | None:
    if part is None or not whole:
        return None
    return round(100 * part / whole, 1)


def latest_run_per_date(runs: list[dict]) -> dict[str, dict]:
    """runs.jsonl is append-only, so the last record for a date wins."""
    by_date: dict[str, dict] = {}
    for run in runs:
        if run.get("date"):
            by_date[run["date"]] = run
    return dict(sorted(by_date.items()))


def source_flow(run: dict, source: str) -> dict:
    """Row counts for one source in one run. Missing values stay None."""
    stages = {s.get("stage"): s for s in run.get("stages", [])}
    ingest, silver, gold = (stages.get(n, {}) for n in SOURCES[source])
    rows_in = silver.get("rows_in")
    rows_out = silver.get("rows_out")
    quarantined = silver.get("rows_quarantined")
    deduped = (rows_in - rows_out - quarantined
               if None not in (rows_in, rows_out, quarantined) else None)
    return {
        "bronze": ingest.get("rows_out"),
        "silver_in": rows_in,
        "quarantined": quarantined,
        "deduped": deduped,
        "silver": rows_out,
        "gold": gold.get("rows_out"),
    }


def _sum_or_none(values: list[int | None]) -> int | None:
    present = [v for v in values if v is not None]
    return sum(present) if present else None


def aggregate_flow(per_date: dict[str, dict], source: str) -> dict:
    """Orders are per-date, so add dates up. Customers/products are snapshots that
    repeat every run, so use the latest run with data (never sum across runs)."""
    flows = [per_date[d][source] for d in per_date]
    if source == "orders":
        agg = {k: _sum_or_none([f[k] for f in flows]) for k in flows[0]} if flows else {}
    else:
        with_data = [f for f in flows if f["bronze"] is not None]
        agg = dict(with_data[-1]) if with_data else {}
    if agg:
        agg["quarantine_pct"] = pct(agg.get("quarantined"), agg.get("silver_in"))
        agg["retention_pct"] = pct(agg.get("gold"), agg.get("bronze"))
    return agg


def compute_stats(runs: list[dict], log_events: list[dict],
                  reasons: dict[str, Counter]) -> dict:
    latest = latest_run_per_date(runs)

    # Run health (all runs, including repeats of the same date)
    durations = [r["duration_sec"] for r in runs
                 if isinstance(r.get("duration_sec"), (int, float))]
    failures = []
    for r in runs:
        if r.get("status") != "SUCCESS":
            failed_stage = next((s.get("stage") for s in r.get("stages", [])
                                 if s.get("status") == "FAIL"), None)
            failures.append({"run_id": r.get("run_id"), "date": r.get("date"),
                             "failed_stage": failed_stage, "error": r.get("error")})
    health = {
        "runs_total": len(runs),
        "runs_succeeded": sum(1 for r in runs if r.get("status") == "SUCCESS"),
        "runs_failed": len(failures),
        "total_duration_sec": round(sum(durations), 3),
        "avg_duration_sec": round(sum(durations) / len(durations), 3) if durations else None,
        "failures": failures,
    }

    # Row flow: per date (latest run per date) and aggregated per source
    per_date = {d: {s: source_flow(run, s) for s in SOURCES} for d, run in latest.items()}
    for d in per_date:
        for s in SOURCES:
            f = per_date[d][s]
            f["quarantine_pct"] = pct(f["quarantined"], f["silver_in"])
    row_flow = {s: aggregate_flow(per_date, s) for s in SOURCES}

    # Customer changes from the dim_customer_written log event
    written = {e["run_id"]: e for e in log_events
               if e.get("event") == "dim_customer_written" and e.get("run_id")}
    # No event for a run (stage skipped, or older log format) -> None, shown as n/a.
    customer_changes = {
        d: {"new": written.get(run.get("run_id"), {}).get("new"),
            "changed": written.get(run.get("run_id"), {}).get("changed")}
        for d, run in latest.items()
    }

    # Mean stage duration (ms) over successful stage executions
    by_stage: dict[str, list[float]] = defaultdict(list)
    for r in runs:
        for s in r.get("stages", []):
            if s.get("status") == "OK" and isinstance(s.get("duration_sec"), (int, float)):
                by_stage[s["stage"]].append(s["duration_sec"])
    stage_ms = {k: round(1000 * sum(v) / len(v), 2) for k, v in by_stage.items()}

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dates_covered": list(latest),
        "run_health": health,
        "row_flow": row_flow,
        "row_flow_per_date": per_date,
        "quarantine_reasons": {s: dict(c.most_common()) for s, c in reasons.items()},
        "customer_changes": customer_changes,
        "stage_duration_ms": stage_ms,
    }


# ── Statistics files ─────────────────────────────────────────────────────────

def _v(value: object) -> str:
    return "n/a" if value is None else str(value)


def _table(headers: list[str], rows: list[list[object]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |",
             "| " + " | ".join("---" for _ in headers) + " |"]
    lines += ["| " + " | ".join(_v(c) for c in row) + " |" for row in rows]
    return lines + [""]


def write_summary(stats: dict, out_dir: Path) -> list[Path]:
    (out_dir / "summary.json").write_text(json.dumps(stats, indent=2), encoding="utf-8")

    h = stats["run_health"]
    md = ["# Pipeline Run Dashboard", "",
          f"Generated: {stats['generated_at']}  ",
          f"Runs covered: {h['runs_total']}  ",
          f"Processing dates: {', '.join(stats['dates_covered']) or 'none'}", "",
          "## Run health", ""]
    md += _table(["Metric", "Value"], [
        ["Runs total", h["runs_total"]],
        ["Succeeded", h["runs_succeeded"]],
        ["Failed", h["runs_failed"]],
        ["Total duration (s)", h["total_duration_sec"]],
        ["Average duration (s)", h["avg_duration_sec"]],
    ])
    if h["failures"]:
        md += _table(["Run", "Date", "Failed stage", "Error"],
                     [[f["run_id"], f["date"], f["failed_stage"], f["error"]]
                      for f in h["failures"]])

    md += ["## Row flow per source", "",
           "Orders are summed across processing dates. Customers and products are "
           "snapshots that repeat every run, so the latest run is shown. Gold counts "
           "for customers and products are the full dimension table size.", ""]
    md += _table(
        ["Source", "Bronze", "Quarantined", "Quarantine %", "Dedup-dropped",
         "Silver", "Gold", "Retention %"],
        [[s, f.get("bronze"), f.get("quarantined"), f.get("quarantine_pct"),
          f.get("deduped"), f.get("silver"), f.get("gold"), f.get("retention_pct")]
         for s, f in stats["row_flow"].items()])
    md += ["Dedup-dropped = Silver rows in - Silver rows out - quarantined. "
           "Retention % = Gold rows / Bronze rows.", ""]

    md += ["## Quarantine reasons", "",
           "Counts cover every quarantine file currently on disk, so repeated "
           "runs of the same date are counted each time. Only the first "
           "validation error of each row is categorised.", ""]
    reason_rows = [[s, reason, n] for s, rs in stats["quarantine_reasons"].items()
                   for reason, n in rs.items()]
    md += _table(["Source", "Reason", "Rows"], reason_rows) if reason_rows else ["None.", ""]

    md += ["## Customer changes per run (SCD2)", ""]
    md += _table(["Date", "New", "Changed"],
                 [[d, c["new"], c["changed"]] for d, c in stats["customer_changes"].items()])

    md += ["## Mean stage duration", ""]
    md += _table(["Stage", "Mean (ms)"], list(stats["stage_duration_ms"].items()))

    (out_dir / "summary.md").write_text("\n".join(md), encoding="utf-8")
    return [out_dir / "summary.md", out_dir / "summary.json"]


# ── Graphs ───────────────────────────────────────────────────────────────────

def _save(fig, out_dir: Path, name: str) -> Path:
    fig.tight_layout()
    path = out_dir / name
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return path


def plot_row_funnel(stats: dict, out_dir: Path) -> Path | None:
    flows = {s: f for s, f in stats["row_flow"].items() if f.get("bronze") is not None}
    if not flows:
        print("  skip row_funnel.png (no row data)")
        return None
    series = [("bronze", "Bronze"), ("quarantined", "Quarantined"),
              ("deduped", "Dedup-dropped"), ("silver", "Silver"), ("gold", "Gold")]
    width = 0.15
    fig, ax = plt.subplots(figsize=(9, 5))
    for i, (key, label) in enumerate(series):
        xs = [j + (i - 2) * width for j in range(len(flows))]
        bars = ax.bar(xs, [f.get(key) or 0 for f in flows.values()], width, label=label)
        ax.bar_label(bars, padding=2, fontsize=8)
    ax.set_xticks(range(len(flows)), list(flows))
    ax.set_ylabel("Rows")
    ax.set_title("Row flow per source (orders summed over dates)")
    ax.legend()
    return _save(fig, out_dir, "row_funnel.png")


def plot_quarantine_rate(stats: dict, out_dir: Path) -> Path | None:
    dates = list(stats["row_flow_per_date"])
    fig, ax = plt.subplots(figsize=(9, 5))
    plotted = False
    for s in SOURCES:
        ys = [stats["row_flow_per_date"][d][s]["quarantine_pct"] for d in dates]
        if any(y is not None for y in ys):
            ax.plot(dates, [float("nan") if y is None else y for y in ys],
                    marker="o", label=s)
            plotted = True
    if not plotted:
        plt.close(fig)
        print("  skip quarantine_rate.png (no row data)")
        return None
    ax.set_ylabel("Quarantined rows (% of Silver input)")
    ax.set_xlabel("Processing date")
    ax.set_title("Quarantine rate per run")
    ax.legend()
    return _save(fig, out_dir, "quarantine_rate.png")


def plot_quarantine_reasons(stats: dict, out_dir: Path) -> Path | None:
    items = [(f"{s}: {r}", n) for s, rs in stats["quarantine_reasons"].items()
             for r, n in rs.items()]
    if not items:
        print("  skip quarantine_reasons.png (nothing quarantined)")
        return None
    items.sort(key=lambda x: x[1])
    fig, ax = plt.subplots(figsize=(9, 1.5 + 0.5 * len(items)))
    bars = ax.barh([i[0] for i in items], [i[1] for i in items])
    ax.bar_label(bars, padding=2)
    ax.set_xlabel("Quarantined rows (all quarantine files on disk)")
    ax.set_title("Quarantine reasons")
    return _save(fig, out_dir, "quarantine_reasons.png")


def plot_stage_duration(stats: dict, out_dir: Path) -> Path | None:
    stages = stats["stage_duration_ms"]
    if not stages:
        print("  skip stage_duration.png (no stage timings)")
        return None
    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(list(stages), list(stages.values()))
    ax.bar_label(bars, padding=2, fontsize=8)
    ax.set_ylabel("Mean duration (ms)")
    ax.set_title("Mean stage duration")
    ax.tick_params(axis="x", rotation=40)
    return _save(fig, out_dir, "stage_duration.png")


def plot_customer_changes(stats: dict, out_dir: Path) -> Path | None:
    changes = {d: c for d, c in stats["customer_changes"].items()
               if c["new"] is not None and c["changed"] is not None}
    if not changes:
        print("  skip customer_changes.png (no customer change events)")
        return None
    dates = list(changes)
    xs = range(len(dates))
    width = 0.35
    fig, ax = plt.subplots(figsize=(9, 5))
    new = ax.bar([x - width / 2 for x in xs], [changes[d]["new"] for d in dates],
                 width, label="New")
    chg = ax.bar([x + width / 2 for x in xs], [changes[d]["changed"] for d in dates],
                 width, label="Changed")
    ax.bar_label(new, padding=2)
    ax.bar_label(chg, padding=2)
    ax.set_xticks(list(xs), dates)
    ax.set_ylabel("Customers")
    ax.set_title("Customer changes per run (SCD2)")
    ax.yaxis.get_major_locator().set_params(integer=True)
    ax.legend()
    return _save(fig, out_dir, "customer_changes.png")


# ── Entry point ──────────────────────────────────────────────────────────────

def main() -> int:
    config = Config.load()
    runs = read_jsonl(config.state / "runs.jsonl")
    if not runs:
        print("No pipeline runs found in state/runs.jsonl. "
              "Run the pipeline first: python -m src.pipeline --date 2025-11-10 --backfill 3",
              file=sys.stderr)
        return 1

    log_events = read_jsonl(config.logs / "pipeline.jsonl")
    reasons = load_quarantine_reasons(config.quarantine)
    stats = compute_stats(runs, log_events, reasons)

    OUT_DIR.mkdir(exist_ok=True)
    written = write_summary(stats, OUT_DIR)
    for plot in (plot_row_funnel, plot_quarantine_rate, plot_quarantine_reasons,
                 plot_stage_duration, plot_customer_changes):
        path = plot(stats, OUT_DIR)
        if path:
            written.append(path)

    print(f"Dashboard written to {OUT_DIR.relative_to(ROOT)}/")
    for p in written:
        print(f"  {p.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
