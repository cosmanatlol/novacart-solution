# novacart-solution

A small ETL pipeline lab. It ingests raw orders, customers, and products from a
landing area, promotes them through a medallion architecture
(Bronze → Silver → Gold), and quarantines bad records along the way.

## Setup

### Prerequisites
- **Python 3.12+** installed ([python.org/downloads](https://www.python.org/downloads/))
- **Git** installed ([git-scm.com/downloads](https://git-scm.com/downloads))

Check both are available:

```bash
python --version
git --version
```

### 1. Clone the repository

```bash
git clone https://github.com/lneale-hkda/novacart-solution.git
cd novacart-solution
```

### 2. Create and activate a virtual environment

A virtual environment keeps this project's packages isolated from your system Python.

**macOS / Linux:**

```bash
python -m venv .venv
source .venv/bin/activate
```

**Windows (PowerShell):**

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Once active, your prompt will show `(.venv)`.

### 3. Install the requirements

scripts/run_everything.py repeats this install, so this step can be skipped if run_everything.py is run immediately. Note that an internet connection is needed to download dependencies.

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Run the project

```bash
# Install dependencies (if step 3 was skipped), generate sample data, run the pipeline, and run the tests in one shot
python scripts/run_everything.py
```

Or run the pipeline directly:

```bash
# Single date
python -m src.pipeline --date 2025-11-07

# A date plus the N days before it (e.g. Nov 7–10)
python -m src.pipeline --date 2025-11-10 --backfill 3
```

### 5. Clean up generated output (optional)

When you're done, wipe all pipeline-generated output (bronze, silver, gold,
quarantine, state, logs). Source data in `data/landing/` is left intact.

```bash
python scripts/cleanup.py
```

## Working as a team

The Setup steps above give each person a personal copy. If you're doing this as a
team, set up one shared repository everyone can push to. Do this **once per team**,
then have teammates clone it. Pick **one** of the two options below.

### Option A — Fork on GitHub (keeps a link to the original)

Best when you want to be able to pull in later updates from the original repo.

1. Open <https://github.com/lneale-hkda/novacart-solution> and click **Fork**
   (top-right). Set the owner to your team account or organization.
2. Clone your fork (replace `YOUR-TEAM` with your account or org name):

   ```bash
   git clone https://github.com/YOUR-TEAM/novacart-solution.git
   cd novacart-solution
   ```

3. (Optional) Add a link to the original so you can pull updates later:

   ```bash
   git remote add upstream https://github.com/lneale-hkda/novacart-solution.git
   git fetch upstream
   ```

### Option B — Create a brand-new team repo (independent copy)

Best when you want your own standalone repo with no fork relationship.

1. On GitHub, create a new **empty** repository under your team account (no README,
   no `.gitignore`) — for example `novacart-team`.
2. From the project folder you cloned during Setup, point it at your new repo and
   push (replace `YOUR-TEAM`):

   ```bash
   git remote remove origin
   git remote add origin https://github.com/YOUR-TEAM/novacart-team.git
   git branch -M main
   git push -u origin main
   ```

### Teammates — clone the shared repo

Everyone else clones the team repo (the fork URL from Option A, or the new repo URL
from Option B), then continues from the virtual-environment step in Setup:

```bash
git clone https://github.com/YOUR-TEAM/novacart-team.git
cd novacart-team
```

### Day-to-day team workflow

Pull the latest before you start, work on a branch, then push and open a pull
request so a teammate can review:

```bash
git pull
git checkout -b your-name/feature
git add -A && git commit -m "Describe your change"
git push -u origin your-name/feature
```


## Scripts

All scripts live in `scripts/` and are run from the repo root.

### `generate_sample_data.py`
Generates the sample source datasets in `data/landing/`:
- `data/landing/orders/orders_<date>.csv` — order CSVs for 2025-11-07 → 2025-11-10,
  including a few deliberately broken rows (zero quantity, invalid status,
  duplicates) to exercise the quarantine and dedup paths.
- `data/landing/customers/customers.json` — customer records with nested
  addresses, including one with a malformed email.
- `data/landing/products.db` — a SQLite database of products.

```bash
python scripts/generate_sample_data.py
```

### `generate_synthetic_data.py`
Generates customizable synthetic datasets in `data/landing/` with configurable record volumes and an option to introduce a percentage of malformed/invalid records to test pipeline validation and quarantine mechanisms:

```bash
# Generate data with default 10% malformed records
python scripts/generate_synthetic_data.py

# Generate data with 20% malformed records and a fixed seed
python scripts/generate_synthetic_data.py --malformed-pct 0.2 --seed 42

# Generate 100% clean synthetic data
python scripts/generate_synthetic_data.py --clean

# Custom volume generation
python scripts/generate_synthetic_data.py --malformed-pct 0.15 --num-customers 50 --num-products 20 --orders-per-day 50
```

### `run_everything.py`
Cross-platform end-to-end runner. Works on macOS, Linux, and Windows. Installs into the active interpreter. Runs five steps in order and exits on the first failure:
1. Install requirements (`pip install -r requirements.txt`).
2. Generate sample data (`generate_sample_data.py`).
3. Run the pipeline for 2025-11-10 with a 3-day backfill (Nov 7–10).
4. Build the dashboard (`build_dashboard.py`).
5. Run the test suite (`pytest`).

```bash
python scripts/run_everything.py
```

### `cleanup.py`
Cleanup run **after** `run_everything.py` or any individual pipeline run. Removes
all pipeline-generated output so the next run starts from a clean slate:

```
data/bronze  data/silver  data/gold  data/quarantine  state  logs  dashboard
```

It is the cross-platform equivalent of:

```bash
rm -rf data/bronze data/silver data/gold data/quarantine state logs dashboard
```

Landing/source data (`data/landing/*`) is **not** touched — regenerate that with
`generate_sample_data.py` if needed.

```bash
python scripts/cleanup.py
```

### `build_dashboard.py`
Builds a simple dashboard from existing pipeline output. It only **reads**
`state/runs.jsonl`, `logs/pipeline.jsonl` and `data/quarantine/`, and writes into a
new `dashboard/` folder:
- `summary.md` / `summary.json` — run health, row flow per source (quarantined and
  dedup-dropped rows, retention %), quarantine reasons, customer changes, and mean
  stage duration.
- `row_funnel.png`, `quarantine_rate.png`, `quarantine_reasons.png`,
  `stage_duration.png`, `customer_changes.png` — the matching graphs.

Run it after the pipeline. `run_everything.py` does this automatically.

```bash
python scripts/build_dashboard.py
```

### `view_parquet.py`
Used for printing parquet contents to the terminal. Replace PARQUET_FILE_PATH with the parquet file's relative path.
```bash
python scripts/view_parquet.py PARQUET_FILE_PATH
```

## Running the pipeline directly

```bash
# Single date
python -m src.pipeline --date 2025-11-07

# A date plus the N days before it
python -m src.pipeline --date 2025-11-10 --backfill 3
```

Pipeline behavior and paths are configured in `config/pipeline.yaml`.

## Email Alerts

When a required column is missing from a source, the pipeline emails all configured recipients with details about the error.

Configure the `email_alerts` block in `config/pipeline.yaml`:

```yaml
email_alerts:
  smtp_server: smtp.gmail.com
  smtp_port: 587
  sender_email: you@example.com
  sender_password: ""           # leave blank — set EMAIL_ALERT_PASSWORD instead
  recipients:
    - oncall@example.com
```

Set the password as an environment variable before running the pipeline:

```bash
export EMAIL_ALERT_PASSWORD="your-smtp-password"
```

> **Gmail users:** use an [App Password](https://myaccount.google.com/apppasswords) instead of your normal login password.

## Folder overview

| Path                 | Purpose |
|----------------------|---------|
| `config/`            | Pipeline configuration (`pipeline.yaml`) — paths and stage settings. |
| `scripts/`           | Operational entry points: data generation, end-to-end runner, cleanup. |
| `src/`               | Pipeline source code. |
| `src/ingest/`        | Landing → Bronze ingestion (orders, customers, products). |
| `src/transform/`     | Bronze → Silver and Silver → Gold transforms. |
| `src/load/`          | Load helpers. |
| `src/utils/`         | Shared utilities: config loading, logging, run-state management. |
| `tests/`             | Pytest suite for schemas and pipeline scenarios. |

### Data folders (medallion layers)

These are created by the scripts/pipeline at runtime and are removed by
`cleanup.py`:

| Path                | Layer       | What lives here |
|---------------------|-------------|-----------------|
| `data/landing/`     | Source      | Raw inputs (order CSVs, customer JSON, products SQLite DB). **Persistent input** — produced by `generate_sample_data.py`, not removed by cleanup. |
| `data/bronze/`      | Bronze      | Raw data ingested as-is from landing, with minimal structure. |
| `data/silver/`      | Silver      | Cleaned, validated, and deduplicated records. |
| `data/gold/`        | Gold        | Analytics-ready dimensional model (dim_product, dim_customer, fact_orders). |
| `data/quarantine/`  | Quarantine  | Records that failed validation (bad quantity, invalid status, malformed email, etc.). |
| `state/`            | Run state   | Per-run metadata and bookkeeping written by the orchestrator. |
| `logs/`             | Logs        | Structured pipeline logs. |
| `dashboard/`        | Reporting   | Statistics (`summary.md`, `summary.json`) and graph images built by `build_dashboard.py`. |
