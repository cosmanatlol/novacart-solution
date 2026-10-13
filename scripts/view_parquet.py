from pathlib import Path
import sys

print("Viewer started", flush=True)

if len(sys.argv) != 2:
    print("Usage: python scripts/view_parquet.py <parquet-file>")
    sys.exit(1)

path = Path(sys.argv[1])

print(f"Checking: {path}", flush=True)

if not path.exists():
    print(f"File not found: {path}")
    sys.exit(1)

try:
    import pandas as pd

    df = pd.read_parquet(path)

    print(f"\nFile: {path}")
    print(f"Rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    print("\nData types:")
    print(df.dtypes)
    print("\nFirst 10 rows:")
    print(df.head(10).to_string(index=False))

except Exception as error:
    print(f"Could not read the Parquet file:\n{type(error).__name__}: {error}")
    sys.exit(1)