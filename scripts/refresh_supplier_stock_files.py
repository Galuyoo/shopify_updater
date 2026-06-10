from __future__ import annotations

import argparse
from datetime import datetime
import os
from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = PROJECT_ROOT / "app"
for path in [PROJECT_ROOT, APP_ROOT]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from ralawise import get_stock as get_ralawise_stock
from uneek import get_stock as get_uneek_stock


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh supplier stock CSV files.")
    parser.add_argument("--ralawise-out", default=Path("data/RALAWISE_stock_lvl.csv"), type=Path)
    parser.add_argument("--uneek-out", default=Path("data/Uneek_stock_levels.csv"), type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    _load_env_file(PROJECT_ROOT / ".env")
    results = [
        _refresh_supplier("Ralawise", get_ralawise_stock, args.ralawise_out),
        _refresh_supplier("Uneek", get_uneek_stock, args.uneek_out),
    ]
    print("Supplier stock refresh complete")
    for result in results:
        print(
            f"{result['supplier']}: rows_fetched={result['rows_fetched']} "
            f"output_path={result['output_path']} LastWriteTime={result['last_write_time']}"
        )
    print("No StoreFeeder API calls or stock updates were made.")
    return 0


def _refresh_supplier(supplier: str, fetcher, output_path: Path) -> dict[str, str | int]:
    print(f"Refreshing {supplier} stock...", flush=True)
    df = fetcher(progress=_safe_progress)
    if not isinstance(df, pd.DataFrame):
        raise RuntimeError(f"{supplier} stock fetcher did not return a DataFrame")
    if df.empty:
        raise RuntimeError(f"{supplier} stock fetch returned zero rows")
    if supplier.casefold() == "ralawise" and "SKU" not in df.columns and "sku" in df.columns:
        df = df.rename(columns={"sku": "SKU"})
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    mtime = datetime.fromtimestamp(output_path.stat().st_mtime).isoformat(timespec="seconds")
    return {
        "supplier": supplier,
        "rows_fetched": int(len(df)),
        "output_path": str(output_path),
        "last_write_time": mtime,
    }


def _safe_progress(message: str) -> None:
    print(str(message).encode("ascii", errors="ignore").decode("ascii"), flush=True)


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        name = name.strip()
        value = value.strip().strip('"').strip("'")
        if name and name not in os.environ:
            os.environ[name] = value


if __name__ == "__main__":
    raise SystemExit(main())
