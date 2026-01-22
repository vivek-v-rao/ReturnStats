"""
Compare read times for prices.csv and prices.parquet.
"""
from __future__ import annotations

import time
from pathlib import Path

import pandas as pd


def _read_csv_prices(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, header=[0, 1], index_col=0, parse_dates=True)
        if isinstance(df.columns, pd.MultiIndex) and df.columns.nlevels == 2:
            return df
    except Exception:
        pass
    return pd.read_csv(path, header=0, index_col=0, parse_dates=True)


def _timed_read(label: str, read_fn) -> pd.DataFrame:
    t_start = time.perf_counter()
    df = read_fn()
    elapsed = time.perf_counter() - t_start
    print(f"{label}: {elapsed:.3f} seconds, shape={df.shape}")
    return df


def main() -> int:
    csv_path = Path("prices.csv")
    parquet_path = Path("prices.parquet")

    if not csv_path.exists():
        print("missing file:", str(csv_path))
        return 1
    if not parquet_path.exists():
        print("missing file:", str(parquet_path))
        return 1

    _timed_read("csv", lambda: _read_csv_prices(csv_path))
    _timed_read("parquet", lambda: pd.read_parquet(parquet_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
