"""
Read a CSV of price data and compute return and correlation summaries.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import List, Optional

import pandas as pd

from stats import compute_returns, pooled_return_stats, return_stats_by_symbol, corr_offdiag_stats


def _read_prices_file(path: Path) -> pd.DataFrame:
    """Read prices from CSV/Parquet/Feather with either MultiIndex or single-level columns."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        try:
            df = pd.read_csv(path, header=[0, 1], index_col=0, parse_dates=True)
            if isinstance(df.columns, pd.MultiIndex) and df.columns.nlevels == 2:
                if isinstance(df.index, pd.DatetimeIndex):
                    df = df[~df.index.isna()]
                return df
        except Exception:
            pass

        df = pd.read_csv(path, header=0, index_col=0, parse_dates=True)
        if isinstance(df.index, pd.DatetimeIndex):
            df = df[~df.index.isna()]
        return df

    if suffix == ".parquet":
        return pd.read_parquet(path)

    raise ValueError(f"Unsupported input suffix: {suffix}")


def _parse_fields_arg(fields_arg: Optional[str], available: List[str]) -> List[str]:
    if fields_arg is None:
        return available
    if isinstance(fields_arg, list):
        return fields_arg
    fields = [f.strip() for f in fields_arg.split(",") if f.strip()]
    return fields


def _get_fields_from_df(df_all: pd.DataFrame, flat_field: str) -> List[str]:
    if isinstance(df_all.columns, pd.MultiIndex) and df_all.columns.nlevels == 2:
        return list(pd.unique(df_all.columns.get_level_values(1)))
    return [flat_field]


def _get_prices_for_field(df_all: pd.DataFrame, field: str) -> pd.DataFrame:
    if isinstance(df_all.columns, pd.MultiIndex) and df_all.columns.nlevels == 2:
        return df_all.xs(field, level=1, axis=1)
    return df_all


def main() -> int:
    t_start = time.perf_counter()
    pd.options.display.float_format = "{:.4f}".format

    ret_scale = 100.0
    use_log_returns = False
    in_prices_file = "prices.csv" # "prices.parquet"
    dropna_df = False
    print_corr_returns = False
    describe_returns = False
    max_symbols = 1000
    date_min = None # "2020-01-01"
    date_max = None # "2025-12-31"

    # correlation off-diagonal summary stats (median/mean/sd/min/max) by field
    compute_corr_stats = False # True

    # fields to process (if None, uses fields in CSV)
    fields = None

    # only compute returns / return stats / correlations for these fields
    fields_ret = ["Open", "Close", "Adj Close"]
    print("ret_scale:", ret_scale)
    
    # return statistics control
    print_return_stats = True
    print_return_stats_by_symbol = True
    obs_year = 252

    in_path = Path(in_prices_file)
    df_all = _read_prices_file(in_path)
    if date_min is not None:
        date_min = pd.to_datetime(date_min)
    if date_max is not None:
        date_max = pd.to_datetime(date_max)
    if date_min is not None or date_max is not None:
        df_all = df_all.loc[date_min:date_max]

    if max_symbols is not None:
        if isinstance(df_all.columns, pd.MultiIndex) and df_all.columns.nlevels == 2:
            symbols = list(pd.unique(df_all.columns.get_level_values(0)))[:max_symbols]
            df_all = df_all.loc[:, df_all.columns.get_level_values(0).isin(symbols)]
        else:
            df_all = df_all.iloc[:, :max_symbols]

    fields_available = _get_fields_from_df(df_all, flat_field="Close")
    fields = _parse_fields_arg(fields, fields_available)
    fields_ret = _parse_fields_arg(fields_ret, fields)
    if isinstance(df_all.columns, pd.MultiIndex) and df_all.columns.nlevels == 2:
        num_symbols = df_all.columns.get_level_values(0).nunique()
    else:
        num_symbols = df_all.shape[1]

    print("prices file:", in_prices_file)
    print("#obs, symbols, columns:", df_all.shape[0], num_symbols, df_all.shape[0])
    print("fields:", fields)
    print("fields_ret:", fields_ret)
    print("return_type:", "log" if use_log_returns else "simple")

    corr_stats = {}
    return_stats = {}

    for field in fields:
        print("\nfield:", field)
        df = _get_prices_for_field(df_all, field)
        if dropna_df:
            df = df.dropna()

        if len(df.index) > 0:
            print("#obs, first, last:", len(df.index), df.index[0].date(), df.index[-1].date())
        else:
            print("#obs, first, last:", 0, "nan", "nan")

        if field not in fields_ret:
            continue

        df_ret = ret_scale * compute_returns(df, log_returns=use_log_returns)

        if describe_returns:
            print(df_ret.describe())

        if print_return_stats:
            return_stats[field] = pooled_return_stats(df_ret, obs_year)

        if print_return_stats_by_symbol:
            df_stats = return_stats_by_symbol(df_ret, obs_year)
            print("\nreturn stats by symbol (" + field.replace(" ", "_") + "):\n" + df_stats.to_string())

        if (print_corr_returns or compute_corr_stats) and df.shape[1] > 1:
            corr = df_ret.corr()
            if print_corr_returns:
                print("\ncorrelations (" + field.replace(" ", "_") + "):\n" + corr.to_string())
            if compute_corr_stats:
                corr_stats[field] = corr_offdiag_stats(df_ret)

    if compute_corr_stats and len(corr_stats) > 0:
        df_corr_stats = pd.DataFrame.from_dict(corr_stats, orient="index")
        df_corr_stats = df_corr_stats.reindex(fields_ret)
        df_corr_stats = df_corr_stats[["median", "mean", "sd", "min", "max"]]
        df_corr_stats.index.name = "field"
        print("\noff-diagonal correlation stats by field:\n" + df_corr_stats.to_string())

    if print_return_stats and len(return_stats) > 0:
        df_return_stats = pd.DataFrame.from_dict(return_stats, orient="index")
        df_return_stats = df_return_stats.reindex(fields_ret)
        df_return_stats = df_return_stats[["ann_mean", "ann_vol", "skew", "kurtosis", "min", "max"]]
        df_return_stats.index.name = "field"
        print("\nreturn stats (pooled across symbols):\n" + df_return_stats.to_string())

    elapsed = time.perf_counter() - t_start
    print(f"\ntime elapsed: {elapsed:.3f} seconds")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
