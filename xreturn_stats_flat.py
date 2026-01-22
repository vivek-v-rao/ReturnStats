"""
Read a flat (single-field) prices CSV and compute return and correlation summaries.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

import pandas as pd

from stats import compute_returns, pooled_return_stats, return_stats_by_symbol, corr_offdiag_stats


def _read_prices_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path, header=0, index_col=0, parse_dates=True)
        if isinstance(df.index, pd.DatetimeIndex):
            df = df[~df.index.isna()]
        return df
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported input suffix: {suffix}")


def main() -> int:
    t_start = time.perf_counter()
    pd.options.display.float_format = "{:.4f}".format

    in_prices_file = "adj_close_1000.parquet" # "adj_close.csv" # "adj_close.parquet" # "spy_efa_eem_tlt.csv"
    dropna_df = False
    print_corr_returns = False # True
    describe_returns = False
    compute_corr_stats = True
    print_return_stats = True
    print_return_stats_by_symbol = True
    obs_year = 252
    ret_scale = 100.0
    use_log_returns = False
    max_symbols = None
    date_min = None
    date_max = None

    print("prices file:", in_prices_file)
    in_path = Path(in_prices_file)
    df_all = _read_prices_file(in_path)

    if date_min is not None:
        date_min = pd.to_datetime(date_min)
    if date_max is not None:
        date_max = pd.to_datetime(date_max)
    if date_min is not None or date_max is not None:
        df_all = df_all.loc[date_min:date_max]

    if max_symbols is not None:
        df_all = df_all.iloc[:, :max_symbols]

    num_symbols = df_all.shape[1]

    print("#obs, symbols, columns:", df_all.shape[0], num_symbols, df_all.shape[1])
    print("return_type:", "log" if use_log_returns else "simple")
    print("ret_scale:", ret_scale)

    if dropna_df:
        df_all = df_all.dropna()

    if len(df_all.index) > 0:
        print("#obs, first, last:", len(df_all.index), df_all.index[0].date(), df_all.index[-1].date())
    else:
        print("#obs, first, last:", 0, "nan", "nan")

    df_ret = ret_scale * compute_returns(df_all, log_returns=use_log_returns)

    if describe_returns:
        print(df_ret.describe())

    if print_return_stats:
        pooled = pooled_return_stats(df_ret, obs_year)
        df_pooled = pd.DataFrame.from_dict({"all": pooled}, orient="index")
        df_pooled = df_pooled[["ann_mean", "ann_vol", "skew", "kurtosis", "min", "max"]]
        df_pooled.index.name = "field"
        print("\nreturn stats (pooled across symbols):\n" + df_pooled.to_string())

    if print_return_stats_by_symbol:
        df_stats = return_stats_by_symbol(df_ret, obs_year)
        print("\nreturn stats by symbol:\n" + df_stats.to_string())

    if (print_corr_returns or compute_corr_stats) and df_all.shape[1] > 1:
        corr = df_ret.corr()
        if print_corr_returns:
            print("\ncorrelations:\n" + corr.to_string())
        if compute_corr_stats:
            corr_stats = corr_offdiag_stats(df_ret)
            df_corr_stats = pd.DataFrame.from_dict({"returns": corr_stats}, orient="index")
            df_corr_stats = df_corr_stats[["median", "mean", "sd", "min", "max"]]
            df_corr_stats.index.name = "field"
            print("\noff-diagonal correlation stats:\n" + df_corr_stats.to_string())

    elapsed = time.perf_counter() - t_start
    print(f"\ntime elapsed: {elapsed:.3f} seconds")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
