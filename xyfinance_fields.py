"""
Download Yahoo Finance daily data for multiple tickers and fields, optionally write either one CSV per field or a
single CSV with (symbol, field) column levels, and compute return stats and correlation summaries only for fields_ret,
including per-symbol tables and pooled (all symbol-date observations) summaries.
"""
import time
t_start = time.perf_counter()

import pandas as pd
from yfinance_util import get_historical_prices
from pathlib import Path
from typing import List
from stats import compute_returns, pooled_return_stats, return_stats_by_symbol, corr_offdiag_stats

def read_tickers(path: Path) -> List[str]:
    """Return tickers from a text file, skipping blank lines and lines starting with '#'."""
    symbols: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        symbols.append(s)
    return symbols

pd.options.display.float_format = "{:.4f}".format

symbols_file = "itot_tickers_20260115.txt" # None
dropna_df = False
print_corr_returns = False
print_prices = False
describe_returns = False
out_prices_file = "prices.csv" # "prices.parquet"
max_stocks = 5 # 1000 # None
ret_scale = 100.0
use_log_returns = False

# correlation off-diagonal summary stats (median/mean/sd/min/max) by field
compute_corr_stats = True

# set one or more fields to download/process
fields = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

# only compute returns / return stats / correlations for these fields
fields_ret = ["Open", "Close", "Adj Close"]

# output control
write_single_csv_all_fields = True  # if True, write one CSV with multi-level columns (symbol, field)

# return statistics control
print_return_stats = True              # pooled across symbols, one row per field
print_return_stats_by_symbol = True    # one table per field, rows are symbols
obs_year = 252


def _write_prices(df: pd.DataFrame, out_path: Path) -> None:
    suffix = out_path.suffix.lower()
    if suffix == ".csv":
        df.round(4).to_csv(out_path)
        return
    if suffix == ".parquet":
        df.to_parquet(out_path)
        return
    raise ValueError(f"Unsupported output suffix: {suffix}")

if symbols_file is not None:
    symbols = read_tickers(Path(symbols_file))
else:
    symbols = ["BRK-B"]
if max_stocks is not None:
    symbols = symbols[:max_stocks]

start_date = "2000-01-01"
end_date = None

print("fields:", fields)
print("fields_ret:", fields_ret)
print("ret_scale:", ret_scale)
print("return_type:", "log" if use_log_returns else "simple")
print("start_date:", start_date)
print("end_date:", end_date)
print("#symbols:", len(symbols))
if symbols_file is not None:
    print("symbols_file:", symbols_file)

out_base = Path(out_prices_file) if out_prices_file is not None else None

# download once (all fields), then iterate
data_all = get_historical_prices(symbols, start_date, end_date, field=None)

corr_stats = {}
df_all = None

symbols_out = [s.lstrip("^") for s in symbols]

return_stats = {}

for field in fields:
    print("\nfield:", field)

    df = data_all[field]
    df = df[[symbol for symbol in symbols]]
    df.columns = [c.lstrip("^") for c in df.columns]

    if dropna_df:
        df = df.dropna()

    # print #obs, first, last
    if len(df.index) > 0:
        print("#obs, first, last:", len(df.index), df.index[0].date(), df.index[-1].date())
    else:
        print("#obs, first, last:", 0, "nan", "nan")

    if print_prices:
        print(df)

    if out_base is not None and not write_single_csv_all_fields:
        field_safe = field.replace(" ", "_")
        out_file = out_base.with_name(f"{out_base.stem}_{field_safe}{out_base.suffix}")
        _write_prices(df, out_file)
        print("wrote prices to", str(out_file))

    if out_base is not None and write_single_csv_all_fields:
        df2 = df.copy()
        df2.columns = pd.MultiIndex.from_product([df2.columns, [field]], names=["symbol", "field"])
        if df_all is None:
            df_all = df2
        else:
            df_all = pd.concat([df_all, df2], axis=1)

    # returns / stats / correlations only for fields in fields_ret
    if field in fields_ret:
        if describe_returns or print_corr_returns or compute_corr_stats or print_return_stats or print_return_stats_by_symbol:
            df_ret = ret_scale * compute_returns(df, log_returns=use_log_returns)

        if describe_returns:
            print(df_ret.describe())

        if print_return_stats:
            return_stats[field] = pooled_return_stats(df_ret, obs_year)

        if print_return_stats_by_symbol:
            df_stats = return_stats_by_symbol(df_ret, obs_year)
            print("\nreturn stats by symbol (" + field.replace(" ", "_") + "):\n" + df_stats.to_string())

        if (compute_corr_stats or print_corr_returns) and len(symbols) > 1:
            corr = df_ret.corr()
            if print_corr_returns:
                print("\ncorrelations (" + field.replace(" ", "_") + "):\n" + corr.to_string())

            if compute_corr_stats:
                corr_stats[field] = corr_offdiag_stats(df_ret)

if out_base is not None and write_single_csv_all_fields and df_all is not None:
    df_all = df_all.reindex(
        columns=pd.MultiIndex.from_product([symbols_out, fields], names=["symbol", "field"])
    )
    _write_prices(df_all, out_base)
    print("\nwrote prices (all fields) to", str(out_base))

# only print corr stats / return stats for fields_ret (and keep order = fields_ret)
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
