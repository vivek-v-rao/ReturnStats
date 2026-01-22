"""
Download Yahoo Finance daily data for multiple tickers for a single field and compute summary statistics.
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
# field to download/process
field = "Adj Close"
out_prices_file = "adj_close.csv" # "adj_close.parquet"
max_stocks = None
ret_scale = 100.0
use_log_returns = False

# correlation off-diagonal summary stats (median/mean/sd/min/max)
compute_corr_stats = True



# return statistics control
print_return_stats = True              # pooled across symbols
print_return_stats_by_symbol = True    # rows are symbols
obs_year = 252

if symbols_file is not None:
    symbols = read_tickers(Path(symbols_file))
else:
    symbols = ["BRK-B"]
if max_stocks is not None:
    symbols = symbols[:max_stocks]

start_date = "2000-01-01"
end_date = None

print("field:", field)
print("ret_scale:", ret_scale)
print("return_type:", "log" if use_log_returns else "simple")
print("start_date:", start_date)
print("end_date:", end_date)
print("#symbols:", len(symbols))
if symbols_file is not None:
    print("symbols_file:", symbols_file)


def _write_prices(df: pd.DataFrame, out_path: Path) -> None:
    suffix = out_path.suffix.lower()
    if suffix == ".csv":
        df.round(4).to_csv(out_path)
        return
    if suffix == ".parquet":
        df.to_parquet(out_path)
        return
    raise ValueError(f"Unsupported output suffix: {suffix}")


out_base = Path(out_prices_file) if out_prices_file is not None else None

df = get_historical_prices(symbols, start_date, end_date, field=field)
df = df[[symbol for symbol in symbols]]
df.columns = [c.lstrip("^") for c in df.columns]

if dropna_df:
    df = df.dropna()

if len(df.index) > 0:
    print("#obs, first, last:", len(df.index), df.index[0].date(), df.index[-1].date())
else:
    print("#obs, first, last:", 0, "nan", "nan")

if print_prices:
    print(df)

if out_base is not None:
    _write_prices(df, out_base)
    print("wrote prices to", str(out_base))

return_stats = {}
corr_stats = {}

if describe_returns or print_corr_returns or compute_corr_stats or print_return_stats or print_return_stats_by_symbol:
    df_ret = ret_scale * compute_returns(df, log_returns=use_log_returns)

if describe_returns:
    print(df_ret.describe())

if print_return_stats:
    return_stats[field] = pooled_return_stats(df_ret, obs_year)

if print_return_stats_by_symbol:
    df_stats = return_stats_by_symbol(df_ret, obs_year)
    print("\nreturn stats by symbol:\n" + df_stats.to_string())

if (compute_corr_stats or print_corr_returns) and len(symbols) > 1:
    corr = df_ret.corr()
    if print_corr_returns:
        print("\ncorrelations:\n" + corr.to_string())
    if compute_corr_stats:
        corr_stats[field] = corr_offdiag_stats(df_ret)

if compute_corr_stats and len(corr_stats) > 0:
    df_corr_stats = pd.DataFrame.from_dict(corr_stats, orient="index")
    df_corr_stats = df_corr_stats[["median", "mean", "sd", "min", "max"]]
    df_corr_stats.index.name = "field"
    print("\noff-diagonal correlation stats:\n" + df_corr_stats.to_string())

if print_return_stats and len(return_stats) > 0:
    df_return_stats = pd.DataFrame.from_dict(return_stats, orient="index")
    df_return_stats = df_return_stats[["ann_mean", "ann_vol", "skew", "kurtosis", "min", "max"]]
    df_return_stats.index.name = "field"
    print("\nreturn stats (pooled across symbols):\n" + df_return_stats.to_string())

elapsed = time.perf_counter() - t_start
print(f"\ntime elapsed: {elapsed:.3f} seconds")
