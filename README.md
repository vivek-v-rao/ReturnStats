# ReturnStats

## Overview
ReturnStats downloads daily Yahoo Finance prices and computes summary statistics, or reads a saved prices file and computes the same statistics. The project is intentionally small and focused on data ingestion and return/correlation summaries.

## Files
- `xyfinance_fields.py`: Download prices from Yahoo Finance, optionally write to CSV or Parquet, and compute summary statistics.
- `xreturn_stats.py`: Read saved prices (CSV or Parquet) and compute the same summary statistics.
- `stats.py`: Shared calculation utilities for returns, pooled stats, per-symbol stats, and correlation summaries.
- `yfinance_util.py`: Helper for Yahoo Finance downloads.

## Requirements
- Python 3.9+
- `pandas`
- `numpy`
- `yfinance`
- `pyarrow` (only required for Parquet)

## Quick start
**1) Download prices and compute stats**
- Edit settings near the top of `xyfinance_fields.py` (`symbols_file`, `fields`, date range, output file).
- Run:
  ```
  python xyfinance_fields.py
  ```

**2) Compute stats from a saved prices file**
- Edit settings near the top of `xreturn_stats.py` (`in_prices_file`, `fields_ret`, `ret_scale`, log returns, optional date and symbol limits).
- Run:
  ```
  python xreturn_stats.py
  ```

## Output formats
- **CSV**: set `out_prices_file` to a `.csv` path.
- **Parquet**: set `out_prices_file` to a `.parquet` path.

If `write_single_csv_all_fields` is **True**, output is a single file with MultiIndex columns (`symbol`, `field`). If **False**, one file per field is written.

## Return settings
Both scripts share the same return logic via `stats.py`.
- **ret_scale**: scale applied to returns (e.g., `100` for percent returns).
- **use_log_returns**: compute log returns if **True**; otherwise simple returns.

## Filters
`xreturn_stats.py` supports optional filters:
- **max_symbols**: limit the number of symbols read from the file.
- **date_min** / **date_max**: limit the date range analyzed (strings like `YYYY-MM-DD`).

## Common fields
Typical Yahoo Finance daily fields include:
- **Open**, **High**, **Low**, **Close**
- **Adj Close** (split/dividend adjusted close)
- **Volume**

## Notes
- Parquet requires `pyarrow`.
