# ReturnStats

## Overview
ReturnStats downloads daily prices from Yahoo Finance for a set of symbols and computes summary statistics, or reads a saved prices file and computes the same statistics. The project is focused on data ingestion and return/correlation summaries. Prices can be saved in Parquet or CSV format. Parquet is a columnar, compressed file format that can be read an order of magnitude faster than CSV for large datasets. 

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

If `write_single_csv_all_fields` is **True**, output is a single file with MultiIndex columns (`symbol`, `field`). If **False**, one file per field is written. A CSV file starts like this:

```
symbol,NVDA,NVDA,NVDA,NVDA,NVDA,NVDA,AAPL,AAPL,AAPL,AAPL,AAPL,AAPL
field,Open,High,Low,Close,Adj Close,Volume,Open,High,Low,Close,Adj Close,Volume
Date,,,,,,,,,,,,
2000-01-03,0.0984,0.0992,0.0919,0.0975,0.0894,300912000,0.9364,1.0045,0.9079,0.9994,0.8393,535796800
2000-01-04,0.0958,0.0961,0.0901,0.0949,0.087,300480000,0.9665,0.9877,0.9035,0.9152,0.7685,512377600
```

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

## Sample Output
Output from `xreturn_stats.py` for 4 stocks is

```
ret_scale: 100.0
prices file: prices.parquet
#obs, symbols, columns: 1508 4 1508
fields: ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
fields_ret: ['Open', 'Close', 'Adj Close']
return_type: simple

field: Open
#obs, first, last: 1508 2020-01-02 2025-12-31

return stats by symbol (Open):
        ann_mean  ann_vol   skew  kurtosis      min     max
symbol                                                     
NVDA     72.5394  54.3091 0.3201    4.5756 -15.8860 27.5174
AAPL     27.0691  32.3920 0.0583    4.5826  -9.1535 13.6163
MSFT     22.7810  28.3199 0.0692    3.2563  -8.4075 10.4561
AMZN     22.2193  37.4451 0.1426    4.2975 -14.0537 14.3395

field: High
#obs, first, last: 1508 2020-01-02 2025-12-31

field: Low
#obs, first, last: 1508 2020-01-02 2025-12-31

field: Close
#obs, first, last: 1508 2020-01-02 2025-12-31

return stats by symbol (Close):
        ann_mean  ann_vol   skew  kurtosis      min     max
symbol                                                     
NVDA     71.5758  53.1843 0.3513    4.5175 -18.4521 24.3696
AAPL     26.5640  31.8072 0.2859    6.7932 -12.8647 15.3289
MSFT     22.8014  29.5564 0.1093    7.5280 -14.7390 14.2169
AMZN     21.2281  35.7027 0.1428    4.3280 -14.0494 13.5359

field: Adj Close
#obs, first, last: 1508 2020-01-02 2025-12-31

return stats by symbol (Adj_Close):
        ann_mean  ann_vol   skew  kurtosis      min     max
symbol                                                     
NVDA     71.6508  53.1849 0.3514    4.5175 -18.4521 24.3696
AAPL     27.1589  31.8095 0.2847    6.7944 -12.8647 15.3289
MSFT     23.6662  29.5473 0.1064    7.5347 -14.7390 14.2169
AMZN     21.2281  35.7027 0.1428    4.3280 -14.0494 13.5359

field: Volume
#obs, first, last: 1508 2020-01-02 2025-12-31

return stats (pooled across symbols):
           ann_mean  ann_vol   skew  kurtosis      min     max
field                                                         
Open        36.1522  39.3917 0.3067    6.6455 -15.8860 27.5174
Close       35.5423  38.7055 0.3590    7.0671 -18.4521 24.3696
Adj Close   35.9260  38.7041 0.3580    7.0683 -18.4521 24.3696

time elapsed: 0.596 seconds
```
