"""
Statistical utilities for return and correlation summaries.
"""
from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd


def compute_returns(df_prices: pd.DataFrame, log_returns: bool = False) -> pd.DataFrame:
    """Compute simple or log returns from price DataFrame."""
    if log_returns:
        return np.log(df_prices).diff()
    return df_prices.pct_change(fill_method=None)


def pooled_return_stats(df_ret: pd.DataFrame, obs_year: int) -> Dict[str, float]:
    """Compute pooled return stats across all symbols."""
    x = df_ret.to_numpy().ravel()
    x = x[np.isfinite(x)]
    if x.size == 0:
        return {
            "ann_mean": np.nan,
            "ann_vol": np.nan,
            "skew": np.nan,
            "kurtosis": np.nan,
            "min": np.nan,
            "max": np.nan,
        }

    s = pd.Series(x)
    return {
        "ann_mean": x.mean() * obs_year,
        "ann_vol": x.std(ddof=1) * np.sqrt(obs_year),
        "skew": s.skew(),
        "kurtosis": s.kurtosis(),
        "min": x.min(),
        "max": x.max(),
    }


def return_stats_by_symbol(df_ret: pd.DataFrame, obs_year: int) -> pd.DataFrame:
    """Compute return stats by symbol."""
    df_stats = pd.DataFrame(index=df_ret.columns)
    df_stats.index.name = "symbol"
    df_stats["n_obs"] = df_ret.count()
    df_stats["ann_mean"] = df_ret.mean(skipna=True) * obs_year
    df_stats["ann_vol"] = df_ret.std(ddof=1, skipna=True) * np.sqrt(obs_year)
    df_stats["skew"] = df_ret.skew(skipna=True)
    df_stats["kurtosis"] = df_ret.kurtosis(skipna=True)
    df_stats["min"] = df_ret.min(skipna=True)
    df_stats["max"] = df_ret.max(skipna=True)
    return df_stats


def corr_offdiag_stats(df_ret: pd.DataFrame) -> Dict[str, float]:
    """Compute off-diagonal correlation summary stats."""
    corr = df_ret.corr()
    n = corr.shape[0]
    if n < 2:
        return {"median": np.nan, "mean": np.nan, "sd": np.nan, "min": np.nan, "max": np.nan}

    i, j = np.triu_indices(n, 1)
    offdiag = pd.Series(corr.values[i, j]).dropna()
    if len(offdiag) == 0:
        return {"median": np.nan, "mean": np.nan, "sd": np.nan, "min": np.nan, "max": np.nan}

    return {
        "median": offdiag.median(),
        "mean": offdiag.mean(),
        "sd": offdiag.std(ddof=1),
        "min": offdiag.min(),
        "max": offdiag.max(),
    }
