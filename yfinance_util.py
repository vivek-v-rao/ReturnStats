import yfinance as yf


def get_historical_prices(symbols, start_date=None, end_date=None, field="Close"):
    """
    Download Yahoo Finance daily data for the given symbols once and optionally select one or more fields.

    field:
      - str: return a DataFrame (dates x symbols) for that field
      - list/tuple: return a dict mapping each field -> DataFrame (dates x symbols)
      - None: return the full yfinance DataFrame with MultiIndex columns (field, symbol)
    """
    data = yf.download(symbols, start=start_date, end=end_date, auto_adjust=False)

    if field is None:
        return data

    if isinstance(field, (list, tuple)):
        return {f: data[f] for f in field}

    return data[field]
