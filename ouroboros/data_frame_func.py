import pandas as pd


def get_ts_value(df:pd.DataFrame, col, time, asof = False):
    if df is None or not len(df):
        return None
    if asof:
        return df[col].asof(pd.to_datetime(time))
    else:
        return df[col][time]


def get_ts_values(df:pd.DataFrame, cols, times, asof = False):
    if df is None or not len(df):
        return None
    if asof:
        return df[cols].asof(pd.to_datetime(times))
    else:
        return df.loc[pd.to_datetime(times), cols]