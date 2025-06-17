import pandas as pd


def contains_datetime_gaps(df: pd.DataFrame, datetime_col_name: str, frequency: str) -> bool:
    """
    Check if the DataFrame contains gaps in the datetime column. Datetime column must be in UTC.
    Parameters:
    df (pd.DataFrame): The DataFrame to check.
    datetime_col_name (str): The name of the datetime column.
    frequency (str): The frequency of the datetime index (e.g., 'H' or '15M').
    Returns:
    bool: True if there are gaps, False otherwise.
    """
    df[datetime_col_name] = pd.to_datetime(df[datetime_col_name], utc=True)
    df = df.sort_values(datetime_col_name).reset_index(drop=True)

    full_range = pd.date_range(start=df[datetime_col_name].min(), end=df[datetime_col_name].max(), freq=frequency, tz='UTC')
    missing_times = full_range.difference(df[datetime_col_name])

    return not missing_times.empty, missing_times