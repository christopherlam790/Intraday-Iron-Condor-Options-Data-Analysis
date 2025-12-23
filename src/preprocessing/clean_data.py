"""
Clean the data by reformating & removing any rows with missing values.
"""

import sys
import pandas as pd
sys.path.insert(0, "src/loaders")  # add loaders path to search list
import load_parquet




"""
# Clean the data by converting UTC time to EST time
"""
def add_est_timestamp(df):
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)  # convert to datetime
    df["timestamp_est"] = df["timestamp"].dt.tz_convert("US/Eastern")
    
    return


"""
# Splice EST timestamp into day and hour columns
"""
def splice_est_tiimestamp(df):
    df["timestamp_day_est"] = df["timestamp_est"].dt.date      # YYYY-MM-DD
    df["timestamp_hour_est"] = df["timestamp_est"].dt.hour    # 0-23 integer

    return


"""
# Subset the data to only include market hours (9:00 AM - 4:00 PM) EST
"""
def subset_market_hour_timestamps(df):
    df = df[df["timestamp_hour_est"] >= 9]
    df = df[df["timestamp_hour_est"] <= 16]

    return df


"""
# Subset the data to only include a specific symbol (SPY by default)
# Throw an error if the symbol is not found in the dataset.
"""
def subset_symbol(df, symbol = "SPY"):
    if df[df["symbol"] == symbol].empty:
        raise ValueError(f"Symbol {symbol} not found in dataset.")

    df = df[df["symbol"] == symbol]

    return df

"""
# Remove any duplicate rows
"""
def remove_duplicates(df):
    df = df.drop_duplicates()

    return df

"""
# Remove any rows with missing values in the specified columns
"""
def remove_incomplete_rows(df):
    df = df.dropna(subset=['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'sma_10', 'sma_50', 'ema_20', 'rsi_14', 'macd', 'macd_signal', 'macd_hist', 'volatility_20', 'target_up_next'])
    
    return df

"""
# Remove the original timestamp and timestamp_est columns
"""
def remove_redundant_columns(df):
    df = df.drop(columns=['timestamp', 'timestamp_est'])
    
    return df


"""
# Clean the data by applying all the above functions
"""
def clean_data(df, symbol="SPY"):

    add_est_timestamp(df)
    splice_est_tiimestamp(df)
    df = subset_market_hour_timestamps(df)
    df = subset_symbol(df, symbol.upper())
    df = remove_duplicates(df)
    df = remove_incomplete_rows(df)
    df = remove_redundant_columns(df)
    
    return df


"""
# Main function to test the clean_data function
"""
if __name__ == "__main__":
    print("TESTING CLEAN_DATA FUNCTION:  \n \n # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # \n")

    PARQUETE_PATH = "data/raw/underlying/RAW_hourly_stock_prices_technical_indicators.parquet"

    # Access & convert parquet file
    table = load_parquet.load_data(path=PARQUETE_PATH)
    df = table.to_pandas()
    
    df = clean_data(df, symbol="spy")

    print(df.head())