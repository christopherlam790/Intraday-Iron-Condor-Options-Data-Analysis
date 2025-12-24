"""
Validate format of parquet file via pandas.

"""

import sys
import pandas as pd
sys.path.insert(0, "src/loaders")  # add loaders path to search list
import load_parquet

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Existence functions
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

"""
# Custom exception class
"""
class ValidationError(Exception):
    """table has a validation error."""
    pass


"""
# Validated use of single symbol
"""
def validate_single_symbol(df):
    if df["symbol"].nunique() != 1:
        raise ValidationError("table has more than one symbol")
    if df["symbol"].isnull().values.any():
        raise ValidationError("table has null symbol")
    
    return

"""
# Validated existence of OHLCV columns
"""
def validate_OHLCV_existence(df):
    required_columns = ["open", "high", "low", "close", "volume"]
    for column in required_columns:
        if column not in df.columns:
            raise ValidationError(f"table is missing required column: {column}")
        if df[column].isnull().values.any():
            raise ValidationError(f"table has null values in column: {column}")
    
    return

"""
# Validated existence of simple & exponential moving averages
"""
def validate_moving_averages_existence(df):
    required_columns = ["sma_10", "sma_50", "ema_20"]
    for column in required_columns:
        if column not in df.columns:
            raise ValidationError(f"table is missing required column: {column}")
        if df[column].isnull().values.any():
            raise ValidationError(f"table has null values in column: {column}")
    
    return    

"""
# Validate existence of RSI
"""
def validate_rsi_existence(df):
    required_columns = ["rsi_14"]
    for column in required_columns:
        if column not in df.columns:
            raise ValidationError(f"table is missing required column: {column}")
        if df[column].isnull().values.any():
            raise ValidationError(f"table has null values in column: {column}")
    
    return

"""
# Validate existence of MACD
"""
def validate_macd_existence(df):
    required_columns = ["macd", "macd_signal", "macd_hist"]
    for column in required_columns:
        if column not in df.columns:
            raise ValidationError(f"table is missing required column: {column}")
        if df[column].isnull().values.any():
            raise ValidationError(f"table has null values in column: {column}")
    
    return

"""
# Validate existence of volatility
"""
def validate_volatility_existence(df):
    required_columns = ["volatility_20"]
    for column in required_columns:
        if column not in df.columns:
            raise ValidationError(f"table is missing required column: {column}")
        if df[column].isnull().values.any():
            raise ValidationError(f"table has null values in column: {column}")
    
    return

"""
# Validate existence of target_up_next
"""
def validate_target_up_next_existence(df):
    required_columns = ["target_up_next"]
    for column in required_columns:
        if column not in df.columns:
            raise ValidationError(f"table is missing required column: {column}")
        if df[column].isnull().values.any():
            raise ValidationError(f"table has null values in column: {column}")
    
    return


"""
# Validate existence of timestamp in EST by day & hour
"""    
def validate_timestamp_existence(df):
    required_columns = ["timestamp_day_est", "timestamp_hour_est"]
    for column in required_columns:
        if column not in df.columns:
            raise ValidationError(f"table is missing required column: {column}")
        if df[column].isnull().values.any():
            raise ValidationError(f"table has null values in column: {column}")
    
    return

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Correctness functions
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


"""
# Validated timestamp range to be within market hours
"""
def validate_timestamp_within_market_hours(df):
    for i in df["timestamp_hour_est"]:
        if i < 9 or i > 16:
            raise ValidationError(f"table has invalid hour value in column: {i}")

"""
# Validate target_up_next to be binary
"""
def validate_target_up_next_is_binary(df):
    for i in df["target_up_next"]:
        if i != 0 and i != 1:
            raise ValidationError(f"table has invalid value in column: {i}")
  
"""
# Validate rsi range to be between 0 & 100
"""
def validate_rsi_range(df):
    for i in df["rsi_14"]:
        if i < 0 or i > 100:
            raise ValidationError(f"table has invalid rsi value in column: {i}")

    

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Lagrer Helper Functions
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

"""
# Validate core cols (symbol and OHLCV) present
"""
def validate_columns_existence_minimized(df):
    validate_single_symbol(df)
    validate_OHLCV_existence(df)

"""
# Validate existence of all cols
"""
def validate_columns_existence(df):
    validate_columns_existence_minimized(df)
    validate_moving_averages_existence(df)
    validate_rsi_existence(df)
    validate_macd_existence(df)
    validate_volatility_existence(df)
    validate_target_up_next_existence(df)
    validate_timestamp_existence(df)
    
    return
 
def validate_columns_correctness(df):
    validate_timestamp_within_market_hours(df)
    validate_target_up_next_is_binary(df)
    validate_rsi_range(df)    
    return
       
       
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Complete Valiation Function
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
        
"""
# Full validation function
"""
def validate_data(df):
    validate_columns_existence(df)
    validate_columns_correctness(df)
    
    return

def validate_data_minimized(df):
    validate_columns_existence_minimized(df)
    return



if __name__ == "__main__":
    print("TESTING VALIDATE_DATA FUNCTION:  \n \n # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #")

    PARQUETE_PATH = "data/processed/underlying/CLEANED_hourly_stock_prices_technical_indicators.parquet"
    

    # Access & convert parquet file
    table = load_parquet.load_data(path=PARQUETE_PATH)
    df = table.to_pandas()
    
    
    validate_data(df)
    
    print(" \n \n # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # \n \nTESTING COMPLETE")
