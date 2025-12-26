"""
# Break down hourly threshold movements
"""
import sys

from torch import threshold 


sys.path.insert(0, "src/loaders")  # add loaders path to search list
import load_parquet
import save_parquet


import datetime
import pandas as pd
import numpy as np




def aggregate_volume_single_hour(df):
    df = df.groupby('timestamp_hour_est').mean()
    df = df.drop(columns=['open', "low", "high", "close"], errors='ignore')
    
    return df
    

def calculate_VNR(df):
    df["vnr"] = (df["high"] - df["low"]) / df["volume"]
    df["vnr"] = (
    df
    .groupby("timestamp_hour_est")["vnr"]
    .transform(lambda x: (x - x.mean()) / x.std())
)
    return df

# # # # # # # # # # # 
# Testing
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
if __name__ == "__main__":
    
    table = load_parquet.load_data("data/processed/underlying/MINIMIZED_CLEANED_hourly_stock_prices_technical_indicators.parquet")
    df = table.to_pandas()
    
    df = calculate_VNR(df)
    
    print(aggregate_volume_single_hour(df))
        
    
