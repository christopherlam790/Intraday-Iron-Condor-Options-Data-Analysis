"""
Clean the data by reformating & removing any rows with missing values.
"""



import sys
import pandas as pd
sys.path.insert(0, "src/loaders")  # add loaders path to search list
import stock_data_loader

# Access & convert parquet file
table = stock_data_loader.load_data()
df = table.to_pandas()


print(df.head())

df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)  # convert to datetime
df["timestamp_est"] = df["timestamp"].dt.tz_convert("US/Eastern")

df["timestamp_day_est"] = df["timestamp_est"].dt.date      # YYYY-MM-DD
df["timestamp_hour_est"] = df["timestamp_est"].dt.hour    # 0-23 integer

print(df.head())
