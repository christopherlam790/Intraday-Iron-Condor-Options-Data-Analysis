"""
Convert raw CSV data to Parquet format and save it as processed for overwriting.

NOTE: This script will overwrite the existing processed data file if it already exists; 
    ONLY run this script if you are sure that you want to overwrite the existing processed data file & reset the processed information.
"""


import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

"""
This script reads a CSV file containing hourly stock prices with technical indicators,
converts it to Parquet format, and saves it to a specified directory.
"""

csv_file_path = 'data/raw/underlying/hourly_stock_prices_technical_indicators.csv'
df = pd.read_csv(csv_file_path)

table = pa.Table.from_pandas(df)
parquet_file_path = 'data/raw/underlying/RAW_hourly_stock_prices_technical_indicators.parquet'
pq.write_table(table, parquet_file_path)

print(f"Converted {csv_file_path} to {parquet_file_path}")