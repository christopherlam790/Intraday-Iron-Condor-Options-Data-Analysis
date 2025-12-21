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
parquet_file_path = 'data/processed/underlying/hourly_stock_prices_technical_ind.parquet'
pq.write_table(table, parquet_file_path)

print(f"Converted {csv_file_path} to {parquet_file_path}")