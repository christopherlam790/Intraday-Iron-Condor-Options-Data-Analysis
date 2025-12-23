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
def csv_to_parquet(csv_file_path, parquet_file_path):
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file_path)

    # Convert the DataFrame to a PyArrow table
    table = pa.Table.from_pandas(df)

    # Write the table to a Parquet file
    pq.write_table(table, parquet_file_path)
    
    return
    
    
if __name__ == "__main__":

    CSV_FILE_PATH = 'data/raw/underlying/hourly_stock_prices_technical_indicators.csv'

    PARQUET_FILE_PATH = 'data/raw/underlying/RAW_hourly_stock_prices_technical_indicators.parquet'

    csv_to_parquet(CSV_FILE_PATH, PARQUET_FILE_PATH)

    print(f"Converted {CSV_FILE_PATH} to {PARQUET_FILE_PATH}")