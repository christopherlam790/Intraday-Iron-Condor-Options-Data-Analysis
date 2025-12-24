import sys
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, "src/loaders")  # add loaders path to search list
import load_parquet
import save_parquet


import clean_data
import validate_data
import csv_to_parquet


"""
# Preprocess data by loading, cleaning, and validating
"""
def preprocess_data(csv_file_path,raw_data_path, cleaned_data_path):

    csv_to_parquet.csv_to_parquet(csv_file_path=csv_file_path, parquet_file_path=raw_data_path)

    table = load_parquet.load_data(path=raw_data_path)
    df = table.to_pandas()

    # Perform data cleaning and preprocessing steps
    df = clean_data.clean_data(df)
    
    validate_data.validate_data(df)
    
    save_parquet.save_parquet(df, cleaned_data_path)
    
    return df


"""
# Preprocess data by loading, cleaning, and validating
"""
def minimized_preprocess_data(csv_file_path,raw_data_path, cleaned_data_path):

    csv_to_parquet.csv_to_parquet(csv_file_path=csv_file_path, parquet_file_path=raw_data_path)

    table = load_parquet.load_data(path=raw_data_path)
    df = table.to_pandas()

    # Perform data cleaning and preprocessing steps
    df = clean_data.minimized_clean_data(df, symbol="SPY")
    
    validate_data.validate_data_minimized(df)
    
    save_parquet.save_parquet(df, cleaned_data_path)
    
    return df 

"""
# Tester function

"""
if __name__ == "__main__":
    
    print("TESTTING preprocess_data.py \n \n # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #")
    
    CSV_FILE_PATH = "data/raw/underlying/hourly_stock_prices_technical_indicators.csv"
    RAW_FILE_PATH = "data/raw/underlying/RAW_hourly_stock_prices_technical_indicators.parquet"
    CLEANED_FILE_PATH = "data/processed/underlying/CLEANED_hourly_stock_prices_technical_indicators.parquet"
    MINIMIZED_CLEANED_FILE_PATH = "data/processed/underlying/MINIMIZED_CLEANED_hourly_stock_prices_technical_indicators.parquet"

    df = preprocess_data(csv_file_path=CSV_FILE_PATH, raw_data_path=RAW_FILE_PATH, cleaned_data_path=CLEANED_FILE_PATH)

    print(df.head())

    print("MINIMIZED VERSION \n \n # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #")

    df = minimized_preprocess_data(csv_file_path=CSV_FILE_PATH, raw_data_path=RAW_FILE_PATH, cleaned_data_path=MINIMIZED_CLEANED_FILE_PATH)


    print("FINISHED TESTING preprocess_data.py \n \n # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #")