"""
Docstring for loaders.stock_data_loader

Load processed parquete file data from the local file system.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq 


def load_data(path):
    table = pq.read_table(path)
        
    return table


"""
# TESTING LOADER
"""
if __name__ == "__main__":
    
    print("TESTING LOADER \n \n # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # \n \n")
    PARQUETE_PATH = "data/raw/underlying/RAW_hourly_stock_prices_technical_indicators.parquet"

    table = load_data(path=PARQUETE_PATH)
    print(table.to_pandas().head())
