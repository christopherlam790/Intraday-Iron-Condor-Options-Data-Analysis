"""
Docstring for loaders.stock_data_loader

Load processed parquete file data from the local file system.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq 

PARQUETE_PATH = "data/raw/underlying/RAW_hourly_stock_prices_technical_indicators.parquet"

def load_data():
    table = pq.read_table(PARQUETE_PATH)
        
    return table


"""
# TESTING LOADER
"""
if __name__ == "__main__":
    
    print("TESTING LOADER \n \n # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # \n \n")
    
    table = load_data()
    print(table.to_pandas().head())
