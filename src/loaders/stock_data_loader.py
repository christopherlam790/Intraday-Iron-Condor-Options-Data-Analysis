"""
Docstring for loaders.stock_data_loader

Load processed parquete file data from the local file system.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq 

PARQUETE_PATH = "data/processed/underlying/hourly_stock_prices_technical_ind.parquet"

def load_data():
    table = pq.read_table(PARQUETE_PATH)
        
    return table

