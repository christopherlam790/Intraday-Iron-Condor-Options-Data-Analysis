import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq 



PARQUETE_PATH = "data/processed/underlying/hourly_stock_prices_technical_ind.parquet"

def load_data():
    table = pq.read_table(PARQUETE_PATH)
        
    return table

