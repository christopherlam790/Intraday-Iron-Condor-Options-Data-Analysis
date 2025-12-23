import sys
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, "src/loaders")  # add loaders path to search list
import load_parquet

import clean_data
import validate_data


# Access & convert parquet file
table = load_parquet.load_data()
df = table.to_pandas()


df = clean_data.clean_data(df)



df.to_parquet("data/processed/underlying/CLEANED_hourly_stock_prices_technical_ind.parquet", index=False)



