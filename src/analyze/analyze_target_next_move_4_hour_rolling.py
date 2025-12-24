"""
# Break down hourly threshold movements
"""
import sys

from torch import threshold 


sys.path.insert(0, "src/loaders")  # add loaders path to search list
import load_parquet

import datetime
import pandas as pd
import numpy as np





# # # # # # # # # # # 
# Testing
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
if __name__ == "__main__":
    table = load_parquet.load_data("data/processed/underlying/CLEANED_hourly_stock_prices_technical_indicators.parquet")
    df = table.to_pandas()

    print(df.columns.tolist())
    print(df)


