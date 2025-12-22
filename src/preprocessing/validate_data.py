"""
Validate format of parquet file via pandas.

"""

import sys
import pandas as pd
sys.path.insert(0, "src/loaders")  # add loaders path to search list
import stock_data_loader

# Access & convert parquet file
table = stock_data_loader.load_data()
df = table.to_pandas()


class ValidationError(Exception):
    """table has a validation error."""
    pass


