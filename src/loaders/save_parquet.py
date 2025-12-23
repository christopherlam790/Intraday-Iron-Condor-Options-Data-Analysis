import pandas as pd

"""
# Save df to parquet file
"""
def save_parquet(df, file_path):
    df.to_parquet(file_path, index=False)
