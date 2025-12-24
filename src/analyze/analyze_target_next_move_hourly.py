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


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Up target funcs
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

"""
# Binary target: 1 if next hour close ≥ {threshold}% higher 
"""
def target_up_hours(df, threshold=0.0005):
    df["target_up_next"] = (
    df["close"].shift(-1) >= df["close"] * (1 + threshold)
    ).astype(int)

    return df
"""
# Calculates percent occurence higher than {threshold} by hour across all data

"""
def target_up_hours_percent(df):
    # Target distribution per symbol
    target_by_hour = df.groupby('timestamp_hour_est')['target_up_next'].agg(['mean', 'count'])
    target_by_hour.columns = ['Ups %','Total Records']
    target_by_hour['Ups %'] = target_by_hour['Ups %'] * 100
    

    return target_by_hour

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Down target funcs
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


"""
# Binary target: 1 if next hour close ≥ {threshold}% lower 
"""
def target_down_hours(df, threshold=0.0005):
    df["target_down_next"] = (
    df["close"].shift(-1) <= df["close"] * (1 - threshold)
    ).astype(int)

    return df
"""
# Calculates percent occurence lower than {threshold} by hour across all data

"""
def target_down_hours_percent(df):
    # Target distribution per symbol
    target_by_hour = df.groupby('timestamp_hour_est')['target_down_next'].agg(['mean', 'count'])
    target_by_hour.columns = ['Downs %','Total Records']
    target_by_hour['Downs %'] = target_by_hour['Downs %'] * 100

    return target_by_hour

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# In between target funcs
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
"""
# Calculates percent occurence in between {threshold} by hour across all data
"""
def target_in_between_percent(df, threshold=0.0005):
    df_up = target_up_hours_percent(target_up_hours(df,threshold=threshold))
    df_down = target_down_hours_percent(target_down_hours(df, threshold=threshold))

    return 100 - df_up["Ups %"] - df_down["Downs %"]

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Combined target funcs
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
"""
# Aggregates by hour percentage report
"""
def combined_target_percent(df, threshold=0.0005):
    df_up = target_up_hours_percent(target_up_hours(df, threshold=threshold))
    df_down = target_down_hours_percent(target_down_hours(df, threshold=threshold))
    df_in_between = target_in_between_percent(df, threshold=threshold)

    combined_merged_df = pd.merge(df_up, df_down, left_index=True, right_index=True)
    
    combined_merged_df["In Between %"] = df_in_between
    combined_merged_df["Sanity Check"] = combined_merged_df["Ups %"] + combined_merged_df["Downs %"] + combined_merged_df["In Between %"]
    

    """
    # Clean up unused cols
    """
    def clean_up(df):
        df = df.drop(columns=['Total Records_x', 'Total Records_y'])
        return df

    combined_merged_df = clean_up(combined_merged_df)

    return combined_merged_df



# # # # # # # # # # # 
# Testing
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
if __name__ == "__main__":
    table = load_parquet.load_data("data/processed/underlying/CLEANED_hourly_stock_prices_technical_indicators.parquet")
    df = table.to_pandas()

    print(combined_target_percent(df, threshold=0.0005))


