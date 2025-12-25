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
# Combined target funcs
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def target_direction_hours(df, threshold=0.0005):
    df = df.sort_values(
        ["symbol", "timestamp_day_est", "timestamp_hour_est"]
    ).reset_index(drop=True)

    next_close = (
        df.groupby(["symbol", "timestamp_day_est"])["close"]
          .shift(-1)
    )

    up = next_close >= df["close"] * (1 + threshold)
    down = next_close <= df["close"] * (1 - threshold)

    df["target_dir_next"] = np.select(
        [up, down],
        [1, -1],
        default=0
    )

    return df


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Clean final hour (16)
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def clean_final_hour(df):
    df.loc[df["timestamp_hour_est"] == 15, "target_dir_next"] = np.nan
 
    df.loc[df["timestamp_hour_est"] == 16, "target_dir_next"] = np.nan
    
    return df.dropna()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def hourly_target_frequencies(df, target_col="target_dir_next"):
    """
    Returns normalized frequency of target values by hour.
    """
    return (
        df
        .dropna(subset=[target_col])
        .groupby("timestamp_hour_est")[target_col]
        .value_counts(normalize=True)
        .unstack()
        .sort_index()
    )

def hourly_sample_counts(df, target_col="target_dir_next"):
    return (
        df
        .dropna(subset=[target_col])
        .groupby("timestamp_hour_est")[target_col]
        .count()
    )

def hourly_directional_probs(df, target_col="target_dir_next"):

    # Remove invalid rows (e.g., hour 16)
    df_valid = df.dropna(subset=[target_col])

    # Compute normalized counts (probabilities) per hour
    hourly_probs = (
        df_valid.groupby("timestamp_hour_est")[target_col]
        .value_counts(normalize=True)   # proportion of each class
        .unstack(fill_value=0)          # make -1,0,1 columns
        .sort_index()
    )

    # Optional: rename columns for clarity
    hourly_probs = hourly_probs.rename(columns={-1: "p_down", 0: "p_flat", 1: "p_up"})

    return hourly_probs

def hourly_directional_bias(df, target_col="target_dir_next"):
    return (
        df
        .dropna(subset=[target_col])
        .groupby("timestamp_hour_est")[target_col]
        .mean()
    )

def binomial_ci(p, n, z=1.96):
    return z * np.sqrt(p * (1 - p) / n)

def hourly_stat_summary(df):
    counts = hourly_sample_counts(df)
    probs = hourly_directional_probs(df)

    summary = probs.join(counts.rename("n"))
    summary["ci"] = binomial_ci(summary["p_up"], summary["n"])
    
    summary["bias"] = hourly_directional_bias(df) 

    return summary

def hourly_analysis(df):
    results = {
        "frequency_table": hourly_target_frequencies(df),
        "sample_counts": hourly_sample_counts(df),
        "directional_probs": hourly_directional_probs(df),
        "directional_bias": hourly_directional_bias(df),
        "stat_summary": hourly_stat_summary(df),
    }
    return results


# # # # # # # # # # # 
# Testing
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
if __name__ == "__main__":
    table = load_parquet.load_data("data/processed/underlying/MINIMIZED_CLEANED_hourly_stock_prices_technical_indicators.parquet")
    df = table.to_pandas()
    
    df = target_direction_hours(df, threshold=.0015)
    
    df = clean_final_hour(df)
    
    results = hourly_analysis(df)
    
    print(results["stat_summary"])



    
    
    
    



