"""
# Break down hourly threshold movements
"""
import sys

from torch import threshold 


sys.path.insert(0, "src/loaders")  # add loaders path to search list
import load_parquet
import save_parquet


import datetime
import pandas as pd
import numpy as np


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Target dir
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def target_dir_multi_hour(df, block_size=2, threshold=0.0005):
    df = df.sort_values(["symbol", "timestamp_day_est", "timestamp_hour_est"])
    
    # Shift by block_size to get the end-of-block close
    next_close = df.groupby(["symbol", "timestamp_day_est"])["close"].shift(-block_size)
    
    df["target_dir_next"] = np.select(
        [next_close >= df["close"]*(1+threshold),
         next_close <= df["close"]*(1-threshold)],
        [1, -1],
        default=0
    )
    
    # Optionally, remove last `block_size` rows per day, since they have no full block
    df.loc[df.groupby(["symbol","timestamp_day_est"]).cumcount() >= len(df["timestamp_hour_est"].unique())-block_size, "target_dir_next"] = np.nan
    
    return df

        
    

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Grouping, Sorting, & Stats
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


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Comubined summary funcs
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


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

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# FINAL CALLABLE
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def generate_all_target_moves(threshold):
    
    stack = []
    
    table = load_parquet.load_data("data/processed/underlying/MINIMIZED_CLEANED_hourly_stock_prices_technical_indicators.parquet")
    df = table.to_pandas()
    
    for i in range(1,7):
       
        df = target_dir_multi_hour(df, block_size=i, threshold=threshold)
        
        # Clean
        df = df[df['timestamp_hour_est'] != 16] 
               
        results = hourly_analysis(df)
        
        results["stat_summary"]["sanity_check"] = results["stat_summary"]["p_up"] + results["stat_summary"]["p_flat"] + results["stat_summary"]["p_down"]
        
        stack.append({
            "block_size": i,
            "summary": results["stat_summary"],
            "results": results,
            "threshold": threshold
            })
                
    return stack

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# View Data
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
def view_stack(stack):
    for df in stack:
        print(df["summary"])

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Saver
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def save_to_parquet(stack):
    for df in stack:
        cleaned_data_path = f'data/processed/target_next_move/CLEANED_{df["block_size"]}_block_size_{df["threshold"]}_threshold_target_next_move.parquet'
        save_parquet.save_parquet(df["summary"], cleaned_data_path)

# # # # # # # # # # # 
# Testing
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
if __name__ == "__main__":
    stack = generate_all_target_moves(threshold=0.0045)
    
    view_stack(stack=stack)
    save_to_parquet(stack)
    



    
    
    
    



