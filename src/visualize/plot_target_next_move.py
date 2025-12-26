import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import sys

sys.path.insert(0, "src/loaders")  # add loaders path to search list
import load_parquet


if __name__ == "__main__":
    
    for block in range(1, 7):        
        for threshold in [0.001, 0.0015, 0.002, 0.0025, 0.003, 0.0035, 0.004, 0.0045]:
    
            table = load_parquet.load_data(f"data/processed/target_next_move/CLEANED_{block}_block_size_{threshold}_threshold_target_next_move.parquet")
            df = table.to_pandas()
        
            df = df.reset_index()

            df["index"] = df["index"] + 9
            
            df_long = df.melt(
                id_vars=["index"],
                value_vars=["p_down", "p_flat", "p_up"],
                var_name="direction",
                value_name="probability"
            )
        
        
            g = sns.catplot(
            data=df_long,
            kind="bar",
            x="direction",
            y="probability",
            col="index",
            col_wrap=3,          # <-- exactly 3 subplots per row
            height=3.5,
            aspect=1.1,
            sharey=True
            )

            g.set_titles("Start Hour {col_name}")
            g.set_axis_labels("", "Probability")


            g.fig.canvas.manager.set_window_title(f'Block: {block}  threshold: {round(threshold * 100,2)}%')   
            
            
            plt.savefig(f"data/charts/target_next_move/CLEANED_FIG_{block}_block_size_{threshold}_threshold_target_next_move.png")
            plt.show()