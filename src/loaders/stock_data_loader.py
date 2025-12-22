import pandas as pd


def load_data():
    df = pd.read_csv("data/raw/underlying/hourly_stock_prices_technical_indicators.csv")

    return df
