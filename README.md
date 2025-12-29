# Intraday_SPY_Next_Move_Analysis
This project aims to analyze hourly stock data (provided by Kaggle user geowtt) to predict the price range of the S&P 500 (SPY) over a block period of hours.

---
# Method

The origninal dataset is comprised of 2023 trading hours rounded to the nearest hour. Specifically, the hours incoprerated span the standard trading hours (9:00 AM EST - 4:00 PM EST), as well as hours before and after this range (filtered out and not applicable for this analysis)

For each row in the data set's applicable open market hours (9:00 AM EST - 4:00 PM EST) the OHLCV is analyzed and given a trinary variable:
- target_next_move (1, 0,-1)

This variable represent the price being above, in between, or below a threshold percent for each row. For example:

- Let **threshold** equal 0.0025 (equivilent to a plus or minus move of 0.25% for SPY)
- Let SPY's **open** equal 100.00 for the hour range 9:00 AM EST to 9:59 AM EST
- Let SPY's **close** equal 100.50 for the hour range 9:00 AM EST to 9:59 AM EST

By the specified threshold of 0.0025, the requirments for **target_next_move** to equal 1 must result in a SPY closng price above 100.25. Inversely, **target_next_move** will result in a value equal to -1 if the closing price of SPY is below 99.75. Finally, the default value of 0 will be assigned were the closing price to be in between these two values.

Thus, by this example, the row will be marked with a value of 1, since the closing price exceeded the established threshold (closing price of 105 is greater than the threshold of 102.5); This process is repeated for each hour for each day available across the applicable timeframe (i.e. each row in the dataset).

Further, the above process is then repeated across several *block hours*. The previous implementation described is incroperatedon the most granular level (i.e. each row is one hour, so **target_next_move** is reported as such), though by using *block hours*, analysis can be conducted across borader time frames.

For example:
- Let **threshold** equal 0.0025 (equivilent to a plus or minus move of 0.25% for SPY)
- Let **block_size** equal 2 (i.e. spans 2 hours instead of the original 1).
- Let SPY's **open** equal 100.00 for the hour range 9:00 AM EST to 9:59 AM EST
- Let SPY's **close** equal 101.00 for the hour range 9:00 AM EST to 9:59 AM EST
- Let SPY's **open** equal 101.00 for the hour range 10:00 AM EST to 10:59 AM EST
- Let SPY's **close** equal 100.10 for the hour range 10:00 AM EST to 10:59 AM EST

Like before, the threshold for **target_next_move** to be 0 (in between) is the range of [99.75, 100.25) inclusive, with values of 1 (above) and -1 (below) being outside of the ranges as applicable. Despite having a large move upward in period of 9:00 AM EST to 9:59 AM EST, the classification for this section is in betwenn (i.e. **target_next_move** equals 0), since the closing price utilizied is based on the **block_size** of 2. This process is repeated for every applicable 2 hour interval for each day in the data set; That is, a **block_size** of 2 does *NOT* include analyzis starting at 3:00 PM EST (since it would bleed into 5:00 PM EST, which is not within the utilized dataset)

As a whole, this repo analyzes **thresholds** 0.001 to 0.0045 in 0.0005 increments (0.001, 0.0015, ..., 0.0045) for all **block_size** 1 to 6 inclusive.

---

# Data Set Information
Data is sourced via Kaggle by user geowtt: https://www.kaggle.com/datasets/geowtt/hourly-stock-prices-technical-indicators-2023  

Dataset follows a Attribution 4.0 International License, as described here: https://creativecommons.org/licenses/by/4.0  

---

# Below is a copy of geowtt's descript of the dataset 

## Hourly Stock Prices + Technical Indicators (2023)

This dataset contains **hourly OHLCV price data** and key **technical indicators**
for 8 major U.S. tickers across different sectors. Perfect for time series forecasting,
technical analysis, and machine learning projects.

**Coverage:** January 3, 2023 – December 18, 2023  
**Symbols:** AAPL, MSFT, NVDA, JPM, XOM, SPY, TSLA, AMZN  
**Records:** 11,202  
**Size:** 2.16 MB  

---

### 📊 Columns

| Column | Description |
|---------|-------------|
| timestamp | Date & time in UTC (YYYY-MM-DD HH:MM:SS) |
| symbol | Stock ticker |
| open, high, low, close, volume | OHLCV data |
| sma_10, sma_50 | Simple moving averages |
| ema_20 | Exponential moving average |
| rsi_14 | Relative Strength Index |
| macd, macd_signal, macd_hist | MACD components |
| volatility_20 | Rolling volatility (20-hour window) |
| target_up_next | Binary target: 1 if next hour close ≥ 0.05% higher |

---

### ⚙️ Technical Details

- **Data source:** Publicly available financial market data (2023), aggregated and preprocessed to include technical indicators and binary movement labels.
- **Interval:** 1 hour (aggregated from minute-level data)
- **Technical indicators:** Calculated using pandas with proper groupby operations per symbol
- **Missing values:** 16 rows (0.14%) in `volatility_20` column - occurs at the start of each symbol's time series where insufficient history exists for 20-hour rolling window
- **Timestamps:** UTC format, ISO 8601 compliant (`YYYY-MM-DD HH:MM:SS`)
- **Metadata:** `metadata.json` contains full dataset generation details including date ranges, symbols, and target threshold

### 📈 Data Quality

- ✅ No duplicate records
- ✅ All prices positive and valid
- ✅ All volumes positive
- ✅ Timestamps properly formatted
- ✅ Target variable balanced (41.75% ups, 58.25% downs)

---

### 🧠 Example Usage

#### Load and explore
```python
import pandas as pd

# Load dataset
df = pd.read_csv('hourly_stock_prices_technical_indicators.csv')
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Basic statistics
print(f"Total records: {len(df):,}")
print(f"Symbols: {df['symbol'].nunique()}")
print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

# Target distribution per symbol
df.groupby('symbol')['target_up_next'].mean()
```

#### Time series analysis
```python
# Filter for specific symbol
aapl = df[df['symbol'] == 'AAPL'].set_index('timestamp')

# Plot price with moving averages
import matplotlib.pyplot as plt
aapl[['close', 'sma_10', 'sma_50', 'ema_20']].plot(figsize=(12, 6))
plt.title('AAPL Price with Technical Indicators')
plt.show()
```
