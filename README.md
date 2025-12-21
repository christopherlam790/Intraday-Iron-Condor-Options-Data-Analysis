# Intraday-Iron-Condor-Options-Data-Analysis
This project aims to analyze hourly stock data (provided by Kaggle user geowtt) in an attempt to derrive an edge in trading intraday iron condor options. 


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
