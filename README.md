# NSE Stock Scanner

A multi-tab, near real-time stock screener for NSE-listed stocks (Nifty 500 + Microcap 250),
with a live web dashboard. Each tab runs an independent scan strategy.

## Tabs

| Tab | Strategy | Universe |
|-----|----------|----------|
| ☀ Morning Star | 3-candle bullish-reversal pattern | Nifty 500 + Microcap 250 |
| 🚀 Breakout | 50D high breakout — swing-trade quality gates | Nifty 500 + Microcap 250 |
| ⚡ Swing Trades | Multi-factor momentum + trend entry | Nifty 500 + Microcap 250 |
| 📈 Stock Momentum | Early momentum detection (RSI/ADX/MACD) | Nifty 500 + Microcap 250 |
| 📉 Sector Momentum | 29 NSE/BSE sector indices ranked by swing score | Sector indices |
| 💎 Fundamentals | Quality + value screening | Nifty 500 + Microcap 250 |
| 🏭 SME Fundamentals | High-growth quality screening | NSE Emerge + BSE SME |
| 🔍 Stock Search | Single stock deep-dive | Any NSE ticker |

See `docs/` for full filter criteria for each tab.

## Tech Stack

- **Backend**: Python 3.11+, FastAPI, APScheduler, uvicorn
- **Frontend**: Bootstrap 5 dark dashboard, Server-Sent Events (SSE) for live polling
- **Port**: 8000
- **Data sources**:
  - yfinance (NSE `.NS` primary, BSE `.BO` fallback)
  - TradingView via tvDatafeed (OHLCV fallback)
  - Zerodha Kite Connect (Breakout tab: historical candles + live quotes)
  - Screener.in (Fundamentals + SME)
  - NSE live quote API (market cap, live price)

## Setup & Run

### 1. Install dependencies

```powershell
cd C:\Rama\Work\scanner-z

# Create virtual environment (first time only)
python -m venv .venv

# Activate
.venv\Scripts\Activate.ps1

# Install packages
pip install -r requirements.txt

# tvDatafeed is not on PyPI — install from GitHub (optional; improves data quality)
pip install git+https://github.com/StreamAlpha/tvdatafeed.git
```

### 2. Run (without Zerodha)

```powershell
cd C:\Rama\Work\scanner-z
python main.py
```

All tabs work except the Breakout tab's live quote patch (falls back to yfinance-only).

### 3. Run (with Zerodha — enables Breakout tab live data)

```powershell
cd C:\Rama\Work\scanner-z
$env:ZERODHA_API_KEY      = "your_api_key"
$env:ZERODHA_ACCESS_TOKEN = "your_access_token"
python main.py
```

Open **http://localhost:8000** in your browser.

> **Zerodha access token** expires daily. Generate a fresh one from
> [Kite Connect](https://kite.trade/) each trading day before starting the app.

## Environment Variables

| Variable | Required | Purpose |
|----------|----------|---------|
| `ZERODHA_API_KEY` | Optional | Breakout tab — Kite Connect API key |
| `ZERODHA_ACCESS_TOKEN` | Optional | Breakout tab — daily session token |
| `APIFY_API_KEY` | Optional | Fundamentals — Apify screener actor fallback |
| `TRADINGVIEW_USERNAME` | Optional | TradingView login for tvDatafeed |
| `TRADINGVIEW_PASSWORD` | Optional | TradingView login for tvDatafeed |

## Feature Flags (`config.py`)

```python
ENABLE_ZERODHA         = True   # set False to force yfinance-only in Breakout tab
ENABLE_TRADINGVIEW     = True
ENABLE_APIFY_SCREENER  = True
ENABLE_NSE_PYTHON_HIST = True
```

## Project Structure

```
scanner-z/
├── README.md
├── requirements.txt
├── runtime.txt
├── render.yaml
├── main.py                   FastAPI app entry point + lifespan + router includes
├── config.py                 All tunable parameters and feature flags
├── scanner.py                Core OHLCV scanning engine (Swing, Momentum, Morning Star)
├── data_sources.py           Multi-source data clients (NSE, Screener.in, TradingView, …)
├── tickers.py                Nifty 500 + Microcap 250 ticker lists
├── sme_tickers.py            NSE Emerge + BSE SME ticker lists
├── sector_map.py             Sector → ticker mapping for sector momentum
├── shared_state.py           Shared mutable state (scanner instances, scan dicts, locks)
├── cache.py                  OHLCV disk cache with incremental update logic
├── routes/
│   ├── breakout.py           Tab: Breakout Finder (Zerodha + yfinance)
│   ├── swing_trades.py       Tab: Swing Trades
│   ├── stock_momentum.py     Tab: Stock Momentum
│   ├── morning_momentum.py   Tab: Morning Star
│   ├── sector_momentum.py    Tab: Sector Momentum
│   ├── fundamentals.py       Tab: Fundamentals
│   ├── sme.py                Tab: SME Fundamentals
│   ├── misc.py               Config, cache management, tab-active, root
│   └── utils.py              Shared helpers (date validation, SSE, stop-loss, industry)
├── templates/
│   └── index.html            Single-page Bootstrap 5 dark dashboard
├── cache/
│   ├── ohlcv/                Per-ticker OHLCV parquet files
│   └── sme/                  SME fundamental cache
└── docs/
    ├── breakout.md           Breakout filter criteria
    ├── swing_trades.md       Swing trade filter criteria
    ├── stock_momentum.md     Momentum filter criteria
    ├── morning_momentum.md   Morning Star pattern criteria
    ├── sector_momentum.md    Sector score formula
    ├── fundamentals.md       Fundamental scoring rubric
    └── sme.md                SME scoring rubric
```

## Updating Ticker Lists

```
Nifty 500:   https://www.niftyindices.com/IndexConstituents/ind_nifty500list.csv
Microcap 250: https://www.niftyindices.com/IndexConstituents/ind_niftymicrocap250_list.csv
```

Update `tickers.py` quarterly. Append `.NS` to each symbol for yfinance compatibility.

## Notes

- Run during NSE trading hours (09:15–15:30 IST) for live volume accuracy
- Breakout tab historical date picker: select any past date to see breakouts as of that date
- Morning Star tab also supports historical date picker
- Fundamentals data is cached for 48 h (Screener.in); hit Refresh to force delta update

