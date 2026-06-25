# Nifty 500 Stock Scanner

A near real-time multi-factor stock screener for NSE-listed Nifty 500 stocks, with a live web dashboard.

## Tech Stack
- **Backend**: Python 3.9+, FastAPI, APScheduler, yfinance, pandas, numpy
- **Frontend**: Bootstrap 5 dark dashboard, Server-Sent Events (SSE)
- **Port**: **8000**

## Filters Applied (all must pass)
| Filter | Threshold |
|--------|-----------|
| P/E Ratio | < 20 (TTM, Yahoo Finance) |
| Volume Spike | Current day > 2× 20-day avg |
| RSI (14-period) | > 55 |
| Price vs 50 DMA | Price must be above |
| MACD | MACD line > Signal line |
| Sector Strength | Sector 20-day avg return > market median |

## Composite Ranking Score
```
score = (20−PE)/20 × 0.30
      + min(vol_ratio/10,1) × 0.30
      + (RSI−55)/45 × 0.20
      + min(pct_above_50DMA/20,1) × 0.20
```

## Setup & Run

### Option 1 — IntelliJ IDEA (recommended)

**Set the Python interpreter**
1. `File` → `Project Structure` → `SDKs` → `+` → `Add Python SDK`
2. Pick **Existing environment** → browse to `.venv\Scripts\python.exe`
3. Apply

**Create a Run Configuration**
1. `Run` → `Edit Configurations` → `+` → **Python**
2. Set:
   - **Name**: `Scanner`
   - Select **Module name** (not Script): `uvicorn`
   - **Parameters**: `main:app --host 0.0.0.0 --port 8000`
   - **Working directory**: `C:\Rama\Work\Work\scanner`
   - **Interpreter**: the `.venv` interpreter above
3. Click **OK** → press ▶ (`Shift+F10`)

**Or use the IntelliJ built-in Terminal** (`Alt+F12`):
```powershell
cd C:\Rama\Work\Work\scanner
.venv\Scripts\uvicorn.exe main:app --host 0.0.0.0 --port 8000
```

### Option 2 — PowerShell (any terminal)

```powershell
# 1. Create virtual environment (first time only)
python -m venv .venv

# 2. Install dependencies (first time only)
.venv\Scripts\pip.exe install -r requirements.txt

# 3a. Activate venv, then start
.venv\Scripts\Activate.ps1
uvicorn main:app --host 0.0.0.0 --port 8000

# 3b. OR start without activating
.venv\Scripts\uvicorn.exe main:app --host 0.0.0.0 --port 8000
```

Open **http://localhost:8000** in your browser.

## Upstox Breakout Tab Setup

Set your Upstox access token before starting the app to enable the
"Upstox Breakout" tab:

```powershell
$env:UPSTOX_ACCESS_TOKEN = "your_upstox_access_token"
```

The tab scans Nifty 500 + Microcap 250 using Upstox daily candles and ranks
fresh breakouts.

## Project Structure
```
scanner/
├── CLAUDE.md              AI agent instructions
├── README.md
├── requirements.txt
├── main.py                FastAPI app + SSE stream + APScheduler
├── scanner.py             Core scanning logic (filters, indicators, ranking)
├── tickers.py             ~260 NSE ticker symbols (update quarterly)
└── templates/
    └── index.html         Bootstrap 5 dark dashboard
```

## Updating the Ticker List
Download the latest constituent list from NSE and update `tickers.py`:
```
https://www.niftyindices.com/IndexConstituents/ind_nifty500list.csv
```
Append `.NS` to each symbol for Yahoo Finance compatibility.

## Notes
- No API key required — yfinance scrapes Yahoo Finance
- P/E data may be unavailable (`None`) for some stocks; they are skipped
- Sector strength is calculated relative to the median 20-day return of all scanned stocks
- Run during NSE trading hours (09:15–15:30 IST) for intraday volume accuracy

