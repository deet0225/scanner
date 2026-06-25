# Breakout Finder — Filter Criteria

Universe: Nifty 500 + Nifty Microcap 250 (750 tickers)  
Data source: Zerodha Kite Connect historical candles (yfinance fallback)  
History fetched: ~400 calendar days per symbol (enough for EMA200 + 52-week high)

---

## Hard Gates (all must pass)

All 7 conditions below must be true simultaneously. A stock failing any single gate
is excluded before scoring.

### 1. Price floor ≥ ₹20
Eliminates penny stocks and illiquid scrips whose price moves are erratic.

### 2. 50-Day High Breakout ≥ 0.3%
```
breakout_pct = (Close_today / max(High[-51:-1])) − 1  × 100
```
The **closing price** must be at least 0.3% above the highest high of the prior 50
trading sessions (today excluded). A 50-day lookback is used instead of the more
common 20-day because:
- 50-day resistance is a structurally significant level watched by institutional traders.
- It filters out intra-week noise and consolidation breaks that rarely follow through.

### 3. Volume Surge ≥ 1.5×
```
vol_ratio = Volume_today / mean(Volume[-21:-1])
```
Today's volume must be at least 1.5× the 20-day average. Volume is the single most
reliable breakout confirmation signal — it shows conviction behind the move.

### 4. Price above EMA20 > EMA50
```
Close > EMA(20) > EMA(50)
```
Ensures the stock is already in an uptrend at two timeframes — short-term (20D) and
medium-term (50D). Breakouts from downtrends or lateral ranges without EMA alignment
carry much higher failure risk.

### 5. RSI(14) between 55 and 80
- **Minimum 55**: some momentum must already be present.
- **Maximum 80**: avoids overbought stocks likely to see mean-reversion pullbacks
  immediately after entry. Stocks with RSI > 80 at breakout often give better
  re-entry on the first pullback.

### 6. ADX(14) ≥ 20
The Average Directional Index measures trend *strength*, not direction. ADX < 20
indicates a sideways / choppy market — breakouts in low-ADX environments have a
high failure rate. ADX ≥ 20 confirms the move has genuine directional energy.

### 7. Bullish Candle Close (top 45% of range)
```
candle_body_pct = (Close − Low) / (High − Low)  ≥ 0.45
```
The close must be in the upper 45% of the day's High−Low range. This filters out
breakout attempts that end as bearish wicks (stock broke out intraday but sellers
pushed it back down by close — a classic bull-trap candle).

---

## Trade Planning (calculated, not gates)

### Stop Loss
Placed using ATR to account for each stock's actual volatility:

```
atr_stop  = Low_today − 0.5 × ATR(14)
ema_stop  = EMA(20) × 0.98
low_stop  = min(Low[-51:-1]) × 0.99
stop_loss = max(atr_stop, ema_stop, low_stop)
stop_loss = max(stop_loss, Close × 0.90)   # hard cap: no more than 10% drawdown
```

The ATR-based stop respects each stock's typical daily noise range. The 0.5 ATR
buffer below today's low reduces the chance of being shaken out by normal volatility.
The hard cap prevents the stop from being placed unreasonably far away.

### Target Price
```
target = Close + 2.5 × ATR(14)
```
A 2.5 ATR target gives a projected move proportional to the stock's own volatility,
making it comparable across different price ranges and sectors.

### Risk-to-Reward Ratio
```
risk = Close − stop_loss
reward = target − Close
rr_ratio = reward / risk
```
Displayed in the UI. Entries with R:R ≥ 2:1 (green) offer the best expected value
for swing trades. R:R ≥ 1.5:1 (amber) is acceptable.

---

## 52-Week High Flag

```
pct_from_52w = (Close / max(High[-253:-1])) − 1  × 100
is_52w_break = pct_from_52w ≥ −0.5%
```

Stocks at or within 0.5% of their 52-week high receive a **52W** badge in the UI.
New 52-week high breakouts are the strongest class of breakout — they signal that
all prior sellers are now underwater and supply overhang is minimal.

---

## Scoring Formula

Stocks passing all gates are ranked by a composite score (0–100+):

| Component | Calculation | Max pts |
|-----------|-------------|---------|
| Volume surge | `min(vol_ratio − 1, 4) × 10` | 40 |
| 52W high | `+30` if `is_52w_break`, else `max(0, 10 + pct_from_52w × 1.5)` | 30 |
| RSI quality | `max(0, (rsi−55) × 1.2)`, penalised by `(rsi−72) × 2.5` if RSI > 72 | ~20 |
| ADX strength | `min(adx − 20, 30) × 1.0` | 30 |
| Candle body | `candle_body_pct × 10` | 10 |
| Breakout margin | `min(breakout_pct, 5) × 3` | 15 |
| EMA200 alignment | `min(ema20_vs_ema200_gap%, 15) × 0.4` | 6 |

**Interpretation:**
- **Score > 70**: High-quality swing trade setup. Strong volume, near 52W high, good trend.
- **Score 50–70**: Solid setup. Worth watching; confirm with broader market.
- **Score < 50**: Passed gates but weaker signal. Needs additional discretionary review.

---

## Data & API

| Item | Detail |
|------|--------|
| API route | `GET /api/breakout` |
| Historical mode | `GET /api/breakout?date=YYYY-MM-DD` |
| Force rescan | `POST /api/breakout/rescan` |
| Primary data | Zerodha Kite Connect `/instruments/historical/{token}/day` |
| Fallback data | Yahoo Finance chart API v8 (`query1.finance.yahoo.com`) |
| History window | `to_date − 400 calendar days` (≈280 trading days) |
| Min rows required | 120 trading days |
| Workers | 6 parallel threads |
| Live quote patch | Zerodha `/quote` API patches today's candle in live mode |

---

## Parameters Reference

```python
BK_LOOKBACK_DAYS    = 50     # rolling high lookback (trading days)
BK_BREAKOUT_MIN_PCT = 0.3    # minimum close above 50D high (%)
BK_VOL_RATIO_MIN    = 1.5    # minimum vol / 20D-avg-vol ratio
BK_RSI_MIN          = 55.0   # minimum RSI(14)
BK_RSI_MAX          = 80.0   # maximum RSI(14)
BK_ADX_MIN          = 20.0   # minimum ADX(14)
BK_MIN_PRICE        = 20.0   # minimum stock price (₹)
BK_CANDLE_BODY_MIN  = 0.45   # minimum candle close position in range
BK_MIN_ROWS         = 120    # minimum OHLCV rows needed
BK_MAX_WORKERS      = 6      # parallel scan threads
```
