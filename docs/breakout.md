# Breakout Finder — Filter Criteria

Universe: Nifty 500 + Nifty Microcap 250 (750 tickers)  
Data source: Zerodha Kite Connect historical candles (yfinance fallback)  
History fetched: ~400 calendar days per symbol (enough for EMA200 + 52-week high + Cup patterns)

---

## Architecture Overview

Each stock is evaluated in two stages:

1. **Fast Gates** — cheap indicator checks that eliminate 90%+ of stocks before pattern detection.
2. **Pattern Detection** — 5 chart pattern functions run on all bars *excluding today*. Today's close must be ≥ 0.1% above the pattern pivot to count as an active breakout. If no pattern is confirmed, the stock is excluded regardless of gate values.

Results are sorted by composite score; the highest-quality pattern is selected when multiple patterns match the same stock.

---

## Stage 1 — Fast Gates (all must pass)

All 6 conditions are evaluated in order. A stock failing any gate is dropped immediately.

### 1. Price floor ≥ ₹20
Eliminates penny stocks and illiquid scrips.

### 2. Volume Surge ≥ 1.4×
```
vol_ratio = Volume_today / mean(Volume[-21:-1])
```
Today's volume must be at least 1.4× the 20-day average. Volume confirms conviction
behind the breakout. The threshold is slightly relaxed (vs the old 1.5×) because the
pattern structure itself already validates the setup.

### 3. RSI(14) between 52 and 80
- **Minimum 52**: some momentum must be present; not dead money.
- **Maximum 80**: avoids immediate overbought mean-reversion risk.

### 4. ADX(14) ≥ 18
Trend strength filter. ADX < 18 = choppy / directionless market; breakouts there fail
at a very high rate.

### 5. Price above EMA20 > EMA50
```
Close > EMA(20) > EMA(50)
```
Short- and medium-term trend alignment. Both must be ordered bullishly.

### 6. Bullish Candle Close (top 40% of range)
```
candle_body_pct = (Close − Low) / (High − Low)  ≥ 0.40
```
Filters bear-wick breakout candles where sellers pushed price back before close.

---

## Stage 2 — Pattern Detection

All five functions receive only **historical bars (today excluded)** as numpy arrays so
that today's breakout candle is not part of the pattern itself. Each function tries
multiple lookback lengths and returns the best match.

Return signature: `(detected: bool, pivot: float, quality: float, depth_pct: float, duration_bars: int)`

### Cup & Handle

**Shape**: U-shaped base followed by a brief handle pullback.

| Criterion | Requirement |
|-----------|-------------|
| Cup length | 40–100 bars |
| Cup depth | 10–45% from left rim to bottom |
| Right rim vs left rim | Right rim ≥ 97% of left rim |
| U-shape check | Middle third of cup must be below outer-third average |
| Handle length | 5–20 bars after cup |
| Handle retrace | 3–20% from right rim; must stay above mid-cup |
| Volume | Handle average volume < 85% of cup average (dries up) |
| Pivot | `max(right_rim, handle_high)` |

**Quality score components** (0–1):  
`ideal_depth (0.25) + rim_symmetry (0.25) + shallow_retrace (0.20) + vol_dry (0.15) + cup_length (0.15)`

---

### Ascending Triangle

**Shape**: Flat resistance ceiling + rising support (higher lows).

| Criterion | Requirement |
|-----------|-------------|
| Pattern length | 25–55 bars |
| Resistance touches | ≥ 2 bars within 1.5% of the highest high |
| Resistance flatness | Std-dev of touches / resistance < 1.8% |
| Support slope | OLS regression on the lows must be positive |
| Convergence | Support must have moved ≥ 8% of the way toward resistance |
| Pivot | Flat resistance line (max high of pattern) |

**Quality score components**:  
`r_touches/4 (0.40) + resistance_flatness (0.35) + convergence (0.25)`

---

### Bull Flag

**Shape**: Strong vertical pole followed by a tight sideways-to-down consolidation.

| Criterion | Requirement |
|-----------|-------------|
| Pole length | 5–13 bars |
| Pole move | ≥ 8% from pole low to pole high |
| Pole peak | Must be in latter half of pole bars (not a spike at bar 1) |
| Flag length | 5–18 bars |
| Flag range | < 9% (High − Low) / High |
| Flag slope | Flat or slightly downward (slope / flag_high ≤ 0.004) |
| Flag retrace | ≤ 55% of pole move |
| Volume | Flag avg volume < 75% of pole avg volume |
| Pivot | Top of the flag (flag high) |

**Quality score components**:  
`pole_size (0.35) + flag_tightness (0.30) + vol_dry (0.20) + flag_length (0.15)`

---

### Flat Base

**Shape**: Extended horizontal consolidation with very tight price action.

| Criterion | Requirement |
|-----------|-------------|
| Base length | 25–50 bars |
| Base depth | (high − low) / high ≤ 12% |
| Close tightness | Std-dev of closes / mean closes < 3.8% |
| Volume contraction | Second-half volume average < first-half average |
| Pivot | Base high (max high of the entire base) |

**Quality score components**:  
`depth_tightness (0.35) + close_tightness (0.40) + vol_contraction (0.25)`

---

### Rectangle

**Shape**: Horizontal channel with clearly defined resistance and support bands.

| Criterion | Requirement |
|-----------|-------------|
| Rectangle length | 18–55 bars |
| Channel width | 7–25% (resistance − support) / resistance |
| Resistance touches | ≥ 2 bars within 1.5% of rectangle high |
| Support touches | ≥ 2 bars within 1.5% of rectangle low |
| Resistance cluster | Std-dev of resistance touches / resistance < 2% |
| Volume contraction | Rectangle avg volume < 85% of pre-rectangle avg |
| Pivot | Rectangle resistance (max high of rectangle) |

**Quality score components**:  
`(r_touches + s_touches)/6 (0.35) + channel_narrowness (0.40) + vol_dry (0.25)`

---

### Pattern Confirmation Gate
After all pattern functions run, only those where  
```
last_close >= pivot × 1.001   (today closed ≥ 0.1% above the pivot)
```
are retained. If zero patterns pass this gate, the stock is excluded.  
When multiple patterns match, the one with the highest quality score is used.

```
breakout_pct = (last_close / pivot − 1) × 100
```

---

## Trade Planning (calculated, not gates)

### Stop Loss
```
atr_stop  = Low_today − 0.5 × ATR(14)
ema_stop  = EMA(20) × 0.98
low_stop  = min(Low[-51:-1]) × 0.99
stop_loss = max(atr_stop, ema_stop, low_stop)
stop_loss = max(stop_loss, Close × 0.90)   # hard cap: max 10% drawdown
```

### Target Price
```
target = Close + 2.5 × ATR(14)
```

### Risk-to-Reward Ratio
```
risk     = Close − stop_loss
reward   = target − Close
rr_ratio = reward / risk
```
R:R ≥ 2.5:1 (green) is ideal. R:R ≥ 1.5:1 (amber) is acceptable.

---

## 52-Week High Flag
```
pct_from_52w = (Close / max(High[-253:-1]) − 1) × 100
is_52w_break = pct_from_52w ≥ −0.5%
```
Stocks at or within 0.5% of their 52-week high get a **52W** badge. These are the
strongest breakouts — all prior sellers are underwater, supply overhang is minimal.

---

## Scoring Formula

Stocks passing all gates are ranked by composite score (0–160+):

| Component | Calculation | Max pts |
|-----------|-------------|---------|
| Volume surge | `min(vol_ratio − 1, 4.0) × 10` | 40 |
| 52W high | `+30` if `is_52w_break`, else `max(0, 10 + pct_from_52w × 1.5)` | 30 |
| RSI quality | `max(0, (rsi − 55) × 1.2)` − `(rsi − 72) × 2.5` if RSI > 72 | ~20 |
| ADX strength | `min(adx − 18, 32) × 1.0` | 32 |
| Candle body | `candle_body_pct × 10` | 10 |
| **Pattern quality** | `pattern_quality × 25` | **25** |
| Breakout margin | `min(breakout_pct, 5.0) × 2.0` | 10 |
| EMA200 alignment | `min(ema20_vs_ema200_gap%, 15) × 0.4` | 6 |

**Interpretation:**
- **Score > 90**: Excellent pattern + strong momentum. High-confidence swing trade.
- **Score 70–90**: Good pattern with solid confirmation. Standard swing trade entry.
- **Score 50–70**: Pattern detected but weaker confirmation. Review manually.
- **Score < 50**: Marginal setup; pass or wait for better entry.

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
BK_PIVOT_MIN_PCT   = 0.10   # min % today's close must be above pattern pivot
BK_VOL_RATIO_MIN   = 1.4    # minimum vol / 20D-avg-vol ratio
BK_RSI_MIN         = 52.0   # minimum RSI(14)
BK_RSI_MAX         = 80.0   # maximum RSI(14)
BK_ADX_MIN         = 18.0   # minimum ADX(14)
BK_MIN_PRICE       = 20.0   # minimum stock price (₹)
BK_CANDLE_BODY_MIN = 0.40   # minimum candle close position in day's range
BK_MIN_ROWS        = 120    # minimum OHLCV rows needed
BK_MAX_WORKERS     = 6      # parallel scan threads
```
