# Breakout Finder — Filter Criteria

Universe: Nifty 500 + Nifty Microcap 250 (750 tickers)  
Data source: Zerodha Kite Connect historical candles (yfinance fallback)  
History fetched: ~400 calendar days per symbol (enough for EMA200 + 52-week high + Cup patterns)

---

## Architecture Overview

Each stock passes through four ordered stages. Failure at any stage drops the stock
immediately — later (expensive) stages only run when all earlier stages pass.

| Stage | Scope | Gates |
|-------|-------|-------|
| **0 — Market Regime** | Scan-level | Aborts the entire scan if Nifty500 is bearish |
| **1 — Daily Fast Gates** | Per-stock | 6 cheap indicator checks |
| **2 — Weekly Trend Gate** | Per-stock | Weekly EMA + weekly RSI |
| **3 — Pattern Detection** | Per-stock | 5 chart patterns; pivot breakout confirmed |

Results are sorted by composite score; the highest-quality pattern is selected when
multiple patterns match the same stock.

---

## Stage 0 — Market Regime Gate

Runs **once per scan** before any stock is evaluated. If the gate fails the scan
returns immediately with `regime_ok: false` and an empty `stocks` list — there is no
point finding pattern breakouts in a market that is already rolling over.

### Conditions (both must pass)

**a. Nifty500 EMA(20) > EMA(50)**  
Confirms the medium-term market trend is bullish. If the 20-day EMA has crossed below
the 50-day EMA the broad market is in a downtrend and breakout failure rates spike.

**b. Nifty500 RSI(14) > 50 on at least 2 of the last 3 sessions**  
Confirms market momentum is healthy and not stalling. Requiring 2 of 3 days (not all
3) avoids being too twitchy on minor daily dips.

### Data source
Tries `^CRSLDX` (Yahoo Finance Nifty500 index) first, falls back to `NIFTYBEES.NS`
then `JUNIORBEES.NS`. If all sources fail the regime gate is skipped and the scan
proceeds (fail-open, never blocks on data outage).

### API response fields
```json
{
  "regime_ok":      true,
  "regime_summary": "Bullish — EMA20=24150 > EMA50=23800 | RSI>50 on 3/3 recent sessions"
}
```
When `regime_ok` is `false`, `stocks` is `[]` and the UI should display the
`regime_summary` string as a warning banner.

---

## Stage 1 — Daily Fast Gates (per-stock)

All 6 conditions are evaluated in order. A stock failing any gate is dropped
immediately — no further processing occurs.

### 1. Price floor >= Rs.20
Eliminates penny stocks and illiquid scrips.

### 2. Volume Surge >= 1.6x
```
vol_ratio = Volume_today / mean(Volume[-21:-1])
```
Today's volume must be at least 1.6x the 20-day average. A high-volume breakout
signals institutional participation — the conviction needed for a swing trade.
*(Raised from 1.4x to enforce higher-quality entries.)*

### 3. RSI(14) between 55 and 80
- **Minimum 55**: genuine upward momentum must be present.
- **Maximum 80**: avoids stocks already in overbought territory that tend to mean-revert.

*(RSI minimum raised from 52 to 55.)*

### 4. ADX(14) >= 22
Trend strength filter. ADX < 22 indicates a directionless, choppy market where
breakouts fail at a very high rate. ADX >= 22 confirms the trend is properly
established.  
*(Raised from 18 to 22.)*

### 5. Price above EMA20 > EMA50
```
Close > EMA(20) > EMA(50)
```
Short- and medium-term trend alignment. Both EMAs must be ordered bullishly.

### 6. Bullish Candle Close (top 50% of range)
```
candle_body_pct = (Close - Low) / (High - Low)  >= 0.50
```
Filters candles where sellers pushed price back significantly before close. A close in
the upper half of the day's range confirms buyers controlled the session.  
*(Raised from 0.40 to 0.50.)*

---

## Stage 2 — Weekly Trend Gate (per-stock)

Runs after all daily fast gates pass. Confirms the stock is in a genuine multi-week
uptrend — not just a single-day daily spike against a weekly downtrend.

Daily OHLCV is resampled to weekly bars (Friday close). Stocks with fewer than 20
complete weekly bars are passed through without this check (insufficient history).

### 7. Weekly close > weekly EMA(20)
```
weekly_close > EMA(weekly_close, 20)
```
The stock must be trading above its 20-week moving average. This is the primary
long-term trend filter — stocks below their 20-week EMA are in structural downtrends.

### 8. Weekly RSI(14) >= 55
Weekly momentum must be building. A weekly RSI below 55 means the multi-week trend is
still weak or recovering — not the right environment for a high-conviction swing entry.

---

## Stage 3 — Pattern Detection (per-stock)

All five pattern functions receive only **historical bars (today excluded)** as numpy
arrays so that today's breakout candle is not part of the pattern itself. Each function
tries multiple lookback lengths and returns the best match.

Return signature: `(detected: bool, pivot: float, quality: float, depth_pct: float, duration_bars: int)`

### Pattern Confirmation Gate
After all pattern functions run, only those where
```
last_close >= pivot x 1.0025   (today closed >= 0.25% above the pivot)
```
are retained. The 0.25% margin filters false breakouts where price merely touched the
pivot and closed flat.  
*(Raised from 0.10% to 0.25%.)*

When multiple patterns match, the one with the highest quality score is used.

```
breakout_pct = (last_close / pivot - 1) x 100
```

---

### Cup & Handle

**Shape**: U-shaped base followed by a brief handle pullback.

| Criterion | Requirement |
|-----------|-------------|
| Cup length | 40-100 bars |
| Cup depth | 10-45% from left rim to bottom |
| Right rim vs left rim | Right rim >= 97% of left rim |
| U-shape check | Middle third of cup must be below outer-third average |
| Handle length | 5-20 bars after cup |
| Handle retrace | 3-20% from right rim; must stay above mid-cup |
| Volume | Handle average volume < 85% of cup average (dries up) |
| Pivot | `max(right_rim, handle_high)` |

**Quality score components** (0-1):  
`ideal_depth (0.25) + rim_symmetry (0.25) + shallow_retrace (0.20) + vol_dry (0.15) + cup_length (0.15)`

---

### Ascending Triangle

**Shape**: Flat resistance ceiling + rising support (higher lows).

| Criterion | Requirement |
|-----------|-------------|
| Pattern length | 25-55 bars |
| Resistance touches | >= 2 bars within 1.5% of the highest high |
| Resistance flatness | Std-dev of touches / resistance < 1.8% |
| Support slope | OLS regression on the lows must be positive |
| Convergence | Support must have moved >= 8% of the way toward resistance |
| Pivot | Flat resistance line (max high of pattern) |

**Quality score components**:  
`r_touches/4 (0.40) + resistance_flatness (0.35) + convergence (0.25)`

---

### Bull Flag

**Shape**: Strong vertical pole followed by a tight sideways-to-down consolidation.

| Criterion | Requirement |
|-----------|-------------|
| Pole length | 5-13 bars |
| Pole move | >= 8% from pole low to pole high |
| Pole peak | Must be in latter half of pole bars (not a spike at bar 1) |
| Flag length | 5-18 bars |
| Flag range | < 9% (High - Low) / High |
| Flag slope | Flat or slightly downward (slope / flag_high <= 0.004) |
| Flag retrace | <= 55% of pole move |
| Volume | Flag avg volume < 75% of pole avg volume |
| Pivot | Top of the flag (flag high) |

**Quality score components**:  
`pole_size (0.35) + flag_tightness (0.30) + vol_dry (0.20) + flag_length (0.15)`

---

### Flat Base

**Shape**: Extended horizontal consolidation with very tight price action.

| Criterion | Requirement |
|-----------|-------------|
| Base length | 25-50 bars |
| Base depth | (high - low) / high <= 12% |
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
| Rectangle length | 18-55 bars |
| Channel width | 7-25% (resistance - support) / resistance |
| Resistance touches | >= 2 bars within 1.5% of rectangle high |
| Support touches | >= 2 bars within 1.5% of rectangle low |
| Resistance cluster | Std-dev of resistance touches / resistance < 2% |
| Volume contraction | Rectangle avg volume < 85% of pre-rectangle avg |
| Pivot | Rectangle resistance (max high of rectangle) |

**Quality score components**:  
`(r_touches + s_touches)/6 (0.35) + channel_narrowness (0.40) + vol_dry (0.25)`

---

## Trade Planning (calculated, not gates)

### Stop Loss
```
atr_stop  = Low_today - 0.5 x ATR(14)
ema_stop  = EMA(20) x 0.98
low_stop  = min(Low[-51:-1]) x 0.99
stop_loss = max(atr_stop, ema_stop, low_stop)
stop_loss = max(stop_loss, Close x 0.90)   # hard cap: max 10% drawdown
```

### Target Price (Pattern-Depth Projection)
The target uses the **measured-move method** — the pattern height projected above the
pivot. This is more accurate for swing trades than a flat ATR multiple because larger,
deeper patterns store more energy and produce proportionally larger breakout moves.

```
pattern_target = pivot x (1 + depth_pct / 100)
atr_target     = Close + 2.5 x ATR(14)
target         = max(pattern_target, atr_target)   # take the higher projection
```

Where `depth_pct` is the pattern height as a percentage (e.g. 25 for a 25%-deep Cup).
The ATR target acts as a floor for shallow patterns.

### Risk-to-Reward Ratio
```
risk     = Close - stop_loss
reward   = target - Close
rr_ratio = reward / risk
```
R:R >= 2.5:1 (green) is ideal for swing trades. R:R >= 1.5:1 (amber) is acceptable.

---

## 52-Week High Flag
```
pct_from_52w = (Close / max(High[-253:-1]) - 1) x 100
is_52w_break = pct_from_52w >= -0.5%
```
Stocks at or within 0.5% of their 52-week high get a **52W** badge. These are the
strongest breakouts — all prior sellers are underwater, supply overhang is minimal.

---

## Scoring Formula

Stocks passing all gates are ranked by composite score (0-160+):

| Component | Calculation | Max pts |
|-----------|-------------|---------|
| Volume surge | `min(vol_ratio - 1, 4.0) x 10` | 40 |
| 52W high | `+30` if `is_52w_break`, else `max(0, 10 + pct_from_52w x 1.5)` | 30 |
| RSI quality | `max(0, (rsi - 55) x 1.2)` minus `(rsi - 72) x 2.5` if RSI > 72 | ~20 |
| ADX strength | `min(adx - 18, 32) x 1.0` | 32 |
| Candle body | `candle_body_pct x 10` | 10 |
| **Pattern quality** | `pattern_quality x 25` | **25** |
| Breakout margin | `min(breakout_pct, 5.0) x 2.0` | 10 |
| EMA200 alignment | `min(ema20_vs_ema200_gap%, 15) x 0.4` | 6 |

**Interpretation:**
- **Score > 90**: Excellent pattern + strong momentum. High-confidence swing trade.
- **Score 70-90**: Good pattern with solid confirmation. Standard swing trade entry.
- **Score 50-70**: Pattern detected but weaker confirmation. Review manually.
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
| History window | `to_date - 400 calendar days` (~280 trading days) |
| Min rows required | 120 trading days |
| Workers | 6 parallel threads |
| Live quote patch | Zerodha `/quote` API patches today's candle in live mode |

---

## Parameters Reference

```python
BK_PIVOT_MIN_PCT   = 0.25   # min % today's close must be above pattern pivot
BK_VOL_RATIO_MIN   = 1.6    # minimum vol / 20D-avg-vol ratio
BK_RSI_MIN         = 55.0   # minimum daily RSI(14)
BK_RSI_MAX         = 80.0   # maximum daily RSI(14)
BK_ADX_MIN         = 22.0   # minimum ADX(14)
BK_MIN_PRICE       = 20.0   # minimum stock price (Rs.)
BK_CANDLE_BODY_MIN = 0.50   # minimum candle close position in day's range (top 50%)
BK_WEEKLY_RSI_MIN  = 55.0   # minimum weekly RSI(14)
BK_MIN_ROWS        = 120    # minimum OHLCV rows needed
BK_MAX_WORKERS     = 6      # parallel scan threads
```
