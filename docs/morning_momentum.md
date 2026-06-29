# Morning Momentum — Filter Criteria

Universe: Nifty 500 + Nifty Microcap 250 (750 tickers)  
Purpose: Bullish-reversal detection — strict 3-candle Morning Star / Morning Doji Star pattern  
Source: OHLCV disk cache (cache-first; no live download when cache is fresh)

---

## Philosophy

The Morning Star scan is **reversal-focused** — it looks for stocks that have pulled
back and are showing the classic 3-candle bullish-reversal formation. Unlike the
momentum scan (which requires an established uptrend), this scan deliberately targets
stocks in or near a trough, identifying potential inflection points early.

No regime check, no benchmark, no momentum/swing criteria — purely pattern + quality gates.

---

## Quality Filters (all must pass before pattern check)

| # | Filter | Threshold | Rationale |
|---|--------|-----------|-----------|
| Q1 | Avg Traded Value 20D | ≥ ₹4 Cr | Minimum liquidity for reliable entries/exits |
| Q2 | Prior decline | ≥ 4% trough in last 6 bars vs close ~10 bars prior | Confirms a meaningful pullback, not shallow noise |
| Q3 | Morning Star pattern | strict 3-candle criteria (see below) | The reversal signal itself |
| Q4 | RSI overextension check | RSI-14 ≤ 70 | Avoids late-stage, overextended rebounds |
| Q5 | Volume confirmation | 3-bar avg volume ≥ 1.1× 20D avg **and** Day-3 volume ≥ 1.2× 20D avg | Buyer conviction on reversal day + follow-through |

---

## Morning Star Pattern — Strict 3-Candle Criteria

Only the **most recent 3 bars** are checked (no sliding window).  
All criteria must be met simultaneously.

| Day | Role | Requirement |
|-----|------|-------------|
| Day 1 | Large Bearish | body ≥ 1% of price  **AND**  body ≥ 0.3 × ATR-14 |
| Day 2 | Indecision Star | \|close − open\| < 30% of Day-1 body |
| Day 3 | Bullish Recovery | close > open  **AND**  close ≥ Day-1 close + 55% × Day-1 body  **AND**  body ≥ 55% of Day-1 body  **AND**  close ≥ max(Day-2 open, Day-2 close) |

### Pattern Quality Levels
- **Minimum**: Day-3 closes above the midpoint of Day-1's body (50% penetration)
- **Full engulf**: Day-3 closes above Day-1's open — highest-conviction reversal

---

## Star Quality Score (0–100)

Four components weighted to reflect empirical reversal reliability:

| Component | Weight | Formula | Notes |
|-----------|--------|---------|-------|
| Candle penetration | 30% | `(penetration − 0.50) / 0.50`, full engulf → 1.0 | Higher close = stronger reversal |
| Volume surge | 25% | `(vol_ratio − 1.0) / 1.5`, cap 1.0 | vol_ratio = 3-bar avg ÷ 20D avg volume |
| Oversold entry | 25% | tent function, peak at RSI 20–50 | Lower RSI = more room to recover |
| Prior decline depth | 20% | `(decline% − 3%) / 12%`, cap 1.0 | Deeper drop = more meaningful bounce |

**Score = (pen × 0.30 + vol × 0.25 + rsi × 0.25 + decline × 0.20) × 100**

---

## Displayed Metrics (no gates — informational only)

| Column | Description |
|--------|-------------|
| RSI-14 | Daily RSI at pattern close; oversold (<40) adds to Star Score |
| 20D Return | Performance over last 20 sessions |
| Avg TV | Avg daily traded value (liquidity signal) |
| Stop Loss | ATR-14 structural stop: clamp(max(candle low, 3-bar low, price − ATR), [price − 1.5×ATR, price − 0.5×ATR]) |
| EMA20 vs 50 | Trend structure context — useful to assess whether reversal is counter-trend or with-trend |

---

## Key Differences vs Swing / Momentum Scans

| Aspect | Morning Momentum | Swing Trades | Stock Momentum |
|--------|-----------------|--------------|----------------|
| Entry style | Reversal (pullback trough) | Breakout / trend continuation | Early trend continuation |
| Regime gate | ❌ None | ✅ Nifty500 EMA + RSI | (none explicit) |
| RSI requirement | None (informational) | > 60 + rising | ≥ 60 + rising |
| ADX requirement | None | > 25, rising | ≥ 22, rising |
| MACD requirement | None | None (swing) | Line > Signal > 0 |
| EMA alignment required | None | Price > EMA-20 > EMA-50 | Price > EMA-20 > EMA-50 |
| Primary signal | 3-candle candlestick pattern | Multi-filter momentum | Momentum + trend quality |

---

## Cache Strategy

Historical date lookups are **nearly instant** once the OHLCV cache has been populated.

| Cache state | Action |
|-------------|--------|
| Fresh cache that covers target date | Slice to date — no download |
| Stale cache / doesn't reach target date | Incremental delta download only |
| No cache at all | Full `HIST_DAYS` download |

