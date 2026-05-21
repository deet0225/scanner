# Stock Momentum — Filter Criteria

Universe: Nifty 500 + Nifty Microcap 250  
Purpose: Early-detection, quality-confirmed momentum — technicals only, no fundamentals

---

## Philosophy

Thresholds are set **one step below** the swing-trade gates so nascent up-moves are
captured *before* the move becomes obvious.  Quality is maintained by mandatory
**rising-trend confirmation** filters (RSI accelerating, weekly RSI trending up,
ADX strengthening, MACD histogram not contracting) so only genuinely building setups
pass — not stocks that briefly touch a threshold and reverse.

---

## Hard Filters (all must pass)

| # | Filter | Threshold | Rationale |
|---|--------|-----------|-----------|
| 1 | Avg Traded Value 20D | ≥ ₹2 Cr | Minimum liquidity for retail entry/exit |
| 2 | Volume Z-score (3-day avg) | ≥ 0.6 | Above-average accumulation occurring |
| 3 | Weekly RSI-14 | ≥ 55 | Weekly trend turning constructive |
| 3b | Weekly RSI **rising** | ≤ 2 pt pullback OK | Higher-timeframe trend pointing UP |
| 4 | Daily RSI-14 | ≥ 58 | Daily momentum zone (early, not peak) |
| 4b | RSI SMA-3 **rising** | must be > prev bar | Momentum accelerating, not stalling |
| 5 | ADX-14 | ≥ 20 | Directional trend establishing |
| 5b | +DI > −DI | always required | Direction is UP, not down |
| 5c | ADX **rising** | ≤ 3 pt dip OK | Trend strengthening, not exhausting |
| 6 | RS outperformance vs index | ≥ +2.5% over 20D | Emerging market leadership |
| 6b | 20-day absolute return | ≥ +3% | Stock is clearly moving |
| 7 | 5-day return | ≥ 0% | Not rolling over last week |
| 8 | EMA alignment | Price > EMA-20 > EMA-50 | Clean uptrend structure on daily chart |
| 9 | MACD(12,26,9) line | > Signal line AND > 0 | Bullish zone, trend still live |
| 9b | MACD histogram contraction | < 30% shrinkage vs prev bar | Momentum pulse still accelerating |

> **No** market cap, D/E ratio, HH20 breakout, closing range, ATR ceiling, weekly EMA
> threshold, or any other fundamental / swing-entry filter is applied.

---

## Stop Loss

Structural ATR-14 stop, bounded to keep risk sensible:

```
candidate   = max(candle_low, 3-bar swing low, entry − 1.0 × ATR14)
stop_loss   = clamp(candidate, entry − 1.5 × ATR14, entry − 0.5 × ATR14)
fallback    = entry × 0.95  (if ATR unavailable)
```

Displayed as a dedicated **Stop Loss** column in the UI showing ₹ price and % risk from entry.

---

## Pure Momentum Score (max 100 pts)

Weights tuned for **win-rate predictability** — components with the highest empirical
correlation to trade continuation receive the most weight.

| Component | Max pts | Formula | Rationale |
|---|---|---|---|
| RSI-14 zone | 30 | (RSI − 50) × 1.0, cap 30 | Primary momentum; 50→80 linear. >80 = overbought risk |
| RS outperformance | 25 | RS% × 2.5, cap 25 | **#1 win-rate predictor** — market leaders continue leading |
| ADX-14 strength | 20 | (ADX − 20) × 0.571, cap 20 | Trend conviction; ADX 20→55 linear |
| MACD histogram | 10 | hist × 200, cap 10 | Momentum acceleration confirmation |
| Weekly RSI-14 | 10 | (wRSI − 50) × 0.5, cap 10 | Higher-timeframe trend alignment |
| Volume Z-score | 5 | (VolZ − 0.5) × 2.5, cap 5 | Institutional accumulation activity |

**Total max = 100 pts.** Top 50 results shown, sorted by Mom Score descending.

---

## What Changed vs Previous Version

| Filter | Before | After | Effect |
|--------|--------|-------|--------|
| RSI-14 | ≥ 62 | ≥ 58 + SMA-3 rising | Earlier entry, only when accelerating |
| Weekly RSI | ≥ 60 | ≥ 55 + rising | Earlier entry, only upward weekly trend |
| ADX | ≥ 25 | ≥ 20 + rising | Catches trend early before it's established |
| Volume Z | ≥ 0.8 | ≥ 0.6 | More sensitive to accumulation start |
| RS outperf | ≥ 3% | ≥ 2.5% | Catches emerging leaders sooner |
| 20D return | ≥ 5% | ≥ 3% | Includes earlier-stage movers |
| MACD | line > sig, line > 0 | + histogram not contracting >30% | Rejects fading momentum |
| Stop Loss | shown in Ticker cell | **dedicated column** with % risk | Prominent risk visibility |

Net result: **more stocks caught earlier**, but only those with all trend/momentum
vectors converging upward — reducing false positives vs simply lowering thresholds alone.
