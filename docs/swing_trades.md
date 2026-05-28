# Swing Trades — Filter Criteria

Universe: Nifty 500 + Nifty Microcap 250 (750 tickers)

---

## Regime (market-wide gate)
1. Nifty500 EMA-20 > EMA-50
2. Nifty500 RSI-14 > 50 for ≥ 2 of last 3 days

---

## Liquidity
3. Avg Traded Value 20D > ₹5 Cr
4. Volume Z-score (3-day avg) > 1.0
5. Relative Volume Percentile (60-session) > 60

---

## Trend
6. EMA-20 > EMA-50 (daily)
7. Weekly close > Weekly EMA-20
8. ADX-14 > 25 AND +DI > −DI

---

## Momentum
9. RSI-14 > 60 AND RSI SMA-3 is rising
10. Weekly RSI-14 > 57
11. Stock 20D return − Index 20D return > +5%
12. Stock 20D return > Sector avg 20D return + 4%

---

## Entry / Price Structure
13. Closing range ≥ 65%  →  (Close − Low) / (High − Low) ≥ 0.65
14. Close ≤ EMA-20 + 1.5 × ATR-14  (not overextended)
15. Gap-up from previous close ≤ 3%

---

## Fundamentals (info-only — gate disabled)
- Market Cap > ₹500 Cr
- D/E < 3.0

---

## Stop Loss
- max(3-bar structural low,  Entry − 1.0 × ATR-14)
- Bounded: [Entry − 1.5×ATR,  Entry − 0.5×ATR]

---

## Composite Score Weights
| Factor | Weight |
|---|---|
| Momentum vs index | 32% |
| ADX trend strength | 20% |
| Volume | 15% |
| RS vs sector | 15% |
| RSI | 10% |
| EMA position | 8% |
