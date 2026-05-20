# Stock Momentum — Filter Criteria

Universe: Nifty 500 + Nifty Microcap 250 (from existing scan results)  
Purpose: Strict high-momentum concentration — technicals only, no fundamentals

---

## Hard Filters (all must pass)
1. RSI-14 ≥ 62
2. Weekly RSI-14 ≥ 60
3. ADX-14 ≥ 25
4. Volume Z-score ≥ 0.8
5. RS outperformance vs index ≥ +3%
6. 20D return ≥ +5%
7. Avg Traded Value ≥ ₹2 Cr
8. EMA alignment: Price > EMA-20 > EMA-50
9. MACD(12,26,9): MACD line > Signal line AND MACD line > 0

> No market cap, D/E, or any other fundamental filter applied.

---

## Pure Momentum Score (max 100 pts)

Weights are tuned for **better winning-rate predictability** — components with the
highest empirical correlation to trade success receive more weight.

| Component | Max pts | Formula | Win-rate rationale |
|---|---|---|---|
| RSI-14 zone | 30 | (RSI − 50) × 1.0  capped at 30 | Primary momentum; 50→80 linear. Reduced from 35 because RSI > 80 signals overbought risk |
| RS outperformance | 25 | RS% × 2.5  capped at 25 | **#1 win-rate predictor** — market leaders continue leading. Boosted from 20 |
| ADX-14 strength | 20 | (ADX − 20) × 0.571  capped at 20 | Trend conviction; ADX 20→55 linear |
| MACD histogram | 10 | hist × 200  capped at 10 | Momentum acceleration — raised to reflect quality of trend |
| Weekly RSI-14 | 10 | (wRSI − 50) × 0.5  capped at 10 | Higher-timeframe alignment |
| Volume Z-score | 5 | (VolZ − 0.5) × 2.5  capped at 5 | Institutional activity confirmation |

> **Note:** Score was previously out of 110 (RSI weighted 35, RS 20, Vol 10, MACD 10).
> Redesigned to 100 with RS outperformance raised to 25 pts for better winning-rate bias.

Top 50 results shown, sorted by Mom Score descending.

