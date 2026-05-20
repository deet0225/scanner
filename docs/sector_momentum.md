# Sector Momentum — Ranking Criteria

Universe: 29 NSE + BSE sector indices  
Source: Yahoo Finance (disk cache, 5-min in-memory TTL)  
Output: All sectors ranked by Swing Score

---

## Metrics Computed per Sector Index
1. 3D return  =  (Close[−1] / Close[−4] − 1) × 100
2. 5D return  =  (Close[−1] / Close[−6] − 1) × 100
3. 20D return =  (Close[−1] / Close[−21] − 1) × 100
4. 50D return =  (Close[−1] / Close[−51] − 1) × 100
5. RSI-9 (daily)
6. RSI-14 (daily)
7. EMA-9, EMA-20, EMA-50
8. % above EMA-9  =  (Close / EMA-9 − 1) × 100
9. 5D RS vs Nifty500  =  5D return − Nifty500 5D return
10. 20D RS vs Nifty500 =  20D return − Nifty500 20D return
11. MACD histogram %  =  (MACD_line − Signal_line) / Close × 100  (using 12, 26, 9)

---

## Swing Score Formula
```
score = (5D return      × 0.30)
      + (5D RS vs Nifty × 0.25)
      + (3D return      × 0.20)
      + (RSI-9 zone     × 0.50)   ← [0–2] range; peaks at RSI-9 = 65
      + (% above EMA-9  × 0.08)
      + (MACD hist%     × 3.50)
```

### RSI-9 Zone Score
- `max(0,  2.0 − |RSI-9 − 65| × 0.10)`
- Sweet spot: RSI-9 = 65 → score = 2.0
- Decays outside 45–85 band → 0

---

## Trend Flags
- `above_ema`      → EMA-9 > EMA-20  (short-term swing trend)
- `above_ema_slow` → EMA-20 > EMA-50  (medium-term trend)

