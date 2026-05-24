# Fundamentals — Ranking Criteria

Universe: Nifty 500 + Nifty Microcap 250 (750 tickers)  
Source: Screener.in (background cache, 48h TTL)  
Output: Top 30 ranked by Fund Score (only stocks with Fund Score ≥ 50 shown)

---

## Performance

- **Parallel download**: 5 worker threads — ~5–8× faster than sequential
- **Delta refresh**: Refresh button only re-downloads entries older than 4 h (not all 750)
- **Known-fail skip**: Stocks that clearly fail hard gates use 72 h TTL instead of 48 h
- **Cache persistence**: `cache/fundamentals_data.json` — survives server restarts

---

## Hard Gates (must pass before scoring)

| # | Criterion | Threshold | Rationale |
|---|-----------|-----------|-----------|
| 1 | ROCE | ≥ 12% | Capital efficiency — penalises high-debt or low-return businesses |
| 2 | ROE (10Y/5Y/TTM) | ≥ 12% | Durable return on equity — moat signal |
| 3 | Debt / Equity | ≤ 1.0 | Conservative leverage — financial resilience |
| 4 | Profit Growth (3Y) | ≥ 8% | No loss-makers or stagnant earners |
| 5 | Sales Growth (3Y) | ≥ 5% | Growing business, not contracting |
| 6 | Cash from Operations | > 0 | **Positive operating cash flow** — real earnings, not just accounting profit |

Stocks failing any available gate are excluded before scoring begins.  
NULL values (data pending) = gate not applied for that field.

---

## Fund Score (max 100 pts + 4 bonus)

### Quality (28 pts)
1. ROCE ≥ 50% → 15 pts  (scale: ROCE × 0.30, capped at 15)
2. ROE 10Y avg ≥ 25% → 8 pts  (scale: ROE × 0.32, capped at 8)
3. Promoter% ≥ 65% → 5 pts  / ≥55% → 4 / ≥45% → 3 / ≥35% → 2 / ≥25% → 1

### Debt & Liquidity (15 pts)
4. D/E = 0 → 10 pts  / ≤0.25 → 9 / ≤0.50 → 7.5 / ≤0.75 → 6 / ≤1.0 → 4.5 / ≤1.5 → 2.5 / ≤2.0 → 1 / >2.0 → 0
5. Current Ratio ≥ 2.5 → 5 pts  / ≥2.0 → 4 / ≥1.5 → 3 / ≥1.0 → 1.5 / <1.0 → 0

### Cash Flow (12 pts)
6. CFO Yield ≥ 10% → 12 pts  / ≥6% → 10 / ≥4% → 8 / ≥2% → 6 / ≥0.5% → 3 / ≥0% → 1 / <0% → 0  
   *(CFO Yield = Cash from Operations ÷ Market Cap × 100)*

### Growth (20 pts)
7. Revenue Growth (3Y+10Y composite) ≥ 25% → 10 pts
8. Profit Growth 3Y ≥ 25% → 5 pts
9. Inst% (FII+DII) ≥ 30% → 5 pts  / ≥20% → 4 / ≥10% → 3 / ≥5% → 2

### Value (15 pts)
10. PEG ≤ 0.5 → 10 pts  / ≤1.0 → 8 / ≤1.5 → 5.5 / ≤2.0 → 3 / ≤3.0 → 1 / >3.0 → 0
11. Earnings Yield ≥ 15% → 5 pts  (scale: EY × 0.33, capped at 5)

### Intrinsic Value (10 pts)
12. Graham MOS ≥ 60% → 7 pts  / ≥40% → 5.5 / ≥25% → 4 / ≥10% → 2.5 / ≥0% → 1
13. Market Cap ≥ ₹1,00,000 Cr → 3 pts  / ≥50K → 2.5 / ≥20K → 2 / ≥5K → 1.5 / ≥1K → 1

### Bonus (max +4 pts)
14. Dividend Yield > 0% → up to +2 pts  (DY × 0.5)
15. Profit Growth 10Y > 0% → up to +2 pts  (PG10Y × 0.08)

---

## Display Filter

| Filter | Value |
|--------|-------|
| Minimum Fund Score shown | ≥ 50 |
| Max results displayed | Top 30 |

---

## Grade Thresholds
| Grade | Score |
|---|---|
| A | ≥ 75 |
| B | ≥ 55 |
| C | ≥ 35 |
| D | < 35 |
