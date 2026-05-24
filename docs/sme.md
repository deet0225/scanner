# SME — Ranking Criteria

Universe: NSE Emerge + BSE SME IPO tickers (~130 tickers)  
Source: Screener.in (background cache, 48h TTL)  
Output: Top 30 ranked by Fund Score

---

## Performance

- **Parallel download**: 4 worker threads
- **Known-fail skip**: Clear gate failures use 72 h TTL instead of 48 h
- **Cache persistence**: `cache/sme_fundamentals_data.json` — survives server restarts

---

## Hard Gates (must pass before scoring)

| # | Criterion | Threshold | Rationale |
|---|-----------|-----------|-----------|
| 1 | ROCE | ≥ 15% | Higher efficiency bar for small companies |
| 2 | ROE (current/5Y/10Y) | ≥ 15% | Strong equity return needed from young companies |
| 3 | Debt / Equity | ≤ 1.5 | Growth capex allowed; more liberal than Nifty500 |
| 4 | Profit Growth (3Y) | ≥ 25% | Only genuine high-growth companies |
| 5 | Sales Growth (3Y) | ≥ 25% | Fast-growing revenue required |
| 6 | OPM | ≥ 8% | Core business must be operationally profitable |
| 7 | TTM Growth (best of profit/sales) | ≥ 15% | Recent momentum — proxy for order book strength |
| 8 | **Composite Cash Quality** | see below | Fraud / junk filter — replaces hard CFO > 0 |

NULL values (data pending) = gate not applied for that field.

---

## Composite Cash Quality Gate (Gate 8)

Positive CFO **always passes**. Only negative CFO triggers the three checks below:

| Layer | Check | Threshold | Meaning |
|-------|-------|-----------|---------|
| 1 | **CCR** = CFO ÷ Net\_Profit\_Est (MCap÷PE) | ≥ −1.0 | Burns MORE cash than profits claim → suspected fraud |
| 2 | **CF/Debt** = CFO ÷ Total\_Debt (only if D/E > 0.5) | ≥ −0.5 | CFO is −50% of debt → cannot service obligations |
| 3 | **Deep negative + no OPM** = CFO < −₹30 Cr AND OPM < 8% | fail if both true | Real operating loss, not just growth capex |

A company in scale-up phase may have temporary negative CFO from working-capital build  
or capex — it passes as long as none of the three red flags above are triggered.

---

## Fund Score (max 100 pts + 4 bonus)

### Quality (28 pts)
1. ROCE × 0.30  capped at 15 pts
2. ROE 10Y avg × 0.32  capped at 8 pts
3. Promoter%: ≥65% → 5 / ≥55% → 4 / ≥45% → 3 / ≥35% → 2 / ≥25% → 1

### Debt & Liquidity (15 pts)
4. D/E: 0 → 10 / ≤0.25 → 9 / ≤0.50 → 7.5 / ≤0.75 → 6 / ≤1.0 → 4.5 / ≤1.5 → 2.5 / ≤2.0 → 1 / >2.0 → 0
5. Current Ratio: ≥2.5 → 5 / ≥2.0 → 4 / ≥1.5 → 3 / ≥1.0 → 1.5 / <1.0 → 0

### Cash Flow (12 pts)
6. CFO Yield: ≥10% → 12 / ≥6% → 10 / ≥4% → 8 / ≥2% → 6 / ≥0.5% → 3 / ≥0% → 1 / <0% → 0  
   Sub-scores: CCR quality (+4 pts), CF/Debt quality (+2 pts), positive CFO sign (+1 pt)

### Growth (20 pts)
7. Revenue Growth (3Y+10Y weighted) × 0.40  capped at 10 pts
8. Profit Growth 3Y × 0.20  capped at 5 pts
9. Inst% (FII+DII): ≥30% → 5 / ≥20% → 4 / ≥10% → 3 / ≥5% → 2

### Value (15 pts)
10. OPM: ≥30% → 5 / ≥20% → 4 / ≥15% → 3 / ≥10% → 2 / ≥8% → 1  *(replaces PEG for SME)*
11. Earnings Yield × 0.33  capped at 5 pts

### Intrinsic Value (10 pts)
12. Graham MOS: ≥60% → 7 / ≥40% → 5.5 / ≥25% → 4 / ≥10% → 2.5 / ≥0% → 1
13. Market Cap: ≥₹1L Cr → 3 / ≥50K → 2.5 / ≥20K → 2 / ≥5K → 1.5 / ≥1K → 1

### Bonus (max +4 pts)
14. Dividend Yield > 0%  →  up to +2 pts
15. Profit Growth 10Y > 0%  →  up to +2 pts

---

## Table Columns

| Column | Description |
|--------|-------------|
| OPM % | Operating Profit Margin (replaces PEG for SME) |
| CFO / CCR | Cash from Ops with CCR and CF/Debt sub-lines |
| TTM ↑↓ | Acceleration arrow: ↑ TTM > 3Y CAGR, ↓ TTM < 3Y CAGR |

---

## Exchange Labels
| Label | Exchange |
|---|---|
| NSE Emerge | NSE SME platform |
| BSE SME | BSE SME IPO platform |

---

## Grade Thresholds
| Grade | Score |
|---|---|
| A | ≥ 75 |
| B | ≥ 55 |
| C | ≥ 35 |
| D | < 35 |
