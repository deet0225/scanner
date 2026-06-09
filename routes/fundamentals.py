"""
routes/fundamentals.py — Tab: Fundamentals
==========================================
Top 30 high-quality stocks from Nifty500 + Microcap250 ranked by strict
fundamental gates (ROCE, ROE, D/E, profit growth, sales growth, CFO)
and a 12-factor quality score.

Cache: cache/fundamentals_data.json — persists across restarts.
Background worker: 12 parallel threads, delta-only refresh.

API routes
----------
GET  /api/fundamentals              — top 30 fundamentally strong stocks
POST /api/fundamentals/clear-cache  — wipe cache and force full re-download
"""

import asyncio
import json
import logging
import math
import time
import threading
from pathlib import Path as _PL

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import shared_state as ss
import cache as _ohlcv_cache
from tickers import NIFTY500_TICKERS, NIFTY_MICROCAP250_TICKERS
from routes.utils import _compute_stop_loss_from_cache

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------------------------------------------------------------------------
# Cache file + TTL constants
# ---------------------------------------------------------------------------
_FUND_CACHE_FILE        = _PL("cache/fundamentals_data.json")
_FUND_CACHE_TTL         = 48 * 3600   # 48 h per ticker
_FUND_FAIL_TTL          = 72 * 3600   # 72 h for known-fail tickers
_FUND_FORCE_REFRESH_TTL =  4 * 3600   # refresh entries older than 4 h on manual Refresh

# ---------------------------------------------------------------------------
# Hard gate + scoring thresholds
# ---------------------------------------------------------------------------
_FUND_MIN_SCORE         = 50.0
_FUND_ROCE_MIN          = 12.0
_FUND_ROE_MIN           = 12.0
_FUND_DE_MAX            =  1.0
_FUND_PROFIT_GROW_MIN   =  8.0
_FUND_SALES_GROW_MIN    =  5.0
_FUND_CFO_POSITIVE      = True
_FUND_MIN_KEY_FIELDS    =  2


# ---------------------------------------------------------------------------
# Disk cache load / save
# ---------------------------------------------------------------------------

def _fund_cache_load() -> None:
    """Load fundamentals disk cache into memory at startup."""
    try:
        if _FUND_CACHE_FILE.exists():
            ss._fund_data = json.loads(_FUND_CACHE_FILE.read_text(encoding="utf-8"))
            invalidated = 0
            # Only invalidate entries that have NO useful fundamental data at all.
            # Previous versions used strict three-group checks (basic/enhanced/cashflow),
            # but that perpetually invalidated financial-sector stocks (banks, NBFCs,
            # insurance) on every restart because they legitimately lack current_ratio,
            # cash_from_operations and cfo_yield on Screener.in — causing repeat
            # downloads that returned the exact same partial result every time.
            # Entries with partial data will be re-downloaded naturally when their
            # 48 h TTL expires; there is no need to force it at startup.
            _USEFUL_FIELDS = (
                "roce", "roe", "roe_5y",
                "profit_growth_3y", "profit_growth_5y",
                "sales_growth_pct", "sales_growth_5y",
                "market_cap_cr", "current_price",
            )
            _now = time.time()
            for entry in ss._fund_data.values():
                if isinstance(entry, dict) and entry.get("_ts", 0) > 0:
                    if not any(k in entry for k in _USEFUL_FIELDS):
                        # Only invalidate if the entry is already old enough to
                        # be re-downloaded anyway.  Tickers fetched recently but
                        # with no data (e.g. new listings not yet on Screener.in)
                        # should stay "fresh" until their TTL expires — resetting
                        # _ts unconditionally re-queues them on every restart and
                        # inflates the "downloading N tickers" counter.
                        ttl = _FUND_FAIL_TTL if entry.get("_gf") else _FUND_CACHE_TTL
                        if (_now - entry["_ts"]) >= ttl:
                            entry["_ts"] = 0
                            invalidated += 1
            logger.info("Fundamentals disk cache loaded: %d entries (%d invalidated)",
                        len(ss._fund_data), invalidated)
        else:
            ss._fund_data = {}
    except Exception as exc:
        logger.warning("Could not load fundamentals cache: %s", exc)
        ss._fund_data = {}


def _fund_cache_save() -> None:
    """Persist in-memory fundamentals cache to disk."""
    ss._fund_result_cache_valid = False
    try:
        _FUND_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _FUND_CACHE_FILE.write_text(
            json.dumps(ss._fund_data, ensure_ascii=False), encoding="utf-8"
        )
    except Exception as exc:
        logger.warning("Fundamentals cache save failed: %s", exc)


# ---------------------------------------------------------------------------
# Hard gates
# ---------------------------------------------------------------------------

def _passes_fund_gates(rec: dict) -> bool:
    """Hard fundamental quality gates — applied BEFORE scoring.

    Active gates (NULL = gate not applied for that field):
      ROCE ≥ 12%          Capital efficiency
      ROE  ≥ 12%          Return on equity
      D/E  ≤ 1.0          Low financial leverage
      Profit Growth ≥ 8%  No loss-makers / stagnant earners
      Sales  Growth ≥ 5%  Growing business
      CFO   > 0           Positive operating cash flow
    """
    key_vals = [
        rec.get("roce"),
        rec.get("roe") or rec.get("roe_5y") or rec.get("roe_10y"),
        rec.get("profit_growth_3y") or rec.get("profit_growth_5y"),
        rec.get("sales_growth_pct") or rec.get("sales_growth_5y"),
    ]
    if sum(1 for v in key_vals if v is not None) < _FUND_MIN_KEY_FIELDS:
        return False

    roce = rec.get("roce")
    if roce is not None and float(roce) < _FUND_ROCE_MIN:
        return False

    roe = rec.get("roe_10y") or rec.get("roe_5y") or rec.get("roe")
    if roe is not None and float(roe) < _FUND_ROE_MIN:
        return False

    de = rec.get("debt_equity")
    if de is not None and float(de) > _FUND_DE_MAX:
        return False

    pg = rec.get("profit_growth_3y") or rec.get("profit_growth_5y")
    if pg is not None and float(pg) < _FUND_PROFIT_GROW_MIN:
        return False

    sg = rec.get("sales_growth_pct") or rec.get("sales_growth_5y")
    if sg is not None and float(sg) < _FUND_SALES_GROW_MIN:
        return False

    if _FUND_CFO_POSITIVE:
        cfo = rec.get("cash_from_operations")
        if cfo is not None and float(cfo) <= 0:
            return False

    return True


# ---------------------------------------------------------------------------
# 12-factor quality score (max 100 pts + bonuses)
# ---------------------------------------------------------------------------

def _fund_quality_score(rec: dict) -> float:
    """
    Quality (28) + Debt & Liquidity (15) + Cash Flow (12) +
    Growth (20) + Value (15) + Intrinsic Value (10) = 100 base pts.
    Bonuses: dividend yield (+2) + 10Y profit growth (+2).
    """
    score = 0.0

    # Quality block ---------------------------------------------------------
    roce = rec.get("roce")
    if roce is not None:
        score += min(15.0, max(0.0, float(roce) * 0.30))

    roe_ref = rec.get("roe_10y") or rec.get("roe_5y") or rec.get("roe")
    if roe_ref is not None:
        score += min(8.0, max(0.0, float(roe_ref) * 0.32))

    ph = rec.get("promoter_holding")
    if ph is not None:
        ph = float(ph)
        if ph >= 65:    score += 5.0
        elif ph >= 55:  score += 4.0
        elif ph >= 45:  score += 3.0
        elif ph >= 35:  score += 2.0
        elif ph >= 25:  score += 1.0

    # Debt & Liquidity block ------------------------------------------------
    de = rec.get("debt_equity")
    if de is not None:
        de = float(de)
        if de == 0.0:       score += 10.0
        elif de <= 0.25:    score += 9.0
        elif de <= 0.50:    score += 7.5
        elif de <= 0.75:    score += 6.0
        elif de <= 1.00:    score += 4.5

    cr = rec.get("current_ratio")
    if cr is not None:
        cr = float(cr)
        if cr >= 2.5:    score += 5.0
        elif cr >= 2.0:  score += 4.0
        elif cr >= 1.5:  score += 3.0
        elif cr >= 1.0:  score += 1.5

    # Cash Flow block -------------------------------------------------------
    cfo_y = rec.get("cfo_yield")
    if cfo_y is not None:
        cfo_y = float(cfo_y)
        if cfo_y >= 10.0:   score += 12.0
        elif cfo_y >= 6.0:  score += 10.0
        elif cfo_y >= 4.0:  score += 8.0
        elif cfo_y >= 2.0:  score += 6.0
        elif cfo_y >= 0.5:  score += 3.0
        elif cfo_y >= 0.0:  score += 1.0

    # Growth block ----------------------------------------------------------
    sg   = rec.get("sales_growth_pct") or rec.get("sales_growth_ttm")
    sg10 = rec.get("sales_growth_10y")
    if sg is not None and sg10 is not None:
        score += min(10.0, max(0.0, (float(sg) * 0.6 + float(sg10) * 0.4) * 0.40))
    elif sg is not None:
        score += min(8.0, max(0.0, float(sg) * 0.32))
    elif sg10 is not None:
        score += min(6.0, max(0.0, float(sg10) * 0.24))

    pg = rec.get("profit_growth_3y") or rec.get("profit_growth_5y")
    if pg is not None:
        score += min(5.0, max(0.0, float(pg) * 0.20))

    inst = rec.get("inst_holding")
    if inst is not None:
        if inst >= 30:    score += 5.0
        elif inst >= 20:  score += 4.0
        elif inst >= 10:  score += 3.0
        elif inst >= 5:   score += 2.0
        else:             score += 1.0

    # Value block -----------------------------------------------------------
    peg = rec.get("peg_ratio")
    if peg is not None:
        peg = float(peg)
        if peg <= 0.5:    score += 10.0
        elif peg <= 1.0:  score += 8.0
        elif peg <= 1.5:  score += 5.5
        elif peg <= 2.0:  score += 3.0
        elif peg <= 3.0:  score += 1.0

    ey = rec.get("earnings_yield")
    if ey is not None:
        score += min(5.0, max(0.0, float(ey) * 0.33))

    # Intrinsic Value block -------------------------------------------------
    mos = rec.get("graham_mos")
    if mos is not None:
        mos = float(mos)
        if mos >= 60:     score += 7.0
        elif mos >= 40:   score += 5.5
        elif mos >= 25:   score += 4.0
        elif mos >= 10:   score += 2.5
        elif mos >= 0:    score += 1.0
        elif mos >= -20:  score += 0.5

    mc = float(rec.get("market_cap_cr") or 0)
    if mc >= 1_00_000:  score += 3.0
    elif mc >= 50_000:  score += 2.5
    elif mc >= 20_000:  score += 2.0
    elif mc >= 5_000:   score += 1.5
    elif mc >= 1_000:   score += 1.0
    else:               score += 0.5

    # Bonus points ----------------------------------------------------------
    dy = rec.get("dividend_yield")
    if dy is not None and float(dy) > 0:
        score += min(2.0, float(dy) * 0.5)

    pg10 = rec.get("profit_growth_10y")
    if pg10 is not None and float(pg10) > 0:
        score += min(2.0, float(pg10) * 0.08)

    return round(min(100.0, max(0.0, score)), 2)


# ---------------------------------------------------------------------------
# Per-ticker fetch + derived metrics
# ---------------------------------------------------------------------------

def _fund_refresh_ticker(ticker: str) -> tuple:
    """Fetch & cache fundamentals for one ticker.  Returns (result, content_changed)."""
    from data_sources import fundamentals as _fc

    result: dict = {}
    try:
        sector, de_x100, mc_inr = _fc.get(ticker)
        if sector:
            result["sector"] = sector
        if de_x100 is not None:
            result["debt_equity"]   = round(float(de_x100) / 100, 2)
        if mc_inr is not None:
            result["market_cap_cr"] = round(float(mc_inr) / 1e7, 0)
    except Exception:
        pass

    try:
        extra = _fc.get_extra_fundamentals(ticker)
        for k in ("roce", "roe", "roe_5y", "roe_10y",
                  "promoter_holding", "fii_holding", "dii_holding",
                  "sales_growth_pct", "sales_growth_5y", "sales_growth_10y",
                  "profit_growth_3y", "profit_growth_5y", "profit_growth_10y",
                  "sales_growth_ttm", "profit_growth_ttm",
                  "pe_ratio", "book_value", "dividend_yield",
                  "debt_equity", "market_cap_cr", "current_price",
                  "current_ratio", "cash_from_operations",
                  "opm", "net_profit_margin",
                  "sector", "industry"):
            if k in extra and extra[k] is not None:
                result[k] = extra[k]
    except Exception:
        pass

    # Derived metrics -------------------------------------------------------
    pe  = result.get("pe_ratio")
    bv  = result.get("book_value")
    cp  = result.get("current_price")
    pg3 = result.get("profit_growth_3y")
    pg5 = result.get("profit_growth_5y")
    mc  = result.get("market_cap_cr")
    cfo = result.get("cash_from_operations")

    eps = (cp / pe) if (cp and pe and pe > 0) else None
    growth_peg = pg3 if (pg3 and pg3 > 0) else (pg5 if pg5 and pg5 > 0 else None)

    if pe and pe > 0 and growth_peg and growth_peg > 0:
        result["peg_ratio"] = round(pe / growth_peg, 2)
    if pe and pe > 0:
        result["earnings_yield"] = round(100.0 / pe, 2)
    if cp and bv and bv > 0:
        result["pb_ratio"] = round(cp / bv, 2)
    if eps and eps > 0 and bv and bv > 0:
        try:
            graham = math.sqrt(22.5 * eps * bv)
            result["graham_number"] = round(graham, 2)
            if cp and cp > 0:
                result["graham_mos"] = round((graham - cp) / cp * 100, 1)
        except Exception:
            pass
    if cfo is not None and mc and mc > 0:
        result["cfo_yield"] = round(cfo / mc * 100, 2)

    fii = result.get("fii_holding")
    dii = result.get("dii_holding")
    if fii is not None and dii is not None:
        result["inst_holding"] = round(fii + dii, 2)
    elif fii is not None:
        result["inst_holding"] = fii

    sg3  = result.get("sales_growth_pct")
    sg10 = result.get("sales_growth_10y")
    if sg3 is not None and sg10 is not None:
        result["sales_growth_avg"] = round((sg3 + sg10) / 2, 1)

    # Content-change detection (excluding _ts) -------------------------------
    existing = ss._fund_data.get(ticker, {})
    content_changed = any(existing.get(k) != v for k, v in result.items())
    if not content_changed:
        content_changed = any(k not in result for k in existing if k != "_ts")

    result["_ts"] = time.time()
    with ss._fund_data_lock:
        ss._fund_data[ticker] = result
    return result, content_changed


# ---------------------------------------------------------------------------
# Parallel background worker
# ---------------------------------------------------------------------------

def _fund_bg_worker(tickers: list, generation: int = 0) -> None:
    """Download fundamentals for stale tickers in parallel.

    Worker count is taken from config.FUNDAMENTALS_THREADS (default 5) so
    it can be tuned without a code change.  Keeping it ≤ 5 avoids hammering
    screener.in with too many simultaneous requests from Render's fixed IP
    (the ScreenerClient rate-limiter provides the primary guard, but fewer
    workers means fewer goroutines competing for that single gate).

    `generation` is captured at launch — worker self-terminates when a newer
    generation is detected (Force-Live-Data clicked again).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    try:
        from config import FUNDAMENTALS_THREADS as _cfg_threads
    except Exception:
        _cfg_threads = 5

    MAX_WORKERS  = max(1, _cfg_threads)
    BATCH_SIZE   = 50
    total_refreshed = total_changed = total_skipped = 0
    now = time.time()

    stale = []
    for t in tickers:
        entry = ss._fund_data.get(t, {})
        ttl   = _FUND_FAIL_TTL if entry.get("_gf") else _FUND_CACHE_TTL
        if now - entry.get("_ts", 0) >= ttl:
            stale.append(t)
        else:
            total_skipped += 1

    if not stale:
        logger.info("Fundamentals BG: all %d tickers fresh — nothing to download", len(tickers))
        ss._fund_bg_running = False
        return

    logger.info("Fundamentals BG: %d stale / %d total — %d workers",
                len(stale), len(tickers), MAX_WORKERS)

    def _worker(t: str):
        result, changed = _fund_refresh_ticker(t)
        return t, result, changed

    for batch_start in range(0, len(stale), BATCH_SIZE):
        if ss._fund_cancel.is_set() or ss._fund_generation != generation:
            logger.info("Fundamentals BG (gen %d): stopping after %d/%d tickers",
                        generation, total_refreshed, len(stale))
            _fund_cache_save()
            ss._fund_bg_running = False
            return

        chunk = stale[batch_start: batch_start + BATCH_SIZE]
        fetched_in_batch = changed_in_batch = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(_worker, t): t for t in chunk}
            for future in as_completed(futures):
                if ss._fund_cancel.is_set() or ss._fund_generation != generation:
                    for f in futures:
                        f.cancel()
                    break
                t = futures[future]
                try:
                    _, result, changed = future.result()
                    key_coverage = sum(
                        1 for v in [result.get("roce"),
                                    result.get("roe") or result.get("roe_5y"),
                                    result.get("profit_growth_3y") or result.get("profit_growth_5y"),
                                    result.get("sales_growth_pct") or result.get("sales_growth_5y")]
                        if v is not None
                    )
                    with ss._fund_data_lock:
                        entry = ss._fund_data.get(t, {})
                        if key_coverage == 0:
                            # Screener.in returned no fundamental data at all —
                            # new listing or ticker not yet on Screener.  Mark as
                            # gate-failed so the longer 72 h retry TTL applies;
                            # between retries the ticker counts as "fresh" and
                            # doesn't bloat the download queue.
                            entry["_gf"] = True
                        elif key_coverage >= 3 and not _passes_fund_gates(result):
                            entry["_gf"] = True
                        else:
                            entry.pop("_gf", None)
                    if any(k in result for k in ("roce", "roe", "promoter_holding")):
                        fetched_in_batch += 1
                    if changed:
                        changed_in_batch += 1
                except Exception as exc:
                    logger.debug("Fund BG worker %s: %s", t, exc)

        total_refreshed += fetched_in_batch
        total_changed   += changed_in_batch
        end = min(batch_start + BATCH_SIZE, len(stale))
        if changed_in_batch:
            _fund_cache_save()
            logger.info("Fundamentals BG: %d/%d — %d fetched, %d changed → saved",
                        end, len(stale), fetched_in_batch, changed_in_batch)
        else:
            logger.info("Fundamentals BG: %d/%d — %d fetched, no content changes",
                        end, len(stale), fetched_in_batch)

    _fund_cache_save()
    ss._fund_last_completed_ts = time.time()
    logger.info("Fundamentals BG done (gen %d): %d fetched, %d changed, %d skipped",
                generation, total_refreshed, total_changed, total_skipped)
    ss._fund_bg_running = False


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/api/fundamentals")
async def get_fundamentals(refresh: int = 0) -> JSONResponse:
    """
    Top 30 stocks by strict fundamental quality from Nifty500 + Microcap250.

    Hard gates: ROCE≥12%, ROE≥12%, D/E≤1.0, ProfitGrowth3Y≥8%,
                SalesGrowth≥5%, CashFromOperations>0.

    Pass ?refresh=1 to force a background re-download of stale entries.
    """
    all_tickers: list = list(dict.fromkeys(NIFTY500_TICKERS + NIFTY_MICROCAP250_TICKERS))
    now = time.time()

    if refresh:
        for t in all_tickers:
            if t in ss._fund_data:
                if now - ss._fund_data[t].get("_ts", 0) > _FUND_FORCE_REFRESH_TTL:
                    ss._fund_data[t]["_ts"] = 0.0
        ss._fund_result_cache_valid = False

    # Build scan-state lookup for live price/tech fields
    scan_map: dict = {}
    for s in list(ss.scan_state.get("data") or []) + list(ss.mc_scan_state.get("data") or []):
        key = (s.get("ticker") or s.get("display_ticker", "")).upper()
        if key:
            scan_map[key] = s

    stale = [
        t for t in all_tickers
        if now - ss._fund_data.get(t, {}).get("_ts", 0)
           > (_FUND_FAIL_TTL if ss._fund_data.get(t, {}).get("_gf") else _FUND_CACHE_TTL)
    ]
    if not stale and not ss._fund_bg_running and ss._fund_last_completed_ts == 0:
        ss._fund_last_completed_ts = time.time()

    if stale and not ss._fund_bg_running:
        ss._fund_bg_running = True
        ss._fund_cancel.clear()
        priority_first = sorted(
            stale,
            key=lambda t: (
                1 if ss._fund_data.get(t, {}).get("_gf") else 0,
                0 if t.upper() in scan_map else 1,
            ),
        )
        gen = ss._fund_generation

        async def _bg_task(tickers=priority_first, g=gen):
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, _fund_bg_worker, tickers, g)
            except Exception:
                ss._fund_bg_running = False

        asyncio.create_task(_bg_task())

    cache_fresh = len(all_tickers) - len(stale)

    # Return cached result if still valid
    if not refresh and ss._fund_result_cache_valid and ss._fund_result_cache_body is not None:
        live_body = dict(ss._fund_result_cache_body)
        live_body["bg_running"]        = ss._fund_bg_running
        live_body["stale_count"]       = len(stale)
        live_body["cache_fresh"]       = cache_fresh
        live_body["last_completed_ts"] = ss._fund_last_completed_ts
        return JSONResponse(live_body)

    # Build combined records
    combined = []
    for ticker in all_tickers:
        key       = ticker.upper()
        cache_rec = ss._fund_data.get(key) or ss._fund_data.get(ticker) or {}
        scan_rec  = scan_map.get(key, {})
        if not cache_rec and not scan_rec:
            continue

        rec: dict = {
            "ticker":         ticker,
            "display_ticker": key.replace(".NS", "").replace(".BO", ""),
        }
        for k in ("sector", "industry", "debt_equity", "market_cap_cr", "current_price",
                  "roce", "roe", "roe_5y", "roe_10y",
                  "promoter_holding", "fii_holding", "dii_holding", "inst_holding",
                  "sales_growth_pct", "sales_growth_5y", "sales_growth_10y", "sales_growth_ttm",
                  "profit_growth_3y", "profit_growth_5y", "profit_growth_10y", "profit_growth_ttm",
                  "pe_ratio", "peg_ratio", "earnings_yield",
                  "book_value", "pb_ratio", "dividend_yield",
                  "graham_number", "graham_mos", "sales_growth_avg",
                  "current_ratio", "cash_from_operations", "cfo_yield",
                  "opm", "net_profit_margin"):
            v = cache_rec.get(k)
            if v is not None:
                rec[k] = v

        # Sector/industry fallback from static map
        sym_plain = rec["display_ticker"]
        _sm = ss._SECTOR_MAP.get(sym_plain, {})
        if not rec.get("sector")   and _sm.get("sector"):
            rec["sector"]   = _sm["sector"]
        if not rec.get("industry") and _sm.get("industry"):
            rec["industry"] = _sm["industry"]

        # Technical fields from scan state (price, RSI, RS — NOT D/E/market_cap)
        for k in ("sector", "score", "rsi", "return_20d", "rs_outperf_pct", "price"):
            v = scan_rec.get(k)
            if v is not None:
                rec[k] = v
        if "market_cap_cr" not in rec and scan_rec.get("market_cap_cr") is not None:
            rec["market_cap_cr"] = scan_rec["market_cap_cr"]
        if scan_rec.get("display_ticker"):
            rec["display_ticker"] = scan_rec["display_ticker"]
        if "price" in rec and "current_price" not in rec:
            rec["current_price"] = rec["price"]

        # Stop loss from OHLCV disk cache
        cp_val = rec.get("current_price") or rec.get("price")
        if cp_val:
            sl_val, atr_val = _compute_stop_loss_from_cache(ticker, float(cp_val))
            if sl_val is not None:
                rec["stop_loss"] = sl_val
                rec["atr14"]     = atr_val

        rec["fund_score"] = _fund_quality_score(rec)
        combined.append(rec)

    qualified = [
        r for r in combined
        if _passes_fund_gates(r) and r.get("fund_score", 0) >= _FUND_MIN_SCORE
    ]
    qualified.sort(key=lambda x: x["fund_score"], reverse=True)
    top30 = qualified[:30]
    for i, s in enumerate(top30, 1):
        s["fund_rank"] = i

    logger.info("Fundamentals: %d total, %d passed gates+score≥%.0f, showing top %d",
                len(combined), len(qualified), _FUND_MIN_SCORE, len(top30))

    response_body = {
        "stocks":             top30,
        "all_stocks":         qualified,
        "total":              len(qualified),
        "total_scored":       len(combined),
        "status":             "complete" if qualified else "no_data",
        "cache_fresh":        cache_fresh,
        "cache_total":        len(all_tickers),
        "bg_running":         ss._fund_bg_running,
        "stale_count":        len(stale),
        "last_completed_ts":  ss._fund_last_completed_ts,
        "last_updated_n500":  ss.scan_state.get("last_updated"),
        "last_updated_mc250": ss.mc_scan_state.get("last_updated"),
        "gates": {
            "roce_min":          _FUND_ROCE_MIN,
            "roe_min":           _FUND_ROE_MIN,
            "de_max":            _FUND_DE_MAX,
            "profit_growth_min": _FUND_PROFIT_GROW_MIN,
            "sales_growth_min":  _FUND_SALES_GROW_MIN,
            "cfo_positive":      _FUND_CFO_POSITIVE,
            "min_score":         _FUND_MIN_SCORE,
        },
    }
    ss._fund_result_cache_body  = response_body
    ss._fund_result_cache_valid = True
    return JSONResponse(response_body)


@router.post("/api/fundamentals/clear-cache")
async def fundamentals_cache_clear() -> JSONResponse:
    """Force a full re-download of all fundamentals data on the next request."""
    ss._fund_generation += 1
    with ss._fund_data_lock:
        deleted_count = len(ss._fund_data)
        ss._fund_data.clear()
    disk_deleted = False
    try:
        if _FUND_CACHE_FILE.exists():
            _FUND_CACHE_FILE.unlink()
            disk_deleted = True
    except Exception as exc:
        logger.warning("Could not delete fundamentals cache file: %s", exc)
    ss._fund_result_cache_valid = False
    ss._fund_result_cache_body  = None
    ss._fund_bg_running         = False

    screener_cleared = 0
    try:
        from data_sources import ScreenerClient as _SC
        screener_cleared = len(_SC._cache)
        _SC._cache.clear()
        _SC._cache_ts.clear()
    except Exception as exc:
        logger.warning("Could not clear ScreenerClient cache: %s", exc)

    logger.info("Fundamentals cache CLEARED: %d entries, %d Screener cache entries, disk %s",
                deleted_count, screener_cleared,
                "deleted" if disk_deleted else "delete-failed")
    return JSONResponse({
        "reset":            deleted_count,
        "screener_cleared": screener_cleared,
        "disk_deleted":     disk_deleted,
        "message": (
            f"{deleted_count} fundamentals entries cleared, "
            f"{screener_cleared} Screener.in HTML-cache entries cleared, "
            f"JSON file {'deleted' if disk_deleted else 'could not be deleted'} — "
            "full re-download will start on next tab visit"
        ),
    })

