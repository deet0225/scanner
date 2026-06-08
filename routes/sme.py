"""
routes/sme.py — Tab: SME (NSE Emerge + BSE SME)
================================================
High-growth SME stocks ranked by a growth-acceleration score.
Uses STRICTER growth gates (25% 3Y CAGR) and a composite cash quality
check to filter out fake/junk companies common in the SME segment.

Cache: cache/sme_fundamentals_data.json — separate from main fund cache.

API routes
----------
GET  /api/sme/fundamentals              — top SME stocks by quality score
POST /api/sme/fundamentals/clear-cache  — wipe cache and force re-download
"""

import asyncio
import json
import logging
import math
import time
from pathlib import Path as _PL

import pandas as _pd
from fastapi import APIRouter
from fastapi.responses import JSONResponse

import cache as _ohlcv_cache
from cache import _ist_today as _sme_ist_today
import shared_state as ss
from routes.utils import _compute_stop_loss_from_cache

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------------------------------------------------------------------------
# Cache file + TTL constants
# ---------------------------------------------------------------------------
_SME_FUND_CACHE_FILE  = _PL("cache/sme_fundamentals_data.json")
_SME_FUND_CACHE_TTL   = 48 * 3600
_SME_FAIL_TTL         = 72 * 3600

# ---------------------------------------------------------------------------
# SME hard gate thresholds (stricter on growth, relaxed on debt)
# ---------------------------------------------------------------------------
_SME_ROCE_MIN         = 15.0
_SME_ROE_MIN          = 15.0
_SME_DE_MAX           =  1.5
_SME_PROFIT_GROW_MIN  = 25.0
_SME_SALES_GROW_MIN   = 25.0
_SME_OPM_MIN          =  8.0
_SME_TTM_GROW_MIN     = 15.0
_SME_CCR_MIN          = -1.0
_SME_CF_DEBT_MIN      = -0.5
_SME_CFO_DEEP_NEG     = -30.0
_SME_MIN_KEY_FIELDS   =  2


# ---------------------------------------------------------------------------
# Disk cache load / save
# ---------------------------------------------------------------------------

def _sme_cache_load() -> None:
    """Load SME fundamentals disk cache into memory at startup."""
    try:
        if _SME_FUND_CACHE_FILE.exists():
            ss._sme_fund_data = json.loads(
                _SME_FUND_CACHE_FILE.read_text(encoding="utf-8")
            )
            # Extract metadata key (not a ticker entry)
            ss._sme_last_download_date = ss._sme_fund_data.pop("__download_date__", "")
            invalidated = 0
            # Only invalidate entries that have NO useful fundamental data at all.
            # Many SME/Emerge stocks legitimately lack CFO or OPM data on Screener.in;
            # invalidating those on every restart causes perpetual re-downloads with no results.
            _USEFUL_FIELDS = (
                "roce", "roe", "roe_5y",
                "profit_growth_3y", "profit_growth_5y",
                "sales_growth_pct", "sales_growth_5y",
                "market_cap_cr", "current_price",
            )
            for entry in ss._sme_fund_data.values():
                if isinstance(entry, dict) and entry.get("_ts", 0) > 0:
                    if not any(k in entry for k in _USEFUL_FIELDS):
                        entry["_ts"] = 0
                        invalidated += 1
            logger.info("SME fund cache loaded: %d entries (%d invalidated), last_download_date=%s",
                        len(ss._sme_fund_data), invalidated, ss._sme_last_download_date or "none")
        else:
            ss._sme_fund_data = {}
    except Exception as exc:
        logger.warning("Could not load SME fund cache: %s", exc)
        ss._sme_fund_data = {}


def _sme_cache_save() -> None:
    """Persist in-memory SME cache to disk."""
    ss._sme_result_cache_valid = False
    try:
        _SME_FUND_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        # Include the download-date metadata so it survives server restarts
        data_to_save = dict(ss._sme_fund_data)
        data_to_save["__download_date__"] = ss._sme_last_download_date or str(_sme_ist_today())
        _SME_FUND_CACHE_FILE.write_text(
            json.dumps(data_to_save, ensure_ascii=False), encoding="utf-8"
        )
    except Exception as exc:
        logger.warning("SME fund cache save failed: %s", exc)


# ---------------------------------------------------------------------------
# Hard gates
# ---------------------------------------------------------------------------

def _passes_sme_gates(rec: dict) -> bool:
    """Hard fundamental gates for SME/Emerge stocks.

    Philosophy: GROWTH is the primary criterion (25% 3Y CAGR floor).
    Cash flow uses a COMPOSITE quality check — not a simple CFO > 0.

    Active gates:
      ROCE ≥ 15%           Capital efficiency
      ROE  ≥ 15%           Return on equity
      D/E  ≤ 1.5           Moderate leverage
      Profit Growth ≥ 25%  No stagnant earners
      Sales  Growth ≥ 25%  Fast-growing revenue
      OPM    ≥ 8%          Operational profitability (only applied when OPM is available)
      TTM    ≥ 15%         Recent order-book momentum
      Cash Quality (composite) — CCR, CF/Debt, deep negative CFO check
    """
    # Use explicit is-not-None checks to avoid the Python `or`-chain pitfall where
    # a value of 0 is treated as "missing" (0 or fallback → fallback instead of 0).
    def _first_valid(*args):
        """Return first non-None value, or None if all are None."""
        for v in args:
            if v is not None:
                return v
        return None

    roce_v  = rec.get("roce")
    roe_v   = _first_valid(rec.get("roe"), rec.get("roe_5y"), rec.get("roe_10y"))
    pg_v    = _first_valid(rec.get("profit_growth_3y"), rec.get("profit_growth_5y"))
    sg_v    = _first_valid(rec.get("sales_growth_pct"), rec.get("sales_growth_5y"))

    key_vals = [roce_v, roe_v, pg_v, sg_v]
    if sum(1 for v in key_vals if v is not None) < _SME_MIN_KEY_FIELDS:
        return False

    if roce_v is not None and float(roce_v) < _SME_ROCE_MIN:
        return False

    if roe_v is not None and float(roe_v) < _SME_ROE_MIN:
        return False

    de = rec.get("debt_equity")
    de_f = float(de) if de is not None else 0.0
    if de is not None and de_f > _SME_DE_MAX:
        return False

    if pg_v is not None and float(pg_v) < _SME_PROFIT_GROW_MIN:
        return False

    if sg_v is not None and float(sg_v) < _SME_SALES_GROW_MIN:
        return False

    # OPM gate: only apply when OPM is explicitly available.
    # net_profit_margin (NPM) is NOT a valid proxy — NPM < OPM due to interest & tax,
    # so using NPM as a fallback would incorrectly reject stocks with good operating margins
    # but high debt costs (common for growth-stage SME companies).
    opm_v = rec.get("opm")
    if opm_v is not None and float(opm_v) < _SME_OPM_MIN:
        return False

    ttm_pg = rec.get("profit_growth_ttm")
    ttm_sg = rec.get("sales_growth_ttm")
    if ttm_pg is not None and ttm_sg is not None:
        if max(float(ttm_pg), float(ttm_sg)) < _SME_TTM_GROW_MIN:
            return False

    # Composite Cash Quality gate
    cfo_val = rec.get("cash_from_operations")
    if cfo_val is not None:
        cfo_f = float(cfo_val)
        if cfo_f <= 0:
            pe_f = float(rec.get("pe_ratio") or 0)
            mc_f = float(rec.get("market_cap_cr") or 0)

            # 1. Cash Conversion Ratio — primary fraud detector
            ccr_val = rec.get("ccr")
            if ccr_val is None and pe_f > 0 and mc_f > 0:
                net_p = mc_f / pe_f
                if net_p > 0:
                    ccr_val = cfo_f / net_p
            if ccr_val is not None and float(ccr_val) < _SME_CCR_MIN:
                return False

            # 2. CF/Debt coverage for leveraged companies
            cf_debt_val = rec.get("cf_to_debt")
            if cf_debt_val is None and de_f > 0.5:
                bv_f = float(rec.get("book_value") or 0)
                cp_f = float(rec.get("current_price") or 0)
                if bv_f > 0 and cp_f > 0 and mc_f > 0:
                    total_debt_cr = de_f * bv_f * mc_f / cp_f
                    if total_debt_cr > 0:
                        cf_debt_val = cfo_f / total_debt_cr
            if (cf_debt_val is not None and float(cf_debt_val) < _SME_CF_DEBT_MIN and de_f > 0.5):
                return False

            # 3. Deep negative CFO without OPM confirmation
            opm_f = float(opm_v) if opm_v is not None else None
            if cfo_f < _SME_CFO_DEEP_NEG and (opm_f is None or opm_f < _SME_OPM_MIN):
                return False

    return True


# ---------------------------------------------------------------------------
# Growth-acceleration score (max 100 pts + bonuses)
# ---------------------------------------------------------------------------

def _sme_quality_score(rec: dict) -> float:
    """
    Growth CAGR (35) + Growth Acceleration (20) + Quality (18) +
    Cash Flow (12) + Debt (8) + Value (4) + Size (3) = 100 base pts.
    Bonuses: big acceleration (+2), div yield (+1), 10Y track record (+1).
    """
    def _first_valid(*args):
        """Return first non-None value, or None if all are None."""
        for v in args:
            if v is not None:
                return v
        return None

    score = 0.0

    # Growth CAGR block (35 pts) -------------------------------------------
    # Use _first_valid (not `or`) so that a genuine 0% growth is honoured
    # rather than falling through to the 5-year figure.
    pg3     = _first_valid(rec.get("profit_growth_3y"), rec.get("profit_growth_5y"))
    pg3_val = float(pg3) if pg3 is not None else None
    if pg3_val is not None:
        score += min(18.0, max(0.0, pg3_val * 0.225))

    sg3     = _first_valid(rec.get("sales_growth_pct"), rec.get("sales_growth_5y"))
    sg3_val = float(sg3) if sg3 is not None else None
    if sg3_val is not None:
        score += min(12.0, max(0.0, sg3_val * 0.20))

    # OPM only — do NOT fall back to net_profit_margin.
    # NPM is after interest & tax, so it is materially lower than OPM and
    # would artificially deflate the score.  Omit the 5 pts when OPM is
    # unavailable; that is preferable to an unfair comparison.
    opm = rec.get("opm")
    if opm is not None:
        score += min(5.0, max(0.0, float(opm) * 0.20))

    # Growth Acceleration block (20 pts) — TTM momentum --------------------
    ttm_pg = rec.get("profit_growth_ttm")
    ttm_sg = rec.get("sales_growth_ttm")
    ttm_pg_val = float(ttm_pg) if ttm_pg is not None else None
    ttm_sg_val = float(ttm_sg) if ttm_sg is not None else None

    if ttm_pg_val is not None:
        score += min(10.0, max(0.0, ttm_pg_val * 0.167))
    if ttm_sg_val is not None:
        score += min(7.0, max(0.0, ttm_sg_val * 0.14))

    if ttm_pg_val is not None and pg3_val is not None:
        accel = ttm_pg_val - pg3_val
        if accel >= 20:   score += 1.5
        elif accel >= 10: score += 1.0
        elif accel >= 0:  score += 0.5
    if ttm_sg_val is not None and sg3_val is not None:
        accel = ttm_sg_val - sg3_val
        if accel >= 15:   score += 1.5
        elif accel >= 7:  score += 1.0
        elif accel >= 0:  score += 0.5

    # Quality block (18 pts) -----------------------------------------------
    roce = rec.get("roce")
    if roce is not None:
        score += min(10.0, max(0.0, float(roce) * 0.25))

    # Use _first_valid so a genuine ROE of 0 is not skipped in favour of
    # a historical average — consistent with _passes_sme_gates().
    roe_ref = _first_valid(rec.get("roe"), rec.get("roe_5y"), rec.get("roe_10y"))
    if roe_ref is not None:
        score += min(5.0, max(0.0, float(roe_ref) * 0.20))

    ph = rec.get("promoter_holding")
    if ph is not None:
        ph = float(ph)
        if ph >= 70:    score += 3.0
        elif ph >= 60:  score += 2.5
        elif ph >= 50:  score += 2.0
        elif ph >= 40:  score += 1.0

    # Cash Flow block (12 pts) ---------------------------------------------
    pe_sc = float(rec.get("pe_ratio")      or 0)
    mc_sc = float(rec.get("market_cap_cr") or 0)
    de_sc = float(rec.get("debt_equity")   or 0)
    cfo_abs = rec.get("cash_from_operations")

    cfo_y = rec.get("cfo_yield")
    if cfo_y is not None:
        cfo_y_f = float(cfo_y)
        if cfo_y_f >= 10.0:   score += 5.0
        elif cfo_y_f >= 6.0:  score += 4.0
        elif cfo_y_f >= 3.0:  score += 3.0
        elif cfo_y_f >= 1.0:  score += 2.0
        elif cfo_y_f >= 0.0:  score += 1.0
        elif cfo_y_f >= -2.0: score += 0.0
        else:                 score -= 1.5

    ccr_v = rec.get("ccr")
    if ccr_v is None and pe_sc > 0 and mc_sc > 0 and cfo_abs is not None:
        net_p = mc_sc / pe_sc
        if net_p > 0:
            ccr_v = float(cfo_abs) / net_p
    if ccr_v is not None:
        ccr_f = float(ccr_v)
        if ccr_f >= 1.0:    score += 4.0
        elif ccr_f >= 0.7:  score += 3.5
        elif ccr_f >= 0.4:  score += 2.5
        elif ccr_f >= 0.15: score += 1.5
        elif ccr_f >= 0.0:  score += 0.5
        elif ccr_f >= -0.5: score += 0.0
        elif ccr_f >= -1.0: score -= 0.5

    cf_debt_v = rec.get("cf_to_debt")
    if cf_debt_v is None and de_sc > 0.1 and cfo_abs is not None:
        bv_sc = float(rec.get("book_value") or 0)
        cp_sc = float(rec.get("current_price") or 0)
        if bv_sc > 0 and cp_sc > 0 and mc_sc > 0:
            total_debt_cr = de_sc * bv_sc * mc_sc / cp_sc
            if total_debt_cr > 0:
                cf_debt_v = float(cfo_abs) / total_debt_cr
    if cf_debt_v is not None:
        cf_d_f = float(cf_debt_v)
        if cf_d_f >= 0.5:    score += 2.0
        elif cf_d_f >= 0.2:  score += 1.5
        elif cf_d_f >= 0.0:  score += 1.0

    if cfo_abs is not None:
        cfo_f_sc = float(cfo_abs)
        if cfo_f_sc >= 30:    score += 1.0
        elif cfo_f_sc >= 10:  score += 0.5
        elif cfo_f_sc >= 0:   score += 0.25

    # Debt block (8 pts) ---------------------------------------------------
    de = rec.get("debt_equity")
    if de is not None:
        de = float(de)
        if de == 0.0:       score += 5.0
        elif de <= 0.25:    score += 4.5
        elif de <= 0.50:    score += 3.5
        elif de <= 0.75:    score += 2.5
        elif de <= 1.00:    score += 1.5
        elif de <= 1.50:    score += 0.5

    cr = rec.get("current_ratio")
    if cr is not None:
        cr = float(cr)
        if cr >= 2.5:    score += 3.0
        elif cr >= 2.0:  score += 2.5
        elif cr >= 1.5:  score += 2.0
        elif cr >= 1.0:  score += 1.0

    # Value block (4 pts) --------------------------------------------------
    peg = rec.get("peg_ratio")
    if peg is not None:
        peg = float(peg)
        if peg <= 0.5:    score += 3.0
        elif peg <= 1.0:  score += 2.5
        elif peg <= 1.5:  score += 1.5
        elif peg <= 2.0:  score += 0.5

    ey = rec.get("earnings_yield")
    if ey is not None:
        score += min(1.0, max(0.0, float(ey) * 0.10))

    # Size block (3 pts) ---------------------------------------------------
    mc = float(rec.get("market_cap_cr") or 0)
    if mc >= 5_000:     score += 3.0
    elif mc >= 2_000:   score += 2.5
    elif mc >= 1_000:   score += 2.0
    elif mc >= 500:     score += 1.5
    elif mc >= 200:     score += 1.0
    else:               score += 0.5

    # Bonus points ---------------------------------------------------------
    if ttm_pg_val is not None and pg3_val is not None and (ttm_pg_val - pg3_val) >= 20:
        score += 1.0
    if ttm_sg_val is not None and sg3_val is not None and (ttm_sg_val - sg3_val) >= 15:
        score += 1.0

    dy = rec.get("dividend_yield")
    if dy is not None and float(dy) > 0:
        score += min(1.0, float(dy) * 0.5)

    pg10 = rec.get("profit_growth_10y")
    if pg10 is not None and float(pg10) >= 20:
        score += 1.0

    return round(min(100.0, max(0.0, score)), 2)


# ---------------------------------------------------------------------------
# Per-ticker fetch + derived metrics
# ---------------------------------------------------------------------------

def _sme_fund_refresh_ticker(ticker: str) -> tuple:
    """Fetch & cache fundamentals for one SME ticker.  Returns (result, content_changed)."""
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
        extra = _fc.get_extra_sme_fundamentals(ticker)
        for k in ("roce", "roe", "roe_5y", "roe_10y",
                  "promoter_holding", "fii_holding", "dii_holding",
                  "sales_growth_pct", "sales_growth_5y", "sales_growth_10y",
                  "profit_growth_3y", "profit_growth_5y", "profit_growth_10y",
                  "sales_growth_ttm", "profit_growth_ttm",
                  "pe_ratio", "book_value", "dividend_yield",
                  "debt_equity", "market_cap_cr", "current_price",
                  "current_ratio", "cash_from_operations",
                  "opm", "net_profit_margin", "sector", "industry"):
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
    de_val = result.get("debt_equity")

    growth_peg = pg3 if (pg3 and pg3 > 0) else (pg5 if pg5 and pg5 > 0 else None)
    if pe and pe > 0 and growth_peg and growth_peg > 0:
        result["peg_ratio"] = round(pe / growth_peg, 2)
    if pe and pe > 0:
        result["earnings_yield"] = round(100.0 / pe, 2)
    if cp and bv and bv > 0:
        result["pb_ratio"] = round(cp / bv, 2)

    eps = (cp / pe) if (cp and pe and pe > 0) else None
    if eps and bv and bv > 0:
        try:
            gn = math.sqrt(22.5 * abs(eps) * abs(bv))
            result["graham_number"] = round(gn, 2)
            if cp and cp > 0:
                result["graham_mos"] = round((gn - cp) / cp * 100, 1)
        except Exception:
            pass

    if mc and cfo is not None:
        mc_f = float(mc)
        if mc_f > 0:
            # cfo and mc are both in crores — same units → straight ratio × 100
            result["cfo_yield"] = round(float(cfo) / mc_f * 100, 2)

    if cfo is not None and pe and float(pe) > 0 and mc and float(mc) > 0:
        net_profit_est = float(mc) / float(pe)
        if net_profit_est > 0:
            result["ccr"] = round(float(cfo) / net_profit_est, 2)

    if (cfo is not None and de_val and float(de_val) > 0
            and bv and float(bv) > 0 and cp and float(cp) > 0 and mc and float(mc) > 0):
        try:
            total_debt_cr = float(de_val) * float(bv) * float(mc) / float(cp)
            if total_debt_cr > 0:
                result["cf_to_debt"] = round(float(cfo) / total_debt_cr, 2)
        except Exception:
            pass

    sg3  = result.get("sales_growth_pct")
    sg10 = result.get("sales_growth_10y")
    if sg3 is not None and sg10 is not None:
        result["sales_growth_avg"] = round((sg3 + sg10) / 2, 1)
    elif sg3 is not None:
        result["sales_growth_avg"] = sg3

    fii = result.get("fii_holding")
    dii = result.get("dii_holding")
    if fii is not None and dii is not None:
        result["inst_holding"] = round(fii + dii, 2)

    # Download OHLCV for ATR-based stop loss (so the Stop Loss column is populated).
    # SME tickers are stored as plain symbols (e.g. "AAKAAR"); we need the exchange
    # suffix (".NS" for NSE Emerge, ".BO" for BSE SME) for yfinance / cache look-up.
    try:
        exchange    = ss._sme_universe.get(ticker, "NSE Emerge")
        suffix      = ".NS" if exchange == "NSE Emerge" else ".BO"
        ticker_yf   = ticker + suffix
        existing_df = _ohlcv_cache.load(ticker_yf)
        if existing_df is None or len(existing_df) < 20 or not _ohlcv_cache.is_fresh(existing_df):
            import yfinance as _yf  # deferred — only needed here
            raw = _yf.download(
                ticker_yf, period="6mo",
                auto_adjust=True, progress=False, threads=False,
            )
            # yfinance ≥ 0.2 may return a MultiIndex; flatten it
            if isinstance(raw.columns, _pd.MultiIndex):
                raw.columns = raw.columns.droplevel(1)
            if raw is not None and not raw.empty and len(raw) >= 20:
                _ohlcv_cache.save(ticker_yf, raw)
    except Exception as _exc:
        logger.debug("SME OHLCV download failed for %s: %s", ticker, _exc)

    # Content-change detection
    existing = ss._sme_fund_data.get(ticker, {})
    content_changed = any(existing.get(k) != v for k, v in result.items())
    if not content_changed:
        content_changed = any(k not in result for k in existing if k != "_ts")

    result["_ts"] = time.time()
    with ss._sme_fund_lock:
        ss._sme_fund_data[ticker] = result
    return result, content_changed


# ---------------------------------------------------------------------------
# Parallel background worker
# ---------------------------------------------------------------------------

def _sme_bg_worker(tickers: list, generation: int = 0) -> None:
    """Download SME fundamentals for stale tickers in parallel.

    Worker count is taken from config.FUNDAMENTALS_THREADS (default 5).
    The ScreenerClient rate-limiter (0.40 s min-gap) is the primary guard
    against Render's IP being flagged by screener.in / Cloudflare.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    try:
        from config import FUNDAMENTALS_THREADS as _cfg_threads
    except Exception:
        _cfg_threads = 5

    MAX_WORKERS = max(1, _cfg_threads)
    BATCH_SIZE  = 50
    total_refreshed = total_changed = total_skipped = 0
    now = time.time()

    stale = []
    for t in tickers:
        entry = ss._sme_fund_data.get(t, {})
        ttl   = _SME_FAIL_TTL if entry.get("_gf") else _SME_FUND_CACHE_TTL
        if now - entry.get("_ts", 0) >= ttl:
            stale.append(t)
        else:
            total_skipped += 1

    if not stale:
        logger.info("SME BG: all %d tickers fresh — nothing to download", len(tickers))
        ss._sme_bg_running = False
        return

    logger.info("SME BG: %d stale / %d total — %d workers", len(stale), len(tickers), MAX_WORKERS)

    def _worker(t: str):
        result, changed = _sme_fund_refresh_ticker(t)
        return t, result, changed

    for batch_start in range(0, len(stale), BATCH_SIZE):
        if ss._sme_cancel.is_set() or ss._sme_generation != generation:
            logger.info("SME BG (gen %d): stopping after %d/%d tickers",
                        generation, total_refreshed, len(stale))
            _sme_cache_save()
            ss._sme_bg_running = False
            return

        chunk = stale[batch_start: batch_start + BATCH_SIZE]
        fetched_in_batch = changed_in_batch = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(_worker, t): t for t in chunk}
            for future in as_completed(futures):
                if ss._sme_cancel.is_set() or ss._sme_generation != generation:
                    for f in futures:
                        f.cancel()
                    break
                t = futures[future]
                try:
                    _, result, changed = future.result()
                    key_coverage = sum(
                        1 for v in [
                            result.get("roce"),
                            next((result.get(k) for k in ("roe", "roe_5y", "roe_10y") if result.get(k) is not None), None),
                            next((result.get(k) for k in ("profit_growth_3y", "profit_growth_5y") if result.get(k) is not None), None),
                            next((result.get(k) for k in ("sales_growth_pct", "sales_growth_5y") if result.get(k) is not None), None),
                        ]
                        if v is not None
                    )
                    with ss._sme_fund_lock:
                        entry = ss._sme_fund_data.get(t, {})
                        if key_coverage >= 3 and not _passes_sme_gates(result):
                            entry["_gf"] = True
                        else:
                            entry.pop("_gf", None)
                    if any(k in result for k in ("roce", "roe", "promoter_holding")):
                        fetched_in_batch += 1
                    if changed:
                        changed_in_batch += 1
                except Exception as exc:
                    logger.debug("SME BG refresh error %s: %s", t, exc)

        total_refreshed += fetched_in_batch
        total_changed   += changed_in_batch
        end = min(batch_start + BATCH_SIZE, len(stale))
        if changed_in_batch:
            _sme_cache_save()
            logger.info("SME BG: %d/%d — %d fetched, %d changed → saved",
                        end, len(stale), fetched_in_batch, changed_in_batch)
        else:
            logger.info("SME BG: %d/%d — %d fetched, no content changes",
                        end, len(stale), fetched_in_batch)

    _sme_cache_save()
    ss._sme_last_completed_ts = time.time()
    ss._sme_last_download_date = str(_sme_ist_today())   # mark today's IST date as downloaded
    _sme_cache_save()   # re-save to persist the updated download date
    logger.info("SME BG done (gen %d): %d fetched, %d changed, %d skipped",
                generation, total_refreshed, total_changed, total_skipped)
    ss._sme_bg_running = False


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/api/sme/fundamentals")
async def get_sme_fundamentals(refresh: int = 0) -> JSONResponse:
    """
    Top SME stocks ranked by high-growth fundamental quality.
    Universe: NSE Emerge + BSE SME.
    Gates: ROCE≥15%, ROE≥15%, D/E≤1.5, ProfitGrowth3Y≥25%, SalesGrowth≥25%.
    """
    if not ss._sme_universe:
        from sme_tickers import build_sme_universe
        ss._sme_universe = build_sme_universe()

    all_tickers = list(ss._sme_universe.keys())
    now = time.time()

    if refresh:
        for t in all_tickers:
            if t in ss._sme_fund_data:
                ss._sme_fund_data[t]["_ts"] = 0.0
        ss._sme_result_cache_valid = False
        ss._sme_last_download_date = ""   # reset date guard so re-download is allowed

    # Date-based daily guard: if we already completed a full download today (IST),
    # treat all existing tickers as fresh — only pick up truly new tickers (never fetched).
    # This prevents repeated re-downloads when the user switches browser tabs or app tabs.
    today_ist = str(_sme_ist_today())
    already_downloaded_today = (ss._sme_last_download_date == today_ist) and not refresh

    if already_downloaded_today:
        # Only include tickers that have NEVER been fetched (no _ts at all)
        stale = [t for t in all_tickers if not ss._sme_fund_data.get(t, {}).get("_ts", 0)]
    else:
        stale = [
            t for t in all_tickers
            if now - ss._sme_fund_data.get(t, {}).get("_ts", 0)
               > (_SME_FAIL_TTL if ss._sme_fund_data.get(t, {}).get("_gf") else _SME_FUND_CACHE_TTL)
        ]
    if not stale and not ss._sme_bg_running and ss._sme_last_completed_ts == 0:
        ss._sme_last_completed_ts = time.time()

    if stale and not ss._sme_bg_running:
        ss._sme_bg_running = True
        ss._sme_cancel.clear()
        # Prioritise: unknown/previously-passed tickers first, known-fail (_gf) last.
        # This mirrors the same pattern used in fundamentals.py so that promising stocks
        # get their data refreshed in the first batch rather than being buried behind
        # the large volume of gate-failing SME tickers.
        priority_first = sorted(
            stale,
            key=lambda t: 1 if ss._sme_fund_data.get(t, {}).get("_gf") else 0,
        )
        gen = ss._sme_generation

        async def _sme_bg_task(tickers=priority_first, g=gen):
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, _sme_bg_worker, tickers, g)
            except Exception:
                ss._sme_bg_running = False

        asyncio.create_task(_sme_bg_task())

    cache_fresh = len(all_tickers) - len(stale)
    nse_count = sum(1 for v in ss._sme_universe.values() if v == "NSE Emerge")
    bse_count = sum(1 for v in ss._sme_universe.values() if v == "BSE SME")

    # Return cached result if still valid
    if not refresh and ss._sme_result_cache_valid and ss._sme_result_cache_body is not None:
        live_body = dict(ss._sme_result_cache_body)
        live_body["bg_running"]        = ss._sme_bg_running
        live_body["stale_count"]       = len(stale)
        live_body["cache_fresh"]       = cache_fresh
        live_body["last_completed_ts"] = ss._sme_last_completed_ts
        live_body["cache_date"]        = ss._sme_last_download_date
        return JSONResponse(live_body)

    combined = []
    for ticker in all_tickers:
        key       = ticker.upper()
        cache_rec = ss._sme_fund_data.get(key) or ss._sme_fund_data.get(ticker) or {}
        if not cache_rec:
            continue

        rec: dict = {
            "ticker":         ticker,
            "display_ticker": key.replace(".NS", "").replace(".BO", ""),
            "exchange":       ss._sme_universe.get(ticker, "NSE Emerge"),
        }
        for k in ("sector", "industry", "debt_equity", "market_cap_cr", "current_price",
                  "roce", "roe", "roe_5y", "roe_10y",
                  "promoter_holding", "fii_holding", "dii_holding", "inst_holding",
                  "sales_growth_pct", "sales_growth_5y", "sales_growth_10y",
                  "profit_growth_3y", "profit_growth_5y", "profit_growth_10y",
                  "sales_growth_ttm", "profit_growth_ttm",
                  "pe_ratio", "peg_ratio", "earnings_yield",
                  "book_value", "pb_ratio", "dividend_yield",
                  "graham_number", "graham_mos", "sales_growth_avg",
                  "current_ratio", "cash_from_operations", "cfo_yield",
                  "opm", "net_profit_margin"):
            v = cache_rec.get(k)
            if v is not None:
                rec[k] = v

        sym_plain = rec["display_ticker"]
        _sm = ss._SECTOR_MAP.get(sym_plain, {})
        if not rec.get("sector")   and _sm.get("sector"):
            rec["sector"]   = _sm["sector"]
        if not rec.get("industry") and _sm.get("industry"):
            rec["industry"] = _sm["industry"]

        if "current_price" not in rec and rec.get("price"):
            rec["current_price"] = rec["price"]

        if not _passes_sme_gates(rec):
            continue

        cp_sme = rec.get("current_price") or rec.get("price")
        if cp_sme:
            # Use the exchange-suffixed ticker so the OHLCV cache can be found
            # (cache files use TICKER_NS.pkl / TICKER_BO.pkl naming).
            _exchange  = ss._sme_universe.get(ticker, "NSE Emerge")
            _suffix    = ".NS" if _exchange == "NSE Emerge" else ".BO"
            _ticker_yf = ticker + _suffix
            sl_sme, atr_sme = _compute_stop_loss_from_cache(_ticker_yf, float(cp_sme))
            if sl_sme is None:
                # Fallback: 5% below current price (ATR data not yet cached).
                sl_sme = round(float(cp_sme) * 0.95, 2)
            rec["stop_loss"] = sl_sme
            if atr_sme is not None:
                rec["atr14"] = atr_sme

        rec["fund_score"] = _sme_quality_score(rec)
        combined.append(rec)

    combined.sort(key=lambda x: x["fund_score"], reverse=True)
    top30 = combined[:30]
    for i, s in enumerate(top30, 1):
        s["sme_rank"] = i

    response_body = {
        "stocks":             top30,
        "all_stocks":         combined,
        "total":              len(combined),
        "status":             "complete" if combined else "no_data",
        "cache_fresh":        cache_fresh,
        "cache_total":        len(all_tickers),
        "bg_running":         ss._sme_bg_running,
        "stale_count":        len(stale),
        "last_completed_ts":  ss._sme_last_completed_ts,
        "cache_date":         ss._sme_last_download_date,   # IST date of last full download
        "nse_count":          nse_count,
        "bse_count":          bse_count,
        "gates": {
            "roce_min":        _SME_ROCE_MIN,
            "roe_min":         _SME_ROE_MIN,
            "de_max":          _SME_DE_MAX,
            "profit_grow_min": _SME_PROFIT_GROW_MIN,
            "sales_grow_min":  _SME_SALES_GROW_MIN,
            "opm_min":         _SME_OPM_MIN,
            "ttm_grow_min":    _SME_TTM_GROW_MIN,
            "ccr_min":         _SME_CCR_MIN,
            "cf_debt_min":     _SME_CF_DEBT_MIN,
            "cfo_deep_neg_cr": _SME_CFO_DEEP_NEG,
        },
    }
    ss._sme_result_cache_body  = response_body
    ss._sme_result_cache_valid = True
    return JSONResponse(response_body)


@router.post("/api/sme/fundamentals/clear-cache")
async def sme_fundamentals_cache_clear() -> JSONResponse:
    """Force a full re-download of all SME fundamentals data on the next request."""
    ss._sme_generation += 1
    with ss._sme_fund_lock:
        deleted_count = len(ss._sme_fund_data)
        ss._sme_fund_data.clear()
    disk_deleted = False
    try:
        if _SME_FUND_CACHE_FILE.exists():
            _SME_FUND_CACHE_FILE.unlink()
            disk_deleted = True
    except Exception as exc:
        logger.warning("Could not delete SME fundamentals cache file: %s", exc)
    ss._sme_result_cache_valid = False
    ss._sme_result_cache_body  = None
    ss._sme_bg_running         = False
    ss._sme_last_download_date = ""   # reset date guard so next visit re-downloads

    screener_cleared = 0
    try:
        from data_sources import ScreenerClient as _SC
        screener_cleared = len(_SC._cache)
        _SC._cache.clear()
        _SC._cache_ts.clear()
    except Exception as exc:
        logger.warning("Could not clear ScreenerClient cache (SME): %s", exc)

    logger.info("SME fund cache CLEARED: %d entries, %d Screener cache entries, disk %s",
                deleted_count, screener_cleared,
                "deleted" if disk_deleted else "delete-failed")
    return JSONResponse({
        "reset":            deleted_count,
        "screener_cleared": screener_cleared,
        "disk_deleted":     disk_deleted,
        "message": (
            f"{deleted_count} SME fundamentals entries cleared, "
            f"{screener_cleared} Screener.in HTML-cache entries cleared, "
            f"JSON file {'deleted' if disk_deleted else 'could not be deleted'} — "
            "full re-download will start on next tab visit"
        ),
    })

