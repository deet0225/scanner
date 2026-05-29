"""
routes/sector_momentum.py — Tab: Sector Momentum
=================================================
Ranks NSE/BSE sector indices and granular industry groups by momentum score.

Two data sources:
  1. Stock-derived (primary)  — derived from SECTOR_MAP using OHLCV disk cache.
     Uses granular industry names (e.g. 'IT Services', 'Private Sector Bank').
  2. Index-based (fallback)   — uses NSE/BSE index tickers from disk cache.
     Falls back when no scan data is available yet.

API routes
----------
GET /api/sector-momentum      — ranked sector/industry momentum
GET /api/sector-stocks        — top 20 stocks for a specific sector/industry
"""

import logging
import time
from datetime import datetime

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

import shared_state as ss
import cache as _ohlcv_cache
from scanner import StockScanner

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------------------------------------------------------------------------
# NSE / BSE sector index definitions
# Each entry: (display_name, primary_yf_ticker, [etf_fallback_tickers])
# ---------------------------------------------------------------------------
_NSE_SECTOR_INDICES = [
    # Broad NSE sectors
    ("Nifty Bank",            "^NSEBANK",    ["BANKBEES.NS"]),
    ("Nifty IT",              "^CNXIT",      ["ITBEES.NS",     "ITETF.NS"]),
    ("Nifty Auto",            "^CNXAUTO",    ["AUTOBEES.NS"]),
    ("Nifty Pharma",          "^CNXPHARMA",  ["PHARMABEES.NS"]),
    ("Nifty FMCG",            "^CNXFMCG",    ["FMCGIETF.NS"]),
    ("Nifty Metal",           "^CNXMETAL",   ["METALBEES.NS"]),
    ("Nifty Realty",          "^CNXREALTY",  ["REALTYBEES.NS"]),
    ("Nifty Energy & Power",  "^CNXENERGY",  ["ENERGYBEES.NS"]),
    ("Nifty PSU Bank",        "^CNXPSUBANK", ["PSUBNKBEES.NS"]),
    ("Nifty Healthcare",      "^CNXHEALTH",  ["HEALTHIETF.NS"]),
    ("Nifty Financial Svc",   "^CNXFIN",     ["FINIETF.NS"]),
    ("Nifty Media",           "^CNXMEDIA",   []),
    ("Nifty Infra",           "^CNXINFRA",   ["INFRABEES.NS"]),
    ("Nifty Oil & Gas",       "^CNXOIL",     ["OILIETF.NS"]),
    ("Nifty Consumer Dur",    "^CNXCONSUMER",["CONSUMBEES.NS"]),
    ("Nifty Commodities",     "^CNXCMDT",    []),
    ("Nifty MNC",             "^CNXMNC",     []),
    ("Nifty PSE",             "^CNXPSE",     ["CPSE.NS"]),
    ("Nifty Services",        "^CNXSERVICE", []),
    ("Nifty Midcap 100",      "^CNXMIDCAP",  ["MID150BEES.NS", "NIFTYMID.NS"]),
    ("Nifty Smallcap 100",    "^CNXSC",      ["SMALLCAP.NS"]),
    # BSE sector indices
    ("BSE Bankex",            "^BSEBANKEX",  ["BANKBEES.NS"]),
    ("BSE IT",                "^BSEIT",      ["ITBEES.NS"]),
    ("BSE Teck",              "^BSETECK",    ["ITETF.NS"]),
    ("BSE Healthcare",        "^BSEHC",      ["HEALTHIETF.NS"]),
    ("BSE FMCG",              "^BSEFMCG",    ["FMCGIETF.NS"]),
    ("BSE Auto",              "^BSEAUTO",    ["AUTOBEES.NS"]),
    ("BSE Consumer Dur",      "^BSECD",      ["CONSUMBEES.NS"]),
    ("BSE Oil & Gas",         "^BSEOIL",     ["OILIETF.NS"]),
    # No cache yet — silently skipped until network is available
    ("Nifty Private Bank",    "^CNXPVTBANK", ["PVTBANKETF.NS"]),
    ("Nifty Capital Goods",   "^CNXCAPGOODS",["CAPGOODS.NS"]),
    ("Nifty Defence",         "^CNXDEFENCE", ["DEFENIETF.NS"]),
    ("Nifty Power",           "^CNXPOWER",   ["POWERIETF.NS", "POWERBEES.NS"]),
    ("Nifty Mfg",             "^CNXMFG",     ["MFGETF.NS"]),
]

# Short-lived in-memory cache (disk cache handles real freshness)
_sec_mom_cache: dict = {"data": None, "ts": 0.0, "ttl": 300}


# ---------------------------------------------------------------------------
# Stock-derived sector momentum (primary source)
# ---------------------------------------------------------------------------

def _compute_derived_sector_momentum(as_of_date=None) -> dict:
    """Compute sector momentum for ALL Nifty500+Microcap250 stocks in SECTOR_MAP.

    Uses granular industry names from SECTOR_MAP and OHLCV disk cache for
    5D/20D returns.  RSI/RS/EMA are supplemented from current scan state.
    """
    from collections import defaultdict

    if not ss._SECTOR_MAP:
        return {
            "sectors": [], "all_sectors": [], "total_sectors": 0,
            "bench_ret20d": 0.0, "bench_ret5d": 0.0,
            "last_updated": datetime.now(ss.IST).strftime("%Y-%m-%d %H:%M:%S IST"),
            "status": "no_data",
        }

    # Build supplemental lookup from all scan states (RSI / RS / EMA)
    scan_data: dict = {}
    for state in (ss.scan_state, ss.mc_scan_state, ss.mom_scan_state,
                  ss.mc_mom_scan_state, ss.ms_scan_state, ss.mc_ms_scan_state):
        for s in (state.get("data") or []):
            sym = s.get("display_ticker") or (s.get("ticker") or "").replace(".NS", "")
            if sym and sym not in scan_data:
                scan_data[sym] = s

    def _compute_returns(sym: str):
        ticker = sym + ".NS"
        df = _ohlcv_cache.load(ticker)
        if df is None or len(df) < 6:
            return None, None
        try:
            if as_of_date is not None:
                import pandas as _pd
                mask = _pd.to_datetime(df.index).date <= as_of_date
                df   = df[mask]
                if len(df) < 6:
                    return None, None
            closes = df["Close"].dropna()
            r5d  = round((float(closes.iloc[-1]) / float(closes.iloc[-6])  - 1) * 100, 2) if len(closes) >= 6  else None
            r20d = round((float(closes.iloc[-1]) / float(closes.iloc[-21]) - 1) * 100, 2) if len(closes) >= 21 else None
            return r5d, r20d
        except Exception:
            return None, None

    sector_stocks: dict = defaultdict(list)
    for sym, sm in ss._SECTOR_MAP.items():
        label = sm.get("industry") or sm.get("sector")
        if not label:
            continue
        r5d, r20d = _compute_returns(sym)
        if r5d is None and r20d is None:
            continue
        sc = scan_data.get(sym, {})
        sector_stocks[label].append({
            "display_ticker": sym,
            "r5d":            r5d,
            "return_20d":     r20d if r20d is not None else (sc.get("return_20d") or 0.0),
            "rsi":            sc.get("rsi"),
            "rs_outperf_pct": sc.get("rs_outperf_pct"),
            "price_vs_ema20": sc.get("price_vs_ema20"),
            "_in_scan":       sym in scan_data,
        })

    if not sector_stocks:
        return {
            "sectors": [], "all_sectors": [], "total_sectors": 0,
            "bench_ret20d": 0.0, "bench_ret5d": 0.0,
            "last_updated": datetime.now(ss.IST).strftime("%Y-%m-%d %H:%M:%S IST"),
            "status": "no_data",
        }

    results = []
    for industry, stocks in sector_stocks.items():
        n = len(stocks)
        ret20_vals = [s["return_20d"]     for s in stocks if s.get("return_20d")     is not None]
        ret5_vals  = [s["r5d"]            for s in stocks if s.get("r5d")            is not None]
        rsi_vals   = [s["rsi"]            for s in stocks if s.get("rsi")             is not None]
        rs_vals    = [s["rs_outperf_pct"] for s in stocks if s.get("rs_outperf_pct") is not None]
        ema_vals   = [s["price_vs_ema20"] for s in stocks if s.get("price_vs_ema20") is not None]

        if not ret5_vals and not ret20_vals:
            continue

        avg_ret20 = (sum(ret20_vals) / len(ret20_vals)) if ret20_vals else 0.0
        avg_ret5  = (sum(ret5_vals)  / len(ret5_vals))  if ret5_vals  else round(avg_ret20 * 0.25, 2)
        avg_rsi   = (sum(rsi_vals)   / len(rsi_vals))   if rsi_vals   else 50.0
        avg_rs    = (sum(rs_vals)    / len(rs_vals))     if rs_vals    else 0.0
        avg_ema   = (sum(ema_vals)   / len(ema_vals))    if ema_vals   else 0.0
        above_ema = (sum(1 for v in ema_vals if v >= 0) > len(ema_vals) / 2) if ema_vals else False

        # Composite score: 5D return (40%) + RS (35%) + RSI (15%) + EMA (10%)
        rsi_norm = max(0.0, min(1.0, (avg_rsi - 30) / 50))
        ema_norm = max(-1.0, min(1.0, avg_ema / 10))
        score = round(
            avg_ret5  * 0.40 + avg_rs  * 0.35 +
            rsi_norm  * 3.0  * 0.15  + ema_norm * 3.0 * 0.10, 3,
        )

        top3     = sorted(stocks, key=lambda x: x.get("r5d") or 0, reverse=True)[:3]
        top_syms = ", ".join(s.get("display_ticker", "") for s in top3)

        results.append({
            "sector":          industry,
            "ticker":          top_syms,
            "ret_3d":          round(avg_ret5 * 0.60, 2),
            "ret_5d":          round(avg_ret5,  2),
            "ret_20d":         round(avg_ret20, 2),
            "rsi":             round(avg_rsi,   1),
            "rsi_9":           round(avg_rsi,   1),
            "above_ema":       above_ema,
            "above_ema_slow":  avg_ema >= 0,
            "pct_above_ema":   round(avg_ema, 2),
            "pct_above_ema20": round(avg_ema, 2),
            "rs_vs_market":    round(avg_rs, 2),
            "rs_5d":           round(avg_rs, 2),
            "macd_hist_pct":   0.0,
            "score":           score,
            "stock_count":     n,
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return {
        "sectors":       results[:5],
        "all_sectors":   results,
        "total_sectors": len(results),
        "bench_ret20d":  0.0,
        "bench_ret5d":   0.0,
        "last_updated":  datetime.now(ss.IST).strftime("%Y-%m-%d %H:%M:%S IST"),
        "as_of_date":    str(as_of_date) if as_of_date else None,
        "status":        "complete" if results else "no_data",
    }


# ---------------------------------------------------------------------------
# Index-based sector momentum (fallback when no scan data)
# ---------------------------------------------------------------------------

def _compute_sector_momentum(as_of_date=None) -> dict:
    """Compute momentum metrics for NSE/BSE sector indices from disk cache.
    Fallback used when stock-derived data is unavailable.
    """
    import time as _time
    import pandas as _pd
    from config import (MARKET_BENCHMARK_TICKER, MARKET_BENCHMARK_ETF_FALLBACKS,
                        CACHE_UPDATE_DAYS)

    FULL_DAYS = 100

    def _cached_fetch(name: str, primary: str, fallbacks: list):
        cached = _ohlcv_cache.load(primary)
        if _ohlcv_cache.is_fresh(cached):
            return cached
        if cached is not None:
            df_new = ss.scanner._fetch_index(primary, days=CACHE_UPDATE_DAYS, etf_fallbacks=fallbacks)
            if df_new is not None and not df_new.empty:
                merged = _ohlcv_cache.merge(cached, df_new, max_rows=FULL_DAYS + 60)
                _ohlcv_cache.save(primary, merged)
                return merged
            return cached
        df = ss.scanner._fetch_index(primary, days=FULL_DAYS, etf_fallbacks=fallbacks)
        if df is not None and not df.empty:
            _ohlcv_cache.save(primary, df)
        return df

    bench_df = _cached_fetch("Nifty500 Benchmark",
                             MARKET_BENCHMARK_TICKER, MARKET_BENCHMARK_ETF_FALLBACKS)

    if as_of_date is not None:
        def _slice_to_date(df):
            if df is None:
                return None
            mask   = _pd.to_datetime(df.index).date <= as_of_date
            sliced = df[mask]
            return sliced if not sliced.empty else None
        bench_df = _slice_to_date(bench_df)

    bench_ret5 = bench_ret20 = 0.0
    if bench_df is not None:
        bc = bench_df["Close"].dropna()
        if len(bc) >= 21:
            bench_ret20 = float((bc.iloc[-1] / bc.iloc[-21] - 1) * 100)
        if len(bc) >= 6:
            bench_ret5  = float((bc.iloc[-1] / bc.iloc[-6]  - 1) * 100)

    results = []
    for name, primary, fallbacks in _NSE_SECTOR_INDICES:
        try:
            df = _cached_fetch(name, primary, fallbacks)
            if as_of_date is not None:
                df = _slice_to_date(df)
            if df is None or len(df) < 25:
                continue
            c = df["Close"].dropna()
            if len(c) < 25:
                continue

            ret_3d  = round(float((c.iloc[-1] / c.iloc[-4]  - 1) * 100), 2) if len(c) >= 4  else 0.0
            ret_5d  = round(float((c.iloc[-1] / c.iloc[-6]  - 1) * 100), 2) if len(c) >= 6  else 0.0
            ret_20d = round(float((c.iloc[-1] / c.iloc[-21] - 1) * 100), 2) if len(c) >= 21 else 0.0
            ret_50d = round(float((c.iloc[-1] / c.iloc[-51] - 1) * 100), 2) if len(c) >= 51 else 0.0

            rsi_14 = round(float(StockScanner._rsi(c, 14).iloc[-1]), 1)
            rsi_9  = round(float(StockScanner._rsi(c, 9).iloc[-1]),  1)

            ema9  = float(c.ewm(span=9,  adjust=False, min_periods=9).mean().iloc[-1])
            ema20 = float(c.ewm(span=20, adjust=False, min_periods=20).mean().iloc[-1])
            ema50 = float(c.ewm(span=50, adjust=False, min_periods=50).mean().iloc[-1])
            pct_above_ema9  = round((float(c.iloc[-1]) / ema9  - 1) * 100, 2) if ema9  > 0 else 0.0
            pct_above_ema20 = round((float(c.iloc[-1]) / ema20 - 1) * 100, 2) if ema20 > 0 else 0.0

            rs_5d  = round(ret_5d  - bench_ret5,  2)
            rs_20d = round(ret_20d - bench_ret20, 2)

            _ema12 = c.ewm(span=12, adjust=False, min_periods=12).mean()
            _ema26 = c.ewm(span=26, adjust=False, min_periods=26).mean()
            _macd  = _ema12 - _ema26
            _sig   = _macd.ewm(span=9, adjust=False, min_periods=9).mean()
            _price = float(c.iloc[-1])
            macd_hist_pct = round(float((_macd - _sig).iloc[-1]) / _price * 100, 4) if _price > 0 else 0.0

            rsi_swing = max(0.0, 2.0 - abs(rsi_9 - 65) * 0.10)
            score = round(
                ret_5d * 0.30 + rs_5d * 0.25 + ret_3d * 0.20 +
                rsi_swing * 0.50 + pct_above_ema9 * 0.08 + macd_hist_pct * 3.50,
                3,
            )

            results.append({
                "sector":          name,
                "ticker":          primary,
                "price":           round(_price, 2),
                "last_date":       str(c.index[-1].date()),
                "ret_3d":          ret_3d,  "ret_5d":  ret_5d,
                "ret_20d":         ret_20d, "ret_50d": ret_50d,
                "rsi":             rsi_14,  "rsi_9":   rsi_9,
                "above_ema":       bool(ema9 > ema20),
                "above_ema_slow":  bool(ema20 > ema50),
                "pct_above_ema":   pct_above_ema9,
                "pct_above_ema20": pct_above_ema20,
                "rs_vs_market":    rs_20d,  "rs_5d":  rs_5d,
                "macd_hist_pct":   macd_hist_pct,
                "score":           score,
            })
            _time.sleep(0.05)
        except Exception as exc:
            logger.debug("Sector momentum %s (%s): %s", name, primary, exc)

    results.sort(key=lambda x: x["score"], reverse=True)
    return {
        "sectors":       results[:5],
        "all_sectors":   results,
        "total_sectors": len(results),
        "bench_ret20d":  round(bench_ret20, 2),
        "bench_ret5d":   round(bench_ret5,  2),
        "last_updated":  (str(as_of_date) + " (historical)") if as_of_date
                         else datetime.now(ss.IST).strftime("%Y-%m-%d %H:%M:%S IST"),
        "as_of_date":    str(as_of_date) if as_of_date else None,
        "status":        "complete" if results else "no_data",
    }


# ---------------------------------------------------------------------------
# Shared response helper
# ---------------------------------------------------------------------------

async def _sector_momentum_response(bust_cache: bool) -> JSONResponse:
    """Build sector momentum response: stock-derived primary, index-based fallback."""
    import asyncio
    global _sec_mom_cache

    if bust_cache:
        _sec_mom_cache["ts"] = 0.0

    if (_sec_mom_cache["data"] is not None and
            time.time() - _sec_mom_cache["ts"] < _sec_mom_cache["ttl"]):
        return JSONResponse(_sec_mom_cache["data"])

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, _compute_derived_sector_momentum)
        if not result.get("all_sectors"):
            result = await loop.run_in_executor(None, _compute_sector_momentum)
        _sec_mom_cache["data"] = result
        _sec_mom_cache["ts"]   = time.time()
        return JSONResponse(result)
    except Exception as exc:
        logger.error("Sector momentum failed: %s", exc, exc_info=True)
        return JSONResponse(
            {"error": str(exc), "sectors": [], "all_sectors": [],
             "total_sectors": 0, "status": "error"},
            status_code=500,
        )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/api/sector-momentum")
async def get_sector_momentum(
    refresh: int = 0,
    date: str = Query(None, description="Historical date YYYY-MM-DD; omit for live data"),
) -> JSONResponse:
    """NSE sector index momentum rankings.
    Pass ?refresh=1 to bust the in-memory cache.
    Pass ?date=YYYY-MM-DD for historical results.
    """
    import asyncio
    from routes.utils import _validate_date_param

    if date:
        target, err = _validate_date_param(date)
        if err:
            return err
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, lambda: _compute_derived_sector_momentum(as_of_date=target)
            )
            if not result.get("all_sectors"):
                result = await loop.run_in_executor(
                    None, lambda: _compute_sector_momentum(as_of_date=target)
                )
            return JSONResponse(result)
        except Exception as exc:
            logger.error("Historical sector momentum failed: %s", exc, exc_info=True)
            return JSONResponse(
                {"error": str(exc), "sectors": [], "all_sectors": [],
                 "total_sectors": 0, "status": "error"},
                status_code=500,
            )
    return await _sector_momentum_response(bust_cache=bool(refresh))


@router.get("/api/sector-stocks")
async def get_sector_stocks(sector: str = "") -> JSONResponse:
    """Return top 20 stocks for the given industry/sector label,
    ranked by fundamental quality score.  Falls back to 20D price return.
    Automatically triggers a background fundamentals fetch for any sector
    stocks that are missing cached data.
    """
    import asyncio as _asyncio

    if not sector:
        return JSONResponse({"error": "sector param required", "stocks": []}, status_code=400)

    tickers_in_sector = [
        sym for sym, sm in ss._SECTOR_MAP.items()
        if (sm.get("industry") or sm.get("sector") or "").lower() == sector.strip().lower()
    ]
    if not tickers_in_sector:
        return JSONResponse({"sector": sector, "stocks": [], "total": 0, "has_fundamentals": False})

    scan_lookup: dict = {}
    for state in (ss.scan_state, ss.mc_scan_state, ss.mom_scan_state, ss.mc_mom_scan_state,
                  ss.ms_scan_state, ss.mc_ms_scan_state):
        for s in (state.get("data") or []):
            sym = s.get("display_ticker") or (s.get("ticker") or "").replace(".NS", "")
            if sym and sym not in scan_lookup:
                scan_lookup[sym] = s

    def _ohlcv_returns(sym: str):
        """Return (r5d, r20d, current_price, rsi14) from the OHLCV disk cache."""
        df = _ohlcv_cache.load(sym + ".NS")
        if df is None or len(df) < 6:
            return None, None, None, None
        try:
            closes = df["Close"].dropna()
            r5  = round((float(closes.iloc[-1]) / float(closes.iloc[-6])  - 1) * 100, 2) if len(closes) >= 6  else None
            r20 = round((float(closes.iloc[-1]) / float(closes.iloc[-21]) - 1) * 100, 2) if len(closes) >= 21 else None
            cp  = round(float(closes.iloc[-1]), 2) if len(closes) >= 1 else None
            rsi = None
            if len(closes) >= 15:
                try:
                    rsi = round(float(StockScanner._rsi(closes, 14).iloc[-1]), 1)
                except Exception:
                    pass
            return r5, r20, cp, rsi
        except Exception:
            return None, None, None, None

    # ------------------------------------------------------------------
    # Identify sector tickers that are missing fundamentals data and
    # fetch them NOW (parallel, with timeout) so the popup always has
    # complete data on the first response.
    # ------------------------------------------------------------------
    def _get_fd(sym: str) -> dict:
        """Return fundamentals dict for sym (tries both bare and .NS key)."""
        return ss._fund_data.get(sym) or ss._fund_data.get(sym + ".NS") or {}

    # A ticker needs a fetch if it has no actual fundamental fields.
    # This catches both completely absent keys AND placeholder {"_ts": 0} entries.
    _FUND_KEY_FIELDS = ("roce", "roe", "roe_5y", "debt_equity",
                        "pe_ratio", "sales_growth_pct", "sales_growth_ttm")

    def _fund_is_complete(sym: str) -> bool:
        fd = _get_fd(sym)
        return any(fd.get(k) is not None for k in _FUND_KEY_FIELDS)

    missing_fund_tickers = [
        sym + ".NS" for sym in tickers_in_sector
        if not _fund_is_complete(sym)
    ]

    fund_loading = False
    if missing_fund_tickers:
        # Fetch fundamental data for missing tickers RIGHT NOW using a
        # thread pool so the response already contains complete data.
        # Cap at 20 tickers / 12 parallel workers to avoid long waits.
        try:
            from routes.fundamentals import _fund_refresh_ticker, _fund_cache_save
            from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed

            fetch_targets = missing_fund_tickers[:20]
            logger.info("Sector stocks: fetching fundamentals for %d missing tickers in '%s'",
                        len(fetch_targets), sector)

            with ThreadPoolExecutor(max_workers=12) as _pool:
                futures = {_pool.submit(_fund_refresh_ticker, t): t for t in fetch_targets}
                for fut in _as_completed(futures, timeout=15):
                    try:
                        fut.result()
                    except Exception:
                        pass

            _fund_cache_save()

            # Re-check how many are still missing after the fetch
            still_missing = [t for t in missing_fund_tickers if not _fund_is_complete(t.replace(".NS", ""))]
            fund_loading = bool(still_missing)
        except Exception as exc:
            logger.debug("Sector fund fetch error: %s", exc)
            fund_loading = True  # signal UI to retry

    has_fund = False
    records  = []
    for sym in tickers_in_sector:
        fd   = _get_fd(sym)
        sc   = scan_lookup.get(sym, {})
        r5, r20, ohlcv_price, ohlcv_rsi = _ohlcv_returns(sym)
        if r5 is None and r20 is None:
            continue

        roce = fd.get("roce")
        roe  = fd.get("roe_10y") or fd.get("roe_5y") or fd.get("roe")
        sg   = fd.get("sales_growth_pct") or fd.get("sales_growth_ttm")
        de   = fd.get("debt_equity")
        pe   = fd.get("pe_ratio")

        f_score   = 0.0
        fund_fields = 0
        if roce is not None:
            f_score += min(40.0, max(0.0, float(roce) * 0.80)); fund_fields += 1
        if roe is not None:
            f_score += min(30.0, max(0.0, float(roe)  * 0.60)); fund_fields += 1
        if sg is not None:
            f_score += min(20.0, max(0.0, float(sg)   * 0.40)); fund_fields += 1
        if de is not None:
            de_f = float(de)
            f_score += 10.0 if de_f == 0 else max(0.0, 10.0 - de_f * 6.0)
            fund_fields += 1

        if fund_fields > 0:
            has_fund = True
        else:
            f_score = (r20 or 0.0)

        # Prefer fundamentals price; fall back to last OHLCV close
        price = fd.get("current_price") or fd.get("price") or ohlcv_price
        # Prefer scan-state RSI (intraday); fall back to OHLCV-computed RSI
        rsi   = sc.get("rsi") or ohlcv_rsi

        records.append({
            "symbol":           sym,
            "name":             fd.get("company_name") or fd.get("name") or sym,
            "current_price":    price,
            "ret_5d":           r5,
            "ret_20d":          r20 if r20 is not None else sc.get("return_20d"),
            "rsi":              rsi,
            "rs_pct":           sc.get("rs_outperf_pct"),
            "roce":             roce,
            "roe":              roe,
            "pe_ratio":         pe,
            "sales_growth":     sg,
            "debt_equity":      de,
            "opm":              fd.get("opm"),
            "promoter":         fd.get("promoter_holding"),
            "market_cap_cr":    fd.get("market_cap_cr"),
            "fund_score":       round(f_score, 2),
            "has_fundamentals": fund_fields > 0,
        })

    records.sort(key=lambda x: x["fund_score"], reverse=True)
    return JSONResponse({
        "sector":           sector,
        "stocks":           records[:20],
        "total":            len(records),
        "has_fundamentals": has_fund,
        "fund_loading":     fund_loading,
    })

