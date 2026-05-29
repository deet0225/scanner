"""
routes/misc.py — Config, Cache & Utility Endpoints
===================================================
Miscellaneous API endpoints not tied to any specific scan tab.

API routes
----------
GET  /api/config              — scan filter constants for the UI
GET  /api/cache/stats         — OHLCV cache statistics
POST /api/cache/clear         — delete all OHLCV cache files
POST /api/stop-all-scans      — signal all BG workers to stop
POST /api/tab-active          — tell the backend which UI tab is active
GET  /                        — serve the HTML dashboard
"""

import asyncio
import logging

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse, JSONResponse

import shared_state as ss
import cache as _ohlcv_cache
from config import (
    SCAN_INTERVAL_MINUTES, AUTO_RESCAN,
    MARKET_CAP_MIN, DEBT_EQUITY_MAX,
    AVG_TRADED_VALUE_20D_MIN, MEDIAN_TRADED_VALUE_20D_MIN,
    REL_VOL_PERCENTILE_MIN, VOLUME_ZSCORE_MIN, VOLUME_LOOKBACK_DAYS,
    ATR_RATIO_MAX, EMA_ATR_MULTIPLIER, REQUIRE_EMA_ATR_CEILING,
    CLOSING_RANGE_MIN, PRICE_PROXIMITY_MAX, GAP_UP_MAX,
    RSI_MIN, WEEKLY_RSI_MIN, ADX_MIN,
    MOMENTUM_OUTPERFORM_MIN, SECTOR_OUTPERFORM_MIN,
    REQUIRE_HH20_BREAKOUT, REQUIRE_ATR_CONTRACTION, REQUIRE_RSI_SMA3_RISING,
    REQUIRE_MEDIAN_TV_20D, REQUIRE_CLOSING_RANGE, REQUIRE_MEDIAN_TV_TREND,
    REQUIRE_PRICE_PROXIMITY, REQUIRE_WEEKLY_EMA, REQUIRE_RS_UPTREND,
    REQUIRE_ADX_THRESHOLD, REQUIRE_FUNDAMENTALS,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/api/config")
async def get_config() -> JSONResponse:
    """Return all scan filter constants so the UI can render them without hardcoding."""
    return JSONResponse({
        "market_cap_min_cr":           round(MARKET_CAP_MIN / 1e7),
        "debt_equity_max":             round(DEBT_EQUITY_MAX / 100, 1),
        "avg_tv_min_cr":               round(AVG_TRADED_VALUE_20D_MIN / 1e7),
        "median_tv_min_cr":            round(MEDIAN_TRADED_VALUE_20D_MIN / 1e7),
        "rel_vol_pct_min":             REL_VOL_PERCENTILE_MIN,
        "vol_zscore_min":              VOLUME_ZSCORE_MIN,
        "closing_range_min_pct":       round(CLOSING_RANGE_MIN * 100),
        "price_proximity_max_pct":     round(PRICE_PROXIMITY_MAX * 100),
        "gap_up_max_pct":              round(GAP_UP_MAX * 100),
        "ema_atr_multiplier":          EMA_ATR_MULTIPLIER,
        "rsi_min":                     RSI_MIN,
        "weekly_rsi_min":              WEEKLY_RSI_MIN,
        "adx_min":                     ADX_MIN,
        "momentum_outperform_min_pct": round(MOMENTUM_OUTPERFORM_MIN * 100, 1),
        "sector_outperform_min_pct":   round(SECTOR_OUTPERFORM_MIN * 100, 1),
        "require_hh20_breakout":       REQUIRE_HH20_BREAKOUT,
        "require_atr_contraction":     REQUIRE_ATR_CONTRACTION,
        "require_rsi_sma3_rising":     REQUIRE_RSI_SMA3_RISING,
        "require_median_tv_20d":       REQUIRE_MEDIAN_TV_20D,
        "require_closing_range":       REQUIRE_CLOSING_RANGE,
        "require_median_tv_trend":     REQUIRE_MEDIAN_TV_TREND,
        "require_price_proximity":     REQUIRE_PRICE_PROXIMITY,
        "require_weekly_ema":          REQUIRE_WEEKLY_EMA,
        "require_rs_uptrend":          REQUIRE_RS_UPTREND,
        "require_adx_threshold":       REQUIRE_ADX_THRESHOLD,
        "require_fundamentals":        REQUIRE_FUNDAMENTALS,
        "require_ema_atr_ceiling":     REQUIRE_EMA_ATR_CEILING,
        "scan_interval_minutes":       SCAN_INTERVAL_MINUTES,
        "atr_ratio_max":               ATR_RATIO_MAX,
        "volume_lookback_days":        VOLUME_LOOKBACK_DAYS,
    })


@router.get("/api/cache/stats")
async def cache_stats() -> JSONResponse:
    return JSONResponse(_ohlcv_cache.stats())


@router.post("/api/cache/clear")
async def cache_clear() -> JSONResponse:
    n = _ohlcv_cache.clear()
    logger.info("OHLCV cache cleared: %d files deleted", n)
    return JSONResponse({"deleted": n, "message": f"{n} cache files removed"})


@router.post("/api/stop-all-scans")
async def stop_all_scans() -> JSONResponse:
    """Immediately signal all running scans and background workers to stop.

    Called by the frontend before clearing caches so that in-flight workers
    don't race with the clear and re-populate the just-wiped cache.
    """
    # 1. Increment generations → BG thread-workers self-terminate
    ss._fund_generation += 1
    ss._sme_generation  += 1

    # 2. Set cancel events (checked at every batch boundary in BG workers)
    ss._fund_cancel.set()
    ss._sme_cancel.set()

    # 3. Reset running flags so a fresh worker can start after cache clear
    ss._fund_bg_running = False
    ss._sme_bg_running  = False

    # 4. Mark all price-scan states as idle
    for state in (ss.scan_state, ss.mc_scan_state,
                  ss.mom_scan_state, ss.mc_mom_scan_state,
                  ss.ms_scan_state,  ss.mc_ms_scan_state):
        if state.get("status") == "scanning":
            state["status"]     = "idle"
            state["scan_stage"] = "Stopped — awaiting fresh scan"

    logger.info("stop-all-scans: fund_gen=%d sme_gen=%d — all workers signalled",
                ss._fund_generation, ss._sme_generation)
    return JSONResponse({
        "stopped":         True,
        "fund_generation": ss._fund_generation,
        "sme_generation":  ss._sme_generation,
        "message":         "All background workers signalled to stop; price scans marked idle",
    })


@router.post("/api/tab-active")
async def set_active_tab(
    tab: str = Query(..., description="Active tab name"),
) -> JSONResponse:
    """Signal which tab is currently visible.
    Cancels any BG workers belonging to tabs other than the active one.
    """
    prev = ss._active_tab
    ss._active_tab = tab

    if tab != "fund":
        ss._fund_cancel.set()
    else:
        ss._fund_cancel.clear()

    if tab != "sme":
        ss._sme_cancel.set()
    else:
        ss._sme_cancel.clear()

    logger.debug("Tab active: %s (prev: %s)", tab, prev)
    return JSONResponse({"tab": tab, "prev": prev})


@router.get("/", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    with open("templates/index.html", "r", encoding="utf-8-sig") as fh:
        content = fh.read()
    return HTMLResponse(content=content, media_type="text/html; charset=utf-8")

