"""
routes/swing_trades.py — Tab: Swing Trades
==========================================
Handles Nifty500 and Microcap250 swing trade scans.

API routes
----------
POST /api/trigger              — trigger Nifty500 scan
POST /api/rescan               — force-rescan Nifty500
GET  /api/results              — latest Nifty500 results
GET  /api/stream               — SSE stream for Nifty500
POST /api/microcap/trigger     — trigger Microcap250 scan
POST /api/microcap/rescan      — force-rescan Microcap250
GET  /api/microcap/results     — latest Microcap250 results
GET  /api/microcap/stream      — SSE stream for Microcap250
GET  /api/stock/{ticker}       — individual stock analysis
GET  /api/snapshot             — historical Nifty500 scan
GET  /api/microcap/snapshot    — historical Microcap250 scan
"""

import asyncio
import logging
import time
from datetime import datetime

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, StreamingResponse

import shared_state as ss
import cache as _ohlcv_cache
from config import SCAN_INTERVAL_MINUTES, AUTO_RESCAN
from routes.utils import (
    _validate_date_param, _make_sse_generator, _enrich_with_industry, _SSE_HEADERS,
)

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Internal scan runner
# ---------------------------------------------------------------------------

async def _do_run_generic_scan(sc, state: dict, label: str) -> None:
    """Inner scan body — must be called while _scan_lock is held."""
    def _progress(stage: str):
        state["scan_stage"] = stage

    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, lambda: sc.scan(progress_cb=_progress))
        _enrich_with_industry(results)

        now_ist = datetime.now(ss.IST).strftime("%Y-%m-%d %H:%M:%S IST")
        state.update({
            "data":           results,
            "momentum_data":  sc.last_momentum_results,
            "last_updated":   now_ist,
            "status":         "complete",
            "scan_count":     state["scan_count"] + 1,
            "filters_passed": len(results),
            "next_scan_ts":   (time.time() + SCAN_INTERVAL_MINUTES * 60) if AUTO_RESCAN else None,
            "scan_stage":     f"{len(results)} stocks qualified",
            "error":          None,
            "regime_ok":      sc.last_regime_ok,
            "regime_summary": sc.last_regime_summary,
        })
        logger.info("=== %s Scan #%d complete — %d qualifying stocks ===",
                    label, state["scan_count"], len(results))

    except Exception as exc:
        logger.error("%s scan failed: %s", label, exc, exc_info=True)
        state["status"] = "error"
        state["error"]  = str(exc)
        state["next_scan_ts"] = (time.time() + SCAN_INTERVAL_MINUTES * 60) if AUTO_RESCAN else None


# ---------------------------------------------------------------------------
# Scan task coroutines (called by scheduler, lifespan, and route handlers)
# ---------------------------------------------------------------------------

async def run_scan() -> None:
    if ss._scan_lock.locked():
        logger.info("Nifty500 scan queued — waiting for Microcap250 scan to finish...")
        ss.scan_state["scan_stage"] = "Queued (waiting for Microcap250 scan to finish)..."
    async with ss._scan_lock:
        ss.scan_state["status"]       = "scanning"
        ss.scan_state["scan_stage"]   = "Downloading OHLCV data..."
        ss.scan_state["next_scan_ts"] = None
        logger.info("=== Nifty500 scan starting ===")
        await _do_run_generic_scan(ss.scanner, ss.scan_state, "Nifty500")


async def run_mc_scan() -> None:
    ss.mc_scan_ever_triggered = True
    if ss._scan_lock.locked():
        logger.info("Microcap250 scan queued — waiting for Nifty500 scan to finish...")
        ss.mc_scan_state["scan_stage"] = "Queued (waiting for Nifty500 scan to finish)..."
        ss.mc_scan_state["status"]     = "scanning"
    async with ss._scan_lock:
        ss.mc_scan_state["status"]       = "scanning"
        ss.mc_scan_state["scan_stage"]   = "Downloading OHLCV data..."
        ss.mc_scan_state["next_scan_ts"] = None
        logger.info("=== Microcap250 scan starting ===")
        await _do_run_generic_scan(ss.scanner_mc, ss.mc_scan_state, "Microcap250")


# Scheduler wrappers (avoid double-scanning when already in progress)
async def _maybe_run_n500_scan() -> None:
    if ss.n500_tab_active and ss.scan_state["status"] not in ("scanning",):
        await run_scan()


async def _maybe_run_mc_scan() -> None:
    if ss.mc_scan_state["status"] not in ("scanning",):
        await run_mc_scan()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/api/trigger")
async def trigger_n500_scan() -> JSONResponse:
    ss.n500_tab_active = True
    if ss.scan_state["status"] not in ("scanning",):
        asyncio.create_task(run_scan())
        return JSONResponse({"triggered": True, "status": "scanning"})
    return JSONResponse({"triggered": False, "status": ss.scan_state["status"]})


@router.post("/api/rescan")
async def force_n500_rescan() -> JSONResponse:
    """Force a fresh Nifty500 scan immediately."""
    ss.n500_tab_active = True
    asyncio.create_task(run_scan())
    return JSONResponse({"triggered": True, "status": "scanning"})


@router.get("/api/results")
async def get_results() -> JSONResponse:
    return JSONResponse(ss.scan_state)


@router.get("/api/stream")
async def stream_results() -> StreamingResponse:
    """SSE stream for Nifty 500."""
    return StreamingResponse(
        _make_sse_generator(ss.scan_state)(),
        media_type="text/event-stream",
        headers=_SSE_HEADERS,
    )


@router.post("/api/microcap/trigger")
async def trigger_mc_scan() -> JSONResponse:
    ss.n500_tab_active     = True
    ss.mc_scan_ever_triggered = True
    status = ss.mc_scan_state["status"]
    if status not in ("scanning",):
        asyncio.create_task(run_mc_scan())
        return JSONResponse({"triggered": True, "status": "scanning"})
    return JSONResponse({"triggered": False, "status": status})


@router.post("/api/microcap/rescan")
async def force_mc_rescan() -> JSONResponse:
    """Force a fresh Microcap250 scan immediately."""
    ss.n500_tab_active = False
    asyncio.create_task(run_mc_scan())
    return JSONResponse({"triggered": True, "status": "scanning"})


@router.get("/api/microcap/results")
async def get_mc_results() -> JSONResponse:
    return JSONResponse(ss.mc_scan_state)


@router.get("/api/microcap/stream")
async def stream_mc_results() -> StreamingResponse:
    """SSE stream for Nifty Microcap 250."""
    return StreamingResponse(
        _make_sse_generator(ss.mc_scan_state)(),
        media_type="text/event-stream",
        headers=_SSE_HEADERS,
    )


@router.get("/api/stock/{ticker}")
async def analyze_stock(
    ticker: str,
    date: str = Query(None, description="Historical date in YYYY-MM-DD format (omit for live data)"),
) -> JSONResponse:
    """Detailed pass/fail analysis for an individual stock ticker."""
    ticker = ticker.upper().strip()
    target_date = None
    if date:
        target_date, err = _validate_date_param(date)
        if err:
            return err

    logger.info("Individual stock analysis requested: %s  date=%s", ticker, date or "live")
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, lambda: ss.scanner.analyze_single(ticker, target_date=target_date)
        )
        if target_date:
            result["as_of_date"] = date
        return JSONResponse(result)
    except Exception as exc:
        logger.error("Stock analysis failed for %s: %s", ticker, exc, exc_info=True)
        return JSONResponse({"error": str(exc), "ticker": ticker}, status_code=500)


@router.get("/api/snapshot")
async def historical_snapshot(
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
) -> JSONResponse:
    """Run a one-time Nifty500 scan AS-OF a specific historical date."""
    target, err = _validate_date_param(date)
    if err:
        return err
    logger.info("Historical snapshot (Nifty500) requested for %s", date)
    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            None, lambda: ss.scanner.scan(target_date=target)
        )
        _enrich_with_industry(results)
        return JSONResponse({
            "data": results, "date": date,
            "filters_passed": len(results),
            "total_tickers":  len(ss.scanner.tickers),
            "status":         "complete",
            "last_updated":   f"Historical data as of {date}",
        })
    except Exception as exc:
        logger.error("Historical Nifty500 scan failed: %s", exc, exc_info=True)
        return JSONResponse({"error": str(exc), "status": "error"}, status_code=500)


@router.get("/api/microcap/snapshot")
async def mc_historical_snapshot(
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
) -> JSONResponse:
    """Run a one-time Nifty Microcap 250 scan AS-OF a specific historical date."""
    target, err = _validate_date_param(date)
    if err:
        return err
    logger.info("Historical snapshot (Microcap250) requested for %s", date)
    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            None, lambda: ss.scanner_mc.scan(target_date=target)
        )
        return JSONResponse({
            "data": results, "date": date,
            "filters_passed": len(results),
            "total_tickers":  len(ss.scanner_mc.tickers),
            "status":         "complete",
            "last_updated":   f"Historical data as of {date}",
        })
    except Exception as exc:
        logger.error("Historical Microcap250 scan failed: %s", exc, exc_info=True)
        return JSONResponse({"error": str(exc), "status": "error"}, status_code=500)

