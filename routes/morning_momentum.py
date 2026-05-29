"""
routes/morning_momentum.py — Tab: Morning Momentum
===================================================
Scans ALL 750 tickers (N500 + MC250) for the 3-candle Morning Star
bullish-reversal candlestick pattern.  No swing/momentum criteria required.

API routes
----------
GET  /api/morning-momentum          — latest Morning Star results (live or historical)
POST /api/morning-momentum/rescan   — force fresh pattern scans
"""

import asyncio
import logging
import time
from datetime import datetime

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

import shared_state as ss
from config import SCAN_INTERVAL_MINUTES, AUTO_RESCAN
from routes.utils import _validate_date_param, _enrich_with_industry, _ist_today

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Internal scan runner
# ---------------------------------------------------------------------------

async def _do_run_ms_scan(sc, state: dict, label: str) -> None:
    """Run scanner.scan_morning_star() — must be called while _ms_scan_lock is held."""
    def _progress(stage: str):
        state["scan_stage"] = stage

    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            None, lambda: sc.scan_morning_star(progress_cb=_progress)
        )
        now_ist = datetime.now(ss.IST).strftime("%Y-%m-%d %H:%M:%S IST")
        state.update({
            "data":           results,
            "last_updated":   now_ist,
            "status":         "complete",
            "scan_count":     state["scan_count"] + 1,
            "filters_passed": len(results),
            "next_scan_ts":   (time.time() + SCAN_INTERVAL_MINUTES * 60) if AUTO_RESCAN else None,
            "scan_stage":     f"{len(results)} Morning Star stocks",
            "error":          None,
        })
        logger.info("=== %s Morning Star Scan #%d complete — %d qualifying stocks ===",
                    label, state["scan_count"], len(results))

    except Exception as exc:
        logger.error("%s morning-star scan failed: %s", label, exc, exc_info=True)
        state["status"] = "error"
        state["error"]  = str(exc)
        state["next_scan_ts"] = (time.time() + SCAN_INTERVAL_MINUTES * 60) if AUTO_RESCAN else None


# ---------------------------------------------------------------------------
# Scan task coroutines
# ---------------------------------------------------------------------------

async def run_n500_ms_scan() -> None:
    """Run a Morning Star pattern scan for the Nifty 500 universe."""
    ss.ms_scan_state["status"] = "scanning"
    if ss._ms_scan_lock.locked():
        logger.info("N500 Morning Star scan queued — waiting for MC250...")
        ss.ms_scan_state["scan_stage"] = "Queued (waiting for MC250 morning-star scan)..."
    async with ss._ms_scan_lock:
        ss.ms_scan_state["scan_stage"]   = "Loading OHLCV data..."
        ss.ms_scan_state["next_scan_ts"] = None
        logger.info("=== Nifty500 Morning Star scan starting ===")
        await _do_run_ms_scan(ss.scanner, ss.ms_scan_state, "Nifty500-MS")


async def run_mc250_ms_scan() -> None:
    """Run a Morning Star pattern scan for the Microcap 250 universe."""
    ss.mc_ms_scan_state["status"] = "scanning"
    if ss._ms_scan_lock.locked():
        logger.info("MC250 Morning Star scan queued — waiting for N500...")
        ss.mc_ms_scan_state["scan_stage"] = "Queued (waiting for N500 morning-star scan)..."
    async with ss._ms_scan_lock:
        ss.mc_ms_scan_state["scan_stage"]   = "Loading OHLCV data..."
        ss.mc_ms_scan_state["next_scan_ts"] = None
        logger.info("=== Microcap250 Morning Star scan starting ===")
        await _do_run_ms_scan(ss.scanner_mc, ss.mc_ms_scan_state, "Microcap250-MS")


# Scheduler wrappers
async def _maybe_run_n500_ms_scan() -> None:
    if ss.ms_scan_state["status"] not in ("scanning",):
        await run_n500_ms_scan()


async def _maybe_run_mc250_ms_scan() -> None:
    if ss.mc_ms_scan_state["status"] not in ("scanning",):
        await run_mc250_ms_scan()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/api/morning-momentum")
async def get_morning_momentum(
    date: str = Query(None, description="Historical date YYYY-MM-DD; omit for live data"),
) -> JSONResponse:
    """
    Morning Momentum — checks ALL 750 tickers for the 3-candle Morning Star
    bullish-reversal pattern.  No momentum/swing criteria required.
    """
    target_date = None
    if date:
        target_date, err = _validate_date_param(date)
        if err:
            return err

    if target_date:
        # Historical mode
        try:
            loop = asyncio.get_event_loop()
            n500_results, mc_results = await asyncio.gather(
                loop.run_in_executor(
                    None, lambda: ss.scanner.scan_morning_star(target_date=target_date)
                ),
                loop.run_in_executor(
                    None, lambda: ss.scanner_mc.scan_morning_star(target_date=target_date)
                ),
            )
        except Exception as exc:
            logger.error("Historical morning-momentum scan failed: %s", exc, exc_info=True)
            return JSONResponse({"error": str(exc), "stocks": [], "total": 0}, status_code=500)
        n500_all  = [dict(s, from_index="Nifty 500")    for s in (n500_results or [])]
        mc_all    = [dict(s, from_index="Microcap 250") for s in (mc_results   or [])]
        n500_status = mc250_status = "complete"
        last_n500 = last_mc250 = f"Historical data as of {date}"
        scan_stage = ""
    else:
        # Live mode — auto-retrigger if data is from a previous IST day
        _today_ist = _ist_today()

        def _is_prev_day(lu: "str | None") -> bool:
            if not lu:
                return False
            try:
                from datetime import date as DateType
                return DateType.fromisoformat(lu.split()[0]) < _today_ist
            except Exception:
                return False

        if _is_prev_day(ss.ms_scan_state.get("last_updated")) \
                and ss.ms_scan_state["status"] not in ("scanning",):
            logger.info("Morning Star N500: previous day data — auto-retriggering")
            ss.ms_scan_state["status"]     = "scanning"
            ss.ms_scan_state["scan_stage"] = "Auto-refresh: previous day data — rescanning..."
            asyncio.create_task(run_n500_ms_scan())

        if _is_prev_day(ss.mc_ms_scan_state.get("last_updated")) \
                and ss.mc_ms_scan_state["status"] not in ("scanning",):
            logger.info("Morning Star MC250: previous day data — auto-retriggering")
            ss.mc_ms_scan_state["status"]     = "scanning"
            ss.mc_ms_scan_state["scan_stage"] = "Auto-refresh: previous day data — rescanning..."
            asyncio.create_task(run_mc250_ms_scan())

        n500_all  = [dict(s, from_index="Nifty 500")    for s in (ss.ms_scan_state.get("data")    or [])]
        mc_all    = [dict(s, from_index="Microcap 250") for s in (ss.mc_ms_scan_state.get("data") or [])]
        n500_status  = ss.ms_scan_state["status"]
        mc250_status = ss.mc_ms_scan_state["status"]
        last_n500    = ss.ms_scan_state.get("last_updated")
        last_mc250   = ss.mc_ms_scan_state.get("last_updated")
        scan_stage   = ss.ms_scan_state.get("scan_stage", "") or ss.mc_ms_scan_state.get("scan_stage", "")

        # Kick scans on first visit
        if not n500_all and not mc_all:
            if n500_status  not in ("scanning", "complete"):
                asyncio.create_task(run_n500_ms_scan())
            if mc250_status not in ("scanning", "complete"):
                asyncio.create_task(run_mc250_ms_scan())

    all_stocks = n500_all + mc_all
    _enrich_with_industry(all_stocks)
    all_stocks.sort(
        key=lambda x: (x.get("mom_score") or 0, x.get("return_20d") or 0), reverse=True
    )

    n500_count = sum(1 for s in all_stocks if s.get("from_index") == "Nifty 500")
    mc_count   = sum(1 for s in all_stocks if s.get("from_index") == "Microcap 250")

    overall_status = (
        "scanning" if n500_status  in ("scanning", "initializing")
                      or mc250_status in ("scanning", "initializing") else
        "complete" if (n500_status == "complete" or mc250_status == "complete") else
        "error"
    )

    logger.info("Morning Momentum: %d Morning Star stocks (N500=%d, MC250=%d)",
                len(all_stocks), n500_count, mc_count)

    return JSONResponse({
        "stocks":               all_stocks[:100],
        "total":                len(all_stocks),
        "n500_count":           n500_count,
        "mc_count":             mc_count,
        "n500_status":          n500_status,
        "mc250_status":         mc250_status,
        "last_updated_n500":    last_n500,
        "last_updated_mc250":   last_mc250,
        "status":               overall_status,
        "scan_stage":           scan_stage,
        "as_of_date":           date if target_date else None,
    })


@router.post("/api/morning-momentum/rescan")
async def force_ms_rescan() -> JSONResponse:
    """Force fresh Morning Star pattern scans for both N500 and MC250 universes."""
    triggered = 0
    if ss.ms_scan_state["status"] not in ("scanning",):
        asyncio.create_task(run_n500_ms_scan())
        triggered += 1
    if ss.mc_ms_scan_state["status"] not in ("scanning",):
        asyncio.create_task(run_mc250_ms_scan())
        triggered += 1
    return JSONResponse({"triggered": triggered > 0, "status": "scanning"})

