"""
routes/stock_momentum.py — Tab: Stock Momentum
===============================================
Dedicated momentum-only scan (no swing-trade entry conditions).
Applies only RSI, weekly RSI, ADX, volume Z-score, RS, MACD, and EMA filters
to catch early momentum moves across Nifty500 + Microcap250.

API routes
----------
GET  /api/stock-momentum          — latest momentum results (live or historical)
POST /api/stock-momentum/rescan   — force fresh momentum scans
"""

import asyncio
import logging
import time
from datetime import datetime

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

import shared_state as ss
from config import SCAN_INTERVAL_MINUTES, AUTO_RESCAN
from scanner import (
    MOM_RSI_MIN, MOM_WRSI_MIN, MOM_ADX_MIN,
    MOM_VOLZ_MIN, MOM_RS_MIN, MOM_RET20_MIN, MOM_RET5_MIN, MOM_TV_MIN_CR,
    MOM_DAY_CHG_MIN, MOM_CLOSE_POS_MIN, MOM_DAY_RANGE_MIN,
)
from routes.utils import _validate_date_param, _enrich_with_industry

logger = logging.getLogger(__name__)
router = APIRouter()

# Aliases for criteria payload / logging
_MOM_RSI_MIN   = MOM_RSI_MIN
_MOM_WRSI_MIN  = MOM_WRSI_MIN
_MOM_ADX_MIN   = MOM_ADX_MIN
_MOM_VOLZ_MIN  = MOM_VOLZ_MIN
_MOM_RS_MIN    = MOM_RS_MIN
_MOM_RET20_MIN = MOM_RET20_MIN
_MOM_RET5_MIN  = MOM_RET5_MIN
_MOM_TVMIN_CR  = MOM_TV_MIN_CR
_MOM_DAY_CHG_MIN = MOM_DAY_CHG_MIN
_MOM_CLOSE_POS_MIN = MOM_CLOSE_POS_MIN
_MOM_DAY_RANGE_MIN = MOM_DAY_RANGE_MIN


# ---------------------------------------------------------------------------
# Momentum score (technicals only, max 100 pts)
# ---------------------------------------------------------------------------

def _mom_score(s: dict) -> float:
    """Pure Momentum Score — technicals only.  Max = 100 pts.

    RSI zone          (0–30)  primary momentum (RSI 50→80)
    RS outperformance (0–25)  #1 win-rate predictor
    ADX strength      (0–20)  directional trend conviction
    MACD histogram    (0–10)  momentum acceleration
    Weekly RSI        (0–10)  higher-timeframe alignment
    Volume surge      (0– 5)  institutional activity
    """
    rsi   = float(s.get("rsi")            or 0)
    wrsi  = float(s.get("weekly_rsi")     or 0)
    adx   = float(s.get("adx")            or 0)
    vz    = float(s.get("vol_zscore")     or 0)
    rs    = float(s.get("rs_outperf_pct") or 0)
    mhist = float(s.get("macd_hist")      or 0)

    rsi_pts  = min(30.0, max(0.0, (rsi  - 50.0) * 1.0))
    wrsi_pts = min(10.0, max(0.0, (wrsi - 50.0) * 0.5))
    adx_pts  = min(20.0, max(0.0, (adx  - 20.0) * 0.571))
    rs_pts   = min(25.0, max(0.0, rs    * 2.5))
    vol_pts  = min( 5.0, max(0.0, (vz   - 0.5)  * 2.5))
    macd_pts = min(10.0, max(0.0, mhist * 200.0))

    return round(rsi_pts + wrsi_pts + adx_pts + rs_pts + vol_pts + macd_pts, 2)


# ---------------------------------------------------------------------------
# Internal scan runner
# ---------------------------------------------------------------------------

async def _do_run_momentum_scan(sc, state: dict, label: str) -> None:
    """Run scanner.scan(momentum_only=True) — must be called while _mom_scan_lock is held."""
    def _progress(stage: str):
        state["scan_stage"] = stage

    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            None, lambda: sc.scan(progress_cb=_progress, momentum_only=True)
        )
        now_ist = datetime.now(ss.IST).strftime("%Y-%m-%d %H:%M:%S IST")
        state.update({
            "data":           results,
            "last_updated":   now_ist,
            "status":         "complete",
            "scan_count":     state["scan_count"] + 1,
            "filters_passed": len(results),
            "next_scan_ts":   (time.time() + SCAN_INTERVAL_MINUTES * 60) if AUTO_RESCAN else None,
            "scan_stage":     f"{len(results)} momentum stocks",
            "error":          None,
            "regime_ok":      sc.last_regime_ok,
            "regime_summary": sc.last_regime_summary,
        })
        logger.info("=== %s Momentum Scan #%d complete — %d qualifying stocks ===",
                    label, state["scan_count"], len(results))

    except Exception as exc:
        logger.error("%s momentum scan failed: %s", label, exc, exc_info=True)
        state["status"] = "error"
        state["error"]  = str(exc)
        state["next_scan_ts"] = (time.time() + SCAN_INTERVAL_MINUTES * 60) if AUTO_RESCAN else None


# ---------------------------------------------------------------------------
# Scan task coroutines
# ---------------------------------------------------------------------------

async def run_n500_momentum_scan() -> None:
    """Run a momentum-only scan for the Nifty 500 universe."""
    ss.mom_scan_state["status"] = "scanning"
    if ss._mom_scan_lock.locked():
        logger.info("N500 momentum scan queued — waiting for MC250 momentum scan...")
        ss.mom_scan_state["scan_stage"] = "Queued (waiting for MC250 momentum scan)..."
    async with ss._mom_scan_lock:
        ss.mom_scan_state["scan_stage"]   = "Loading OHLCV data..."
        ss.mom_scan_state["next_scan_ts"] = None
        logger.info("=== Nifty500 Momentum scan starting ===")
        await _do_run_momentum_scan(ss.scanner, ss.mom_scan_state, "Nifty500-Momentum")


async def run_mc250_momentum_scan() -> None:
    """Run a momentum-only scan for the Microcap 250 universe."""
    ss.mc_mom_scan_state["status"] = "scanning"
    if ss._mom_scan_lock.locked():
        logger.info("MC250 momentum scan queued — waiting for N500 momentum scan...")
        ss.mc_mom_scan_state["scan_stage"] = "Queued (waiting for N500 momentum scan)..."
    async with ss._mom_scan_lock:
        ss.mc_mom_scan_state["scan_stage"]   = "Loading OHLCV data..."
        ss.mc_mom_scan_state["next_scan_ts"] = None
        logger.info("=== Microcap250 Momentum scan starting ===")
        await _do_run_momentum_scan(ss.scanner_mc, ss.mc_mom_scan_state, "Microcap250-Momentum")


# Scheduler wrappers
async def _maybe_run_n500_momentum_scan() -> None:
    if ss.mom_scan_state["status"] not in ("scanning",):
        await run_n500_momentum_scan()


async def _maybe_run_mc250_momentum_scan() -> None:
    if ss.mc_mom_scan_state["status"] not in ("scanning",):
        await run_mc250_momentum_scan()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/api/stock-momentum")
async def get_stock_momentum(
    date: str = Query(None, description="Historical date YYYY-MM-DD; omit for live data"),
) -> JSONResponse:
    """
    High-momentum subset of the N500 + MC250 universe.
    Completely independent of Swing Trade — only momentum criteria apply.
    Pass ?date=YYYY-MM-DD for historical results.
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
            await asyncio.gather(
                loop.run_in_executor(
                    None, lambda: ss.scanner.scan(target_date=target_date, momentum_only=True)
                ),
                loop.run_in_executor(
                    None, lambda: ss.scanner_mc.scan(target_date=target_date, momentum_only=True)
                ),
            )
        except Exception as exc:
            logger.error("Historical stock-momentum scan failed: %s", exc, exc_info=True)
            return JSONResponse({"error": str(exc), "stocks": [], "total": 0}, status_code=500)
        n500_stocks  = [dict(s, from_index="Nifty 500")    for s in (ss.scanner.last_momentum_results    or [])]
        mc_stocks    = [dict(s, from_index="Microcap 250") for s in (ss.scanner_mc.last_momentum_results or [])]
        n500_status  = mc250_status = "complete"
        last_n500    = last_mc250 = f"Historical data as of {date}"
    else:
        # Live mode
        n500_stocks  = [dict(s, from_index="Nifty 500")    for s in (ss.mom_scan_state.get("data")    or [])]
        mc_stocks    = [dict(s, from_index="Microcap 250") for s in (ss.mc_mom_scan_state.get("data") or [])]
        n500_status  = ss.mom_scan_state["status"]
        mc250_status = ss.mc_mom_scan_state["status"]
        last_n500    = ss.mom_scan_state.get("last_updated")
        last_mc250   = ss.mc_mom_scan_state.get("last_updated")

    all_stocks = n500_stocks + mc_stocks

    if not all_stocks and not target_date:
        if n500_status not in ("scanning", "complete"):
            asyncio.create_task(run_n500_momentum_scan())
        if mc250_status not in ("scanning", "complete"):
            asyncio.create_task(run_mc250_momentum_scan())

    # Score, enrich, and sort
    for s in all_stocks:
        s["mom_score"] = _mom_score(s)
        sym = s.get("display_ticker") or (s.get("ticker") or "").replace(".NS", "")
        static = ss._SECTOR_MAP.get(sym, {})
        if static.get("sector")   and not s.get("sector"):
            s["sector"]   = static["sector"]
        if static.get("industry") and not s.get("industry"):
            s["industry"] = static["industry"]
        fund = ss._fund_data.get(sym) or ss._fund_data.get(sym + ".NS") or {}
        if fund.get("sector"):
            s["sector"] = fund["sector"]
        if fund.get("industry"):
            s["industry"] = fund["industry"]

    all_stocks.sort(key=lambda x: x["mom_score"], reverse=True)

    n500_count = sum(1 for s in all_stocks if s.get("from_index") == "Nifty 500")
    mc_count   = sum(1 for s in all_stocks if s.get("from_index") == "Microcap 250")

    overall_status = (
        "scanning" if n500_status in ("scanning", "initializing")
                      or mc250_status in ("scanning", "initializing") else
        "complete" if (n500_status == "complete" or mc250_status == "complete") else
        "error"
    )

    return JSONResponse({
        "stocks":               all_stocks[:50],
        "total":                len(all_stocks),
        "n500_count":           n500_count,
        "mc_count":             mc_count,
        "n500_status":          n500_status,
        "mc250_status":         mc250_status,
        "last_updated_n500":    last_n500,
        "last_updated_mc250":   last_mc250,
        "status":               overall_status,
        "scan_stage":           ss.mom_scan_state.get("scan_stage", "") if not target_date else "",
        "as_of_date":           date if target_date else None,
        "criteria": {
            "rsi_min":    _MOM_RSI_MIN,
            "wrsi_min":   _MOM_WRSI_MIN,
            "adx_min":    _MOM_ADX_MIN,
            "volz_min":   _MOM_VOLZ_MIN,
            "rs_min":     _MOM_RS_MIN,
            "ret20_min":  _MOM_RET20_MIN,
            "ret5_min":   _MOM_RET5_MIN,
            "tv_min_cr":  _MOM_TVMIN_CR,
            "day_chg_min": _MOM_DAY_CHG_MIN,
            "close_pos_min_pct": round(_MOM_CLOSE_POS_MIN * 100, 1),
            "day_range_min": _MOM_DAY_RANGE_MIN,
            "macd":       "MACD(12,26,9) > Signal AND MACD > 0",
        },
    })


@router.post("/api/stock-momentum/rescan")
async def force_momentum_rescan() -> JSONResponse:
    """Force fresh momentum-only scans for both N500 and MC250 universes."""
    triggered = 0
    if ss.mom_scan_state["status"] not in ("scanning",):
        asyncio.create_task(run_n500_momentum_scan())
        triggered += 1
    if ss.mc_mom_scan_state["status"] not in ("scanning",):
        asyncio.create_task(run_mc250_momentum_scan())
        triggered += 1
    return JSONResponse({"triggered": triggered > 0, "status": "scanning"})

