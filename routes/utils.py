"""
routes/utils.py — Shared helper functions used by multiple route modules.
"""

import json
import time
from datetime import datetime, date as DateType

from fastapi.responses import JSONResponse, StreamingResponse

import shared_state as ss
import cache as _ohlcv_cache


# ---------------------------------------------------------------------------
# IST date helper
# ---------------------------------------------------------------------------
def _ist_today() -> DateType:
    """Return today's date in IST (UTC+5:30) regardless of server timezone."""
    return datetime.now(ss.IST).date()


# ---------------------------------------------------------------------------
# Date parameter validation
# ---------------------------------------------------------------------------
def _validate_date_param(date: str):
    """Parse and validate a YYYY-MM-DD date string.
    Returns (DateType, None) on success, or (None, JSONResponse) on error.
    """
    try:
        target = DateType.fromisoformat(date)
    except ValueError:
        return None, JSONResponse(
            {"error": "Invalid date format — use YYYY-MM-DD"}, status_code=400
        )
    today = _ist_today()
    if target > today:
        return None, JSONResponse(
            {"error": "Date cannot be in the future"}, status_code=400
        )
    min_date = today.replace(year=today.year - 3)
    if target < min_date:
        return None, JSONResponse(
            {"error": f"Date cannot be earlier than {min_date}"}, status_code=400
        )
    return target, None


# ---------------------------------------------------------------------------
# SSE stream generator
# ---------------------------------------------------------------------------
_SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
}


def _make_sse_generator(state: dict):
    """Return an async generator that streams SSE events from a scan state dict."""
    import asyncio

    async def event_generator():
        last_hash = None
        while True:
            nsi = (
                max(0, int(state["next_scan_ts"] - time.time()))
                if state.get("next_scan_ts") else None
            )
            current = {
                "data":           state["data"],
                "last_updated":   state["last_updated"],
                "status":         state["status"],
                "scan_count":     state["scan_count"],
                "filters_passed": state["filters_passed"],
                "total_tickers":  state["total_tickers"],
                "scan_stage":     state["scan_stage"],
                "server_time":    datetime.now(ss.IST).strftime("%Y-%m-%d %H:%M:%S IST"),
                "next_scan_in":   nsi,
                "error":          state.get("error"),
                "regime_ok":      state.get("regime_ok", True),
                "regime_summary": state.get("regime_summary", ""),
            }
            state_hash = (state["status"], state["scan_count"], state["scan_stage"])
            if state_hash != last_hash:
                last_hash = state_hash
                yield f"data: {json.dumps(current)}\n\n"
            else:
                hb = {
                    "status":       state["status"],
                    "server_time":  datetime.now(ss.IST).strftime("%H:%M:%S IST"),
                    "next_scan_in": nsi,
                }
                yield f"event: heartbeat\ndata: {json.dumps(hb)}\n\n"
            await asyncio.sleep(3)

    return event_generator


# ---------------------------------------------------------------------------
# Sector / industry enrichment
# ---------------------------------------------------------------------------
def _enrich_with_industry(stocks: list) -> None:
    """Inject 'sector' and 'industry' from the static SECTOR_MAP and from
    the live fundamentals cache.  Best-effort — silently skips tickers not found."""
    for s in stocks:
        sym = s.get("display_ticker") or (s.get("ticker") or "").replace(".NS", "")
        cur_sector   = s.get("sector")   or ""
        cur_industry = s.get("industry") or ""
        if cur_sector   in ("Unknown", ""):  cur_sector   = ""
        if cur_industry in ("Unknown", ""):  cur_industry = ""

        # 1. Static map — always available
        static = ss._SECTOR_MAP.get(sym, {})
        if static.get("sector")   and not cur_sector:
            s["sector"]   = static["sector"];   cur_sector   = static["sector"]
        if static.get("industry") and not cur_industry:
            s["industry"] = static["industry"]; cur_industry = static["industry"]

        # 2. Live fundamentals cache — overrides static map
        fund = ss._fund_data.get(sym) or ss._fund_data.get(sym + ".NS") or {}
        if fund.get("sector"):
            s["sector"] = fund["sector"]
        if fund.get("industry"):
            s["industry"] = fund["industry"]


# ---------------------------------------------------------------------------
# ATR-based stop-loss from OHLCV disk cache (no network)
# ---------------------------------------------------------------------------
def _compute_stop_loss_from_cache(ticker: str, cp: float):
    """Compute ATR14-based structural stop loss from OHLCV disk cache.
    Returns (stop_loss, atr14) or (None, None) on any failure.
    """
    try:
        import pandas as _pd
        df = _ohlcv_cache.load(ticker)
        if df is None or len(df) < 20:
            return None, None
        h  = df["High"]
        lo = df["Low"]
        c  = df["Close"]
        tr = _pd.concat([
            (h - lo).abs(),
            (h - c.shift(1)).abs(),
            (lo - c.shift(1)).abs(),
        ], axis=1).max(axis=1)
        atr14_s   = tr.ewm(com=13, min_periods=14, adjust=False).mean()
        atr_clean = atr14_s.dropna()
        if atr_clean.empty:
            return None, None
        atr14_val = float(atr_clean.iloc[-1])
        if atr14_val <= 0 or _pd.isna(atr14_val):
            return None, None
        cl        = float(lo.iloc[-1])
        sl_recent = float(lo.iloc[-4:-1].min()) if len(lo) >= 4 else cl
        sl_struct = max(cl, sl_recent)
        sl_cand   = max(sl_struct, cp - 1.0 * atr14_val)
        stop_loss = round(
            min(max(sl_cand, cp - 1.5 * atr14_val), cp - 0.5 * atr14_val), 2
        )
        return stop_loss, round(atr14_val, 2)
    except Exception:
        return None, None

