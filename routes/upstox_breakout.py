"""
routes/upstox_breakout.py -- Tab: Upstox Breakout Finder
=========================================================
Finds fresh daily breakouts across Nifty 500 + Microcap 250 using Upstox
historical candle data.

API routes
----------
GET  /api/upstox-breakout          — latest breakout results (auto-triggers first run)
POST /api/upstox-breakout/rescan   — force fresh breakout scan
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from threading import Lock
from urllib.parse import quote

import pandas as pd
import requests
import ta
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from tickers import NIFTY500_TICKERS, NIFTY_MICROCAP250_TICKERS

logger = logging.getLogger(__name__)
router = APIRouter()

# Breakout gates
BK_LOOKBACK_DAYS = 20
BK_BREAKOUT_MIN_PCT = 0.6
BK_VOL_RATIO_MIN = 1.4
BK_RSI_MIN = 58.0
BK_MAX_WORKERS = 6


class _UpstoxClient:
    _INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
    _HIST_URL = "https://api.upstox.com/v2/historical-candle/{instrument_key}/day/{to_date}/{from_date}"
    _TIMEOUT = 18

    def __init__(self) -> None:
        self._token = (os.getenv("UPSTOX_ACCESS_TOKEN") or "").strip()
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "scanner-upstox-breakout/1.0",
        })
        if self._token:
            self._session.headers.update({"Authorization": f"Bearer {self._token}"})

        self._map_lock = Lock()
        self._symbol_to_key: dict[str, str] = {}
        self._map_loaded_ts: float = 0.0

    @property
    def enabled(self) -> bool:
        return bool(self._token)

    def _load_instrument_map(self) -> None:
        with self._map_lock:
            now_ts = datetime.now().timestamp()
            # Refresh every 6 hours to pick up daily BOD updates.
            if self._symbol_to_key and (now_ts - self._map_loaded_ts) < 6 * 3600:
                return

            r = self._session.get(self._INSTRUMENTS_URL, timeout=self._TIMEOUT)
            r.raise_for_status()
            rows = json.loads(gzip.decompress(r.content).decode("utf-8"))

            mp: dict[str, str] = {}
            for row in rows:
                if row.get("segment") != "NSE_EQ":
                    continue
                if row.get("instrument_type") != "EQ":
                    continue
                sym = str(row.get("trading_symbol") or "").strip().upper()
                key = str(row.get("instrument_key") or "").strip()
                if sym and key:
                    mp[sym] = key

            self._symbol_to_key = mp
            self._map_loaded_ts = now_ts
            logger.info("Upstox instrument map loaded: %d NSE_EQ symbols", len(mp))

    def instrument_key_for_symbol(self, symbol: str) -> str | None:
        self._load_instrument_map()
        return self._symbol_to_key.get(symbol.upper())

    def get_daily_candles(self, instrument_key: str, from_date: str, to_date: str) -> pd.DataFrame:
        url = self._HIST_URL.format(
            instrument_key=quote(instrument_key, safe=""),
            from_date=from_date,
            to_date=to_date,
        )
        r = self._session.get(url, timeout=self._TIMEOUT)
        r.raise_for_status()
        body = r.json() or {}
        candles = (((body.get("data") or {}).get("candles")) or [])
        if not candles:
            return pd.DataFrame()

        # Upstox returns [ts, open, high, low, close, volume, oi]
        df = pd.DataFrame(candles, columns=["ts", "Open", "High", "Low", "Close", "Volume", "OI"])
        for col in ("Open", "High", "Low", "Close", "Volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["Date"] = pd.to_datetime(df["ts"], errors="coerce")
        df = df.dropna(subset=["Date", "Open", "High", "Low", "Close", "Volume"])
        if df.empty:
            return df
        df = df.sort_values("Date").reset_index(drop=True)
        return df[["Date", "Open", "High", "Low", "Close", "Volume"]]


_upstox = _UpstoxClient()
_breakout_lock = asyncio.Lock()

_scan_state: dict = {
    "stocks": [],
    "status": "initializing",
    "scan_stage": "",
    "scan_count": 0,
    "last_updated": None,
    "error": None,
    "total": 0,
    "n500_count": 0,
    "mc_count": 0,
    "source": "Upstox",
    "criteria": {
        "lookback_days": BK_LOOKBACK_DAYS,
        "breakout_min_pct": BK_BREAKOUT_MIN_PCT,
        "vol_ratio_min": BK_VOL_RATIO_MIN,
        "rsi_min": BK_RSI_MIN,
    },
}


def _calc_score(close_px: float, breakout_pct: float, vol_ratio: float, rsi: float, ema20: float, ema50: float) -> float:
    ema_gap_pct = ((ema20 / ema50) - 1.0) * 100.0 if ema50 > 0 else 0.0
    price_vs_ema20 = ((close_px / ema20) - 1.0) * 100.0 if ema20 > 0 else 0.0
    score = (
        max(0.0, breakout_pct) * 9.0
        + max(0.0, (vol_ratio - 1.0)) * 25.0
        + max(0.0, (rsi - 50.0)) * 1.5
        + max(0.0, ema_gap_pct) * 7.0
        + max(0.0, price_vs_ema20) * 2.0
    )
    return round(score, 2)


def _analyze_symbol(ticker: str, from_index: str, from_date: str, to_date: str) -> dict | None:
    symbol = ticker.replace(".NS", "").upper().strip()
    instrument_key = _upstox.instrument_key_for_symbol(symbol)
    if not instrument_key:
        return None

    df = _upstox.get_daily_candles(instrument_key=instrument_key, from_date=from_date, to_date=to_date)
    if len(df) < 60:
        return None

    close = df["Close"].astype(float)
    high = df["High"].astype(float)
    low = df["Low"].astype(float)
    vol = df["Volume"].astype(float)

    ema20 = close.ewm(span=20, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()
    rsi14 = ta.momentum.RSIIndicator(close=close, window=14, fillna=False).rsi()

    if len(close) <= BK_LOOKBACK_DAYS:
        return None

    last_close = float(close.iloc[-1])
    last_vol = float(vol.iloc[-1])
    last_ema20 = float(ema20.iloc[-1])
    last_ema50 = float(ema50.iloc[-1])
    last_rsi = float(rsi14.iloc[-1]) if pd.notna(rsi14.iloc[-1]) else 0.0

    prev_high = float(high.iloc[-(BK_LOOKBACK_DAYS + 1):-1].max())
    avg_vol20 = float(vol.iloc[-(BK_LOOKBACK_DAYS + 1):-1].mean())
    low20 = float(low.iloc[-(BK_LOOKBACK_DAYS + 1):-1].min())

    if prev_high <= 0 or avg_vol20 <= 0:
        return None

    breakout_pct = ((last_close / prev_high) - 1.0) * 100.0
    vol_ratio = last_vol / avg_vol20

    passed = (
        breakout_pct >= BK_BREAKOUT_MIN_PCT
        and vol_ratio >= BK_VOL_RATIO_MIN
        and last_close > last_ema20 > last_ema50
        and last_rsi >= BK_RSI_MIN
    )
    if not passed:
        return None

    stop_loss = max(low20, last_ema20 * 0.99)
    risk_pct = ((last_close - stop_loss) / last_close) * 100.0 if last_close > 0 else None

    return {
        "ticker": f"{symbol}.NS",
        "display_ticker": symbol,
        "from_index": from_index,
        "price": round(last_close, 2),
        "breakout_pct": round(breakout_pct, 2),
        "vol_ratio": round(vol_ratio, 2),
        "rsi": round(last_rsi, 2),
        "ema20": round(last_ema20, 2),
        "ema50": round(last_ema50, 2),
        "stop_loss": round(stop_loss, 2),
        "risk_pct": round(risk_pct, 2) if risk_pct is not None else None,
        "score": _calc_score(last_close, breakout_pct, vol_ratio, last_rsi, last_ema20, last_ema50),
    }


def _scan_universe(tickers: list[str], from_index: str, from_date: str, to_date: str) -> list[dict]:
    out: list[dict] = []
    with ThreadPoolExecutor(max_workers=BK_MAX_WORKERS) as ex:
        futures = [
            ex.submit(_analyze_symbol, t, from_index, from_date, to_date)
            for t in tickers
        ]
        for f in as_completed(futures):
            try:
                row = f.result()
            except Exception:
                continue
            if row:
                out.append(row)
    return out


def _run_breakout_scan_blocking() -> dict:
    if not _upstox.enabled:
        return {
            "stocks": [],
            "total": 0,
            "n500_count": 0,
            "mc_count": 0,
            "error": "UPSTOX_ACCESS_TOKEN is not configured. Set it in environment and retry.",
        }

    to_date = datetime.now().date()
    from_date = to_date - timedelta(days=140)
    to_date_s = to_date.strftime("%Y-%m-%d")
    from_date_s = from_date.strftime("%Y-%m-%d")

    n500 = _scan_universe(NIFTY500_TICKERS, "Nifty 500", from_date_s, to_date_s)
    mc = _scan_universe(NIFTY_MICROCAP250_TICKERS, "Microcap 250", from_date_s, to_date_s)

    all_rows = sorted(n500 + mc, key=lambda x: x.get("score", 0.0), reverse=True)
    return {
        "stocks": all_rows[:100],
        "total": len(all_rows),
        "n500_count": len(n500),
        "mc_count": len(mc),
        "error": None,
    }


async def _run_breakout_scan() -> None:
    if _breakout_lock.locked():
        return
    async with _breakout_lock:
        _scan_state["status"] = "scanning"
        _scan_state["scan_stage"] = "Scanning Nifty 500 + Microcap 250 via Upstox..."
        _scan_state["error"] = None
        loop = asyncio.get_event_loop()
        try:
            payload = await loop.run_in_executor(None, _run_breakout_scan_blocking)
            now_ist = datetime.now().strftime("%Y-%m-%d %H:%M:%S IST")
            _scan_state.update({
                "stocks": payload.get("stocks") or [],
                "total": payload.get("total") or 0,
                "n500_count": payload.get("n500_count") or 0,
                "mc_count": payload.get("mc_count") or 0,
                "error": payload.get("error"),
                "status": "error" if payload.get("error") else "complete",
                "scan_stage": "Ready" if not payload.get("error") else "Configuration required",
                "last_updated": now_ist,
                "scan_count": _scan_state.get("scan_count", 0) + 1,
            })
        except Exception as exc:
            logger.error("Upstox breakout scan failed: %s", exc, exc_info=True)
            _scan_state["status"] = "error"
            _scan_state["error"] = str(exc)
            _scan_state["scan_stage"] = "Failed"


@router.get("/api/upstox-breakout")
async def get_upstox_breakout(
    refresh: int = Query(0, description="Set 1 to force a fresh scan"),
) -> JSONResponse:
    if refresh == 1 and _scan_state.get("status") != "scanning":
        asyncio.create_task(_run_breakout_scan())
    elif not _scan_state.get("stocks") and _scan_state.get("status") != "scanning":
        asyncio.create_task(_run_breakout_scan())
    return JSONResponse(_scan_state)


@router.post("/api/upstox-breakout/rescan")
async def force_upstox_breakout_rescan() -> JSONResponse:
    if _scan_state.get("status") != "scanning":
        asyncio.create_task(_run_breakout_scan())
    return JSONResponse({"status": "scanning", "triggered": True})
