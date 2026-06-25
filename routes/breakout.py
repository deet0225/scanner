"""
routes/upstox_breakout.py -- Tab: Breakout Finder
=================================================
Finds fresh daily breakouts across Nifty 500 + Microcap 250 using Zerodha
Kite Connect historical candle data.

API routes
----------
GET  /api/breakout          — latest breakout results (auto-triggers first run)
POST /api/breakout/rescan   — force fresh breakout scan

Required environment variables
------------------------------
ZERODHA_API_KEY      — Zerodha Kite Connect API key
ZERODHA_ACCESS_TOKEN — Zerodha Kite Connect access token
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date as _date_type
from threading import Lock

import pandas as pd
import requests
import ta
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from tickers import NIFTY500_TICKERS, NIFTY_MICROCAP250_TICKERS

logger = logging.getLogger(__name__)
router = APIRouter()

# Breakout gates — tuned for swing trading
BK_LOOKBACK_DAYS     = 50     # 50-day high breakout (more selective than 20D)
BK_BREAKOUT_MIN_PCT  = 0.3    # must close at least 0.3% above 50D high
BK_VOL_RATIO_MIN     = 1.5    # today vol >= 1.5× 20D avg vol
BK_RSI_MIN           = 55.0   # minimum RSI (momentum present)
BK_RSI_MAX           = 80.0   # maximum RSI (avoid overbought)
BK_ADX_MIN           = 20.0   # minimum ADX (trend has direction)
BK_MIN_PRICE         = 20.0   # price floor in ₹ (no penny stocks)
BK_CANDLE_BODY_MIN   = 0.45   # close in top 45% of day's High-Low range
BK_MIN_ROWS          = 120    # minimum OHLCV rows required
BK_MAX_WORKERS       = 6


class _ZerodhaClient:
    """Zerodha Kite Connect client for historical OHLCV data.

    Instrument map : GET https://api.kite.trade/instruments  (public CSV, no auth)
    Historical data: GET https://api.kite.trade/instruments/historical/{token}/day
    Auth header    : Authorization: token {api_key}:{access_token}
    """
    _INSTRUMENTS_URL = "https://api.kite.trade/instruments"
    _HIST_URL = "https://api.kite.trade/instruments/historical/{token}/day"
    _QUOTE_URL = "https://api.kite.trade/quote"
    _TIMEOUT = 18
    _QUOTE_BATCH_MAX = 500

    def __init__(self) -> None:
        self._api_key      = (os.getenv("ZERODHA_API_KEY")      or "").strip()
        self._access_token = (os.getenv("ZERODHA_ACCESS_TOKEN") or "").strip()
        self._session = requests.Session()
        self._session.verify = False
        self._session.headers.update({
            "Accept": "application/json",
            "X-Kite-Version": "3",
            "User-Agent": "scanner-breakout/1.0",
        })
        if self._api_key and self._access_token:
            self._session.headers.update({
                "Authorization": f"token {self._api_key}:{self._access_token}"
            })

        self._map_lock = Lock()
        self._symbol_to_token: dict[str, int] = {}
        self._map_loaded_ts: float = 0.0

    @property
    def enabled(self) -> bool:
        try:
            from config import ENABLE_ZERODHA  # type: ignore[import]
            if not ENABLE_ZERODHA:
                return False
        except ImportError:
            pass
        return bool(self._api_key and self._access_token)

    def _load_instrument_map(self) -> None:
        with self._map_lock:
            now_ts = datetime.now().timestamp()
            # Refresh every 6 hours to pick up daily instrument list updates.
            if self._symbol_to_token and (now_ts - self._map_loaded_ts) < 6 * 3600:
                return

            r = self._session.get(self._INSTRUMENTS_URL, timeout=self._TIMEOUT)
            r.raise_for_status()

            # Kite instruments endpoint returns a plain CSV (no auth required).
            df = pd.read_csv(io.StringIO(r.text))
            nse_eq = df[
                (df["exchange"] == "NSE") & (df["instrument_type"] == "EQ")
            ]
            mp: dict[str, int] = {}
            for _, row in nse_eq.iterrows():
                sym = str(row.get("tradingsymbol") or "").strip().upper()
                tok = row.get("instrument_token")
                if sym and pd.notna(tok):
                    mp[sym] = int(tok)

            self._symbol_to_token = mp
            self._map_loaded_ts = now_ts
            logger.info("Zerodha instrument map loaded: %d NSE EQ symbols", len(mp))

    def token_for_symbol(self, symbol: str) -> int | None:
        self._load_instrument_map()
        return self._symbol_to_token.get(symbol.upper())

    def get_daily_candles(self, token: int, from_date: str, to_date: str) -> pd.DataFrame:
        """Fetch daily OHLCV candles from Zerodha for the given instrument token."""
        r = self._session.get(
            self._HIST_URL.format(token=token),
            params={"from": from_date, "to": to_date},
            timeout=self._TIMEOUT,
        )
        r.raise_for_status()
        body = r.json() or {}
        candles = (body.get("data") or {}).get("candles") or []
        if not candles:
            return pd.DataFrame()

        # Zerodha returns [timestamp, open, high, low, close, volume, oi]
        df = pd.DataFrame(candles, columns=["ts", "Open", "High", "Low", "Close", "Volume", "OI"])
        for col in ("Open", "High", "Low", "Close", "Volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["Date"] = pd.to_datetime(df["ts"], errors="coerce")
        df = df.dropna(subset=["Date", "Open", "High", "Low", "Close", "Volume"])
        if df.empty:
            return df
        df = df.sort_values("Date").reset_index(drop=True)
        return df[["Date", "Open", "High", "Low", "Close", "Volume"]]

    def get_live_quotes_batch(self, tickers: list) -> dict:
        """
        Fetch live OHLCV for multiple NSE tickers via Zerodha /quote API.
        Returns {SYMBOL: {"open", "high", "low", "close", "volume"}}
        where "close" = last_price (current traded price).
        """
        if not self.enabled or not tickers:
            return {}
        symbols = list({t.upper().replace(".NS", "").replace(".BO", "") for t in tickers})
        out: dict = {}
        for i in range(0, len(symbols), self._QUOTE_BATCH_MAX):
            batch  = symbols[i: i + self._QUOTE_BATCH_MAX]
            params = [("i", f"NSE:{s}") for s in batch]
            try:
                r = self._session.get(self._QUOTE_URL, params=params, timeout=self._TIMEOUT)
                r.raise_for_status()
                data = (r.json() or {}).get("data") or {}
                for key, val in data.items():
                    sym  = key.split(":")[-1].upper()
                    lp   = val.get("last_price")
                    vol  = val.get("volume")
                    ohlc = val.get("ohlc") or {}
                    if lp:
                        out[sym] = {
                            "open":   float(ohlc.get("open")  or lp),
                            "high":   float(ohlc.get("high")  or lp),
                            "low":    float(ohlc.get("low")   or lp),
                            "close":  float(lp),
                            "volume": float(vol or 0),
                        }
            except Exception as exc:
                logger.warning("Zerodha live quotes batch [%d:%d] failed: %s",
                               i, i + len(batch), exc)
        return out


_zerodha = _ZerodhaClient()
_breakout_lock = asyncio.Lock()

# ---------------------------------------------------------------------------
# Shared yfinance fallback session (SSL-bypass, no NSE referer)
# ---------------------------------------------------------------------------
ssl._create_default_https_context = ssl._create_unverified_context  # type: ignore
_yf_session = requests.Session()
_yf_session.verify = False
_yf_session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
})


def _ist_today_bk() -> _date_type:
    """Return today's date in IST (UTC+5:30)."""
    import datetime as _dt_mod
    _IST = _dt_mod.timezone(_dt_mod.timedelta(hours=5, minutes=30))
    return _dt_mod.datetime.now(_IST).date()


def _validate_bk_date(date_str: str):
    """Validate YYYY-MM-DD. Returns (date, None) or (None, JSONResponse error)."""
    try:
        d = _date_type.fromisoformat(date_str)
        if d > _ist_today_bk():
            return None, JSONResponse({"error": "Date cannot be in the future."}, status_code=400)
        return d, None
    except ValueError:
        return None, JSONResponse(
            {"error": f"Invalid date: {date_str!r}. Use YYYY-MM-DD."},
            status_code=400,
        )


def _fetch_ohlcv(symbol: str, from_date_s: str, to_date_s: str) -> pd.DataFrame:
    """
    Fetch daily OHLCV: Zerodha primary, Yahoo Finance chart API fallback.
    Returns DataFrame with columns: Date (pd.Timestamp), Open, High, Low, Close, Volume.
    Empty DataFrame if all sources fail.
    """
    # 1. Zerodha (primary, requires credentials)
    if _zerodha.enabled:
        token = _zerodha.token_for_symbol(symbol)
        if token:
            try:
                df = _zerodha.get_daily_candles(
                    token=token, from_date=from_date_s, to_date=to_date_s
                )
                if len(df) >= 60:
                    return df
            except Exception as exc:
                logger.debug("Zerodha OHLCV failed for %s: %s", symbol, exc)

    # 2. Yahoo Finance chart API fallback
    try:
        p1 = int(datetime.fromisoformat(from_date_s + "T00:00:00").timestamp())
        p2 = int(datetime.fromisoformat(to_date_s   + "T23:59:59").timestamp())
        r = _yf_session.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.NS",
            params={"period1": p1, "period2": p2, "interval": "1d", "events": "div,split"},
            timeout=18,
        )
        if r.status_code == 200:
            body   = r.json()
            result = (body.get("chart") or {}).get("result") or []
            if result:
                ts_list = result[0].get("timestamp") or []
                quote   = result[0].get("indicators", {}).get("quote", [{}])[0]
                adj_cl  = result[0].get("indicators", {}).get("adjclose", [{}])
                closes  = (adj_cl[0].get("adjclose") if adj_cl else None) or quote.get("close", [])
                records = []
                for i, ts in enumerate(ts_list):
                    try:
                        records.append({
                            "Date":   pd.Timestamp(ts, unit="s").normalize(),
                            "Open":   float((quote.get("open",   []) or [])[i] or 0),
                            "High":   float((quote.get("high",   []) or [])[i] or 0),
                            "Low":    float((quote.get("low",    []) or [])[i] or 0),
                            "Close":  float((closes or [])[i] or 0),
                            "Volume": float((quote.get("volume", []) or [])[i] or 0),
                        })
                    except (IndexError, TypeError, ValueError):
                        pass
                if records:
                    df = pd.DataFrame(records)
                    df = df[df["Close"] > 0].sort_values("Date").reset_index(drop=True)
                    if len(df) >= 60:
                        logger.debug("yfinance fallback OK for %s: %d rows", symbol, len(df))
                        return df
    except Exception as exc:
        logger.debug("yfinance chart API fallback failed for %s: %s", symbol, exc)

    return pd.DataFrame()

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
    "source": "Zerodha",
    "criteria": {
        "lookback_days":    BK_LOOKBACK_DAYS,
        "breakout_min_pct": BK_BREAKOUT_MIN_PCT,
        "vol_ratio_min":    BK_VOL_RATIO_MIN,
        "rsi_min":          BK_RSI_MIN,
        "rsi_max":          BK_RSI_MAX,
        "adx_min":          BK_ADX_MIN,
        "min_price":        BK_MIN_PRICE,
    },
}


def _calc_score(
    breakout_pct: float, vol_ratio: float, rsi: float, adx: float,
    is_52w_break: bool, candle_body_pct: float, pct_from_52w: float,
    ema200_gap_pct: float,
) -> float:
    """Score 0–100+ indicating swing-trade breakout quality."""
    score = 0.0
    # 1. Volume surge: 1.5x→5 pts, 3x→20 pts, 5x+→40 pts (capped)
    score += min(max(vol_ratio - 1.0, 0.0), 4.0) * 10.0
    # 2. 52-week high breakout: big bonus; proximity credit otherwise
    if is_52w_break:
        score += 30.0
    else:
        # pct_from_52w is negative; closer = more pts (max 10 at -0.5%)
        score += max(0.0, 10.0 + pct_from_52w * 1.5)
    # 3. RSI sweet spot 60-72 scores highest; penalise above 72
    rsi_pts = max(0.0, rsi - 55.0) * 1.2
    if rsi > 72:
        rsi_pts -= (rsi - 72) * 2.5
    score += max(0.0, rsi_pts)
    # 4. ADX trend strength (ADX20→0 pts, ADX50→30 pts)
    score += min(max(adx - 20.0, 0.0), 30.0) * 1.0
    # 5. Candle body quality (closed near top of range)
    score += candle_body_pct * 10.0
    # 6. Breakout margin above 50D high (0%→0, 5%+→15 pts)
    score += min(max(breakout_pct, 0.0), 5.0) * 3.0
    # 7. EMA200 alignment bonus (ema20 ahead of ema200 = longer uptrend)
    if ema200_gap_pct > 0:
        score += min(ema200_gap_pct, 15.0) * 0.4
    return round(score, 2)


def _analyze_symbol(ticker: str, from_index: str, from_date: str, to_date: str,
                    live_quote: "dict | None" = None,
                    target_date: "_date_type | None" = None) -> "dict | None":
    symbol = ticker.replace(".NS", "").upper().strip()

    df = _fetch_ohlcv(symbol, from_date, to_date)
    if len(df) < BK_MIN_ROWS:
        return None

    # Patch / add today's candle with live data (live mode only)
    if live_quote and target_date is None:
        today_ts = pd.Timestamp(datetime.now().date())
        last_date = df["Date"].iloc[-1]
        if hasattr(last_date, "date"):
            last_date = last_date.date()
        if last_date == datetime.now().date():
            df.loc[df.index[-1], "Open"]   = live_quote["open"]
            df.loc[df.index[-1], "High"]   = max(float(df["High"].iloc[-1]), live_quote["high"])
            df.loc[df.index[-1], "Low"]    = min(float(df["Low"].iloc[-1]),  live_quote["low"])
            df.loc[df.index[-1], "Close"]  = live_quote["close"]
            df.loc[df.index[-1], "Volume"] = live_quote["volume"]
        else:
            new_row = pd.DataFrame([{
                "Date": today_ts, "Open": live_quote["open"],
                "High": live_quote["high"], "Low": live_quote["low"],
                "Close": live_quote["close"], "Volume": live_quote["volume"],
            }])
            df = pd.concat([df, new_row], ignore_index=True).sort_values("Date").reset_index(drop=True)

    close  = df["Close"].astype(float)
    high   = df["High"].astype(float)
    low    = df["Low"].astype(float)
    open_  = df["Open"].astype(float)
    vol    = df["Volume"].astype(float)

    # ── Technical indicators ──────────────────────────────────────────
    ema20  = close.ewm(span=20,  adjust=False).mean()
    ema50  = close.ewm(span=50,  adjust=False).mean()
    ema200 = close.ewm(span=200, adjust=False).mean()
    rsi14  = ta.momentum.RSIIndicator(close=close, window=14, fillna=False).rsi()
    atr14  = ta.volatility.AverageTrueRange(
                 high=high, low=low, close=close, window=14, fillna=False
             ).average_true_range()
    adx14  = ta.trend.ADXIndicator(
                 high=high, low=low, close=close, window=14, fillna=False
             ).adx()

    if len(close) <= BK_LOOKBACK_DAYS + 1:
        return None

    last_close  = float(close.iloc[-1])
    last_high   = float(high.iloc[-1])
    last_low    = float(low.iloc[-1])
    last_vol    = float(vol.iloc[-1])
    last_ema20  = float(ema20.iloc[-1])
    last_ema50  = float(ema50.iloc[-1])
    last_ema200 = float(ema200.iloc[-1])
    last_rsi    = float(rsi14.iloc[-1]) if pd.notna(rsi14.iloc[-1]) else 0.0
    last_atr    = float(atr14.iloc[-1]) if pd.notna(atr14.iloc[-1]) else 0.0
    last_adx    = float(adx14.iloc[-1]) if pd.notna(adx14.iloc[-1]) else 0.0

    # ── Rolling reference windows (excluding today) ───────────────────
    prev_highs = high.iloc[-(BK_LOOKBACK_DAYS + 1):-1]
    prev_lows  = low.iloc[-(BK_LOOKBACK_DAYS + 1):-1]
    prev_vol20 = vol.iloc[-21:-1]            # 20-day avg vol (excludes today)
    n52        = min(252, len(high) - 1)     # up to 252 trading days for 52W
    w52_highs  = high.iloc[-(n52 + 1):-1]

    prev_high_bk = float(prev_highs.max())
    prev_low_bk  = float(prev_lows.min())
    avg_vol20    = float(prev_vol20.mean())
    high_52w     = float(w52_highs.max())

    if prev_high_bk <= 0 or avg_vol20 <= 0 or last_close <= 0:
        return None

    # ── Derived metrics ───────────────────────────────────────────────
    breakout_pct    = ((last_close / prev_high_bk) - 1.0) * 100.0
    vol_ratio       = last_vol / avg_vol20
    candle_range    = last_high - last_low
    # Fraction of day's range captured above the low (1.0 = closed at high)
    candle_body_pct = (last_close - last_low) / candle_range if candle_range > 0.01 else 0.5
    pct_from_52w    = ((last_close / high_52w) - 1.0) * 100.0 if high_52w > 0 else -999.0
    is_52w_break    = pct_from_52w >= -0.5   # at or within 0.5% of 52W high
    ema200_gap_pct  = ((last_ema20 / last_ema200) - 1.0) * 100.0 if last_ema200 > 0 else 0.0

    # ── Hard gates (all must pass) ────────────────────────────────────
    passed = (
        last_close >= BK_MIN_PRICE                       # no penny stocks
        and breakout_pct >= BK_BREAKOUT_MIN_PCT          # broke above 50D high
        and vol_ratio >= BK_VOL_RATIO_MIN                # strong volume surge
        and last_close > last_ema20 > last_ema50         # price above rising EMAs
        and BK_RSI_MIN <= last_rsi <= BK_RSI_MAX         # momentum present, not overbought
        and last_adx >= BK_ADX_MIN                       # directional trend strength
        and candle_body_pct >= BK_CANDLE_BODY_MIN        # bullish close (not bearish wick)
    )
    if not passed:
        return None

    # ── ATR-based stop loss ───────────────────────────────────────────
    # Place stop below today's low with 0.5 ATR buffer
    atr_stop  = (last_low - 0.5 * last_atr) if last_atr > 0 else last_close * 0.95
    ema_stop  = last_ema20 * 0.98
    stop_loss = max(atr_stop, ema_stop, prev_low_bk * 0.99)
    stop_loss = max(stop_loss, last_close * 0.90)  # hard cap: max 10% drawdown

    risk_amt = last_close - stop_loss
    risk_pct = (risk_amt / last_close) * 100.0 if last_close > 0 else None

    # Target: 2.5 ATR above close
    target_price = (last_close + 2.5 * last_atr) if last_atr > 0 else None
    rr_ratio = ((target_price - last_close) / risk_amt
                if (target_price and risk_amt > 0) else None)

    score = _calc_score(
        breakout_pct, vol_ratio, last_rsi, last_adx,
        is_52w_break, candle_body_pct, pct_from_52w, ema200_gap_pct,
    )

    return {
        "ticker":         f"{symbol}.NS",
        "display_ticker": symbol,
        "from_index":     from_index,
        "price":          round(last_close, 2),
        "breakout_pct":   round(breakout_pct, 2),
        "vol_ratio":      round(vol_ratio, 2),
        "rsi":            round(last_rsi, 2),
        "adx":            round(last_adx, 2),
        "ema20":          round(last_ema20, 2),
        "ema50":          round(last_ema50, 2),
        "ema200":         round(last_ema200, 2),
        "atr":            round(last_atr, 2),
        "stop_loss":      round(stop_loss, 2),
        "risk_pct":       round(risk_pct, 2) if risk_pct is not None else None,
        "target":         round(target_price, 2) if target_price else None,
        "rr_ratio":       round(rr_ratio, 2) if rr_ratio is not None else None,
        "is_52w_break":   is_52w_break,
        "pct_from_52w":   round(pct_from_52w, 2),
        "score":          score,
    }


def _scan_universe(tickers: list[str], from_index: str, from_date: str, to_date: str,
                   live_quotes: "dict | None" = None,
                   target_date: "_date_type | None" = None) -> list[dict]:
    out: list[dict] = []
    with ThreadPoolExecutor(max_workers=BK_MAX_WORKERS) as ex:
        futures = [
            ex.submit(
                _analyze_symbol, t, from_index, from_date, to_date,
                (live_quotes or {}).get(t.replace(".NS", "").upper().strip()),
                target_date,
            )
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


def _run_breakout_scan_blocking(target_date: "_date_type | None" = None) -> dict:
    to_date   = target_date or _ist_today_bk()
    from_date = to_date - timedelta(days=400)   # enough for EMA200 + 52W high
    to_date_s   = to_date.strftime("%Y-%m-%d")
    from_date_s = from_date.strftime("%Y-%m-%d")

    # Fetch live quotes only in live mode (Zerodha only)
    live_quotes: dict = {}
    if target_date is None and _zerodha.enabled:
        all_tickers = list(NIFTY500_TICKERS) + list(NIFTY_MICROCAP250_TICKERS)
        live_quotes = _zerodha.get_live_quotes_batch(all_tickers)
        logger.info("Zerodha live quotes fetched: %d symbols", len(live_quotes))

    source = "Zerodha" if _zerodha.enabled else "yfinance"
    n500 = _scan_universe(NIFTY500_TICKERS, "Nifty 500", from_date_s, to_date_s, live_quotes, target_date)
    mc   = _scan_universe(NIFTY_MICROCAP250_TICKERS, "Microcap 250", from_date_s, to_date_s, live_quotes, target_date)

    all_rows = sorted(n500 + mc, key=lambda x: x.get("score", 0.0), reverse=True)
    return {
        "stocks":     all_rows[:100],
        "total":      len(all_rows),
        "n500_count": len(n500),
        "mc_count":   len(mc),
        "source":     source,
        "error":      None,
    }


async def _run_breakout_scan() -> None:
    if _breakout_lock.locked():
        return
    async with _breakout_lock:
        _scan_state["status"] = "scanning"
        _scan_state["scan_stage"] = "Scanning Nifty 500 + Microcap 250 via Zerodha / yfinance..."
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
            logger.error("Zerodha breakout scan failed: %s", exc, exc_info=True)
            _scan_state["status"] = "error"
            _scan_state["error"] = str(exc)
            _scan_state["scan_stage"] = "Failed"


@router.get("/api/breakout")
async def get_breakout(
    refresh: int = Query(0, description="Set 1 to force a fresh scan"),
    date: str    = Query(None, description="Historical date YYYY-MM-DD; omit for live data"),
) -> JSONResponse:
    # Historical mode — run synchronously and return directly
    if date:
        target_date, err = _validate_bk_date(date)
        if err:
            return err
        loop = asyncio.get_event_loop()
        try:
            payload = await loop.run_in_executor(
                None, lambda: _run_breakout_scan_blocking(target_date=target_date)
            )
            payload["status"]    = "error" if payload.get("error") else "complete"
            payload["as_of_date"] = str(target_date)
            return JSONResponse(payload)
        except Exception as exc:
            logger.error("Historical breakout scan failed: %s", exc, exc_info=True)
            return JSONResponse(
                {"error": str(exc), "stocks": [], "total": 0, "status": "error"},
                status_code=500,
            )

    # Live mode
    if refresh == 1 and _scan_state.get("status") != "scanning":
        asyncio.create_task(_run_breakout_scan())
    elif not _scan_state.get("stocks") and _scan_state.get("status") != "scanning":
        asyncio.create_task(_run_breakout_scan())
    return JSONResponse(_scan_state)


@router.post("/api/breakout/rescan")
async def force_breakout_rescan() -> JSONResponse:
    if _scan_state.get("status") != "scanning":
        asyncio.create_task(_run_breakout_scan())
    return JSONResponse({"status": "scanning", "triggered": True})
