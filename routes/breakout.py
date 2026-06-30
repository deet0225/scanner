"""
routes/breakout.py -- Tab: Breakout Finder
=================================================
Scans Nifty 500 + Microcap 250 (750 tickers) for high-quality chart-pattern
breakouts suitable for swing trading. Each stock passes through four ordered
stages; failure at any stage drops the stock immediately.

Scan pipeline
-------------
Stage 0 — Market Regime Gate (scan-level; aborts entire scan if market is bearish)
  a. Nifty500 daily EMA(20) > EMA(50)
  b. Nifty500 RSI(14) > 50 on at least 2 of the last 3 sessions
  Source: ^CRSLDX (Yahoo Finance), fallback NIFTYBEES.NS / JUNIORBEES.NS

Stage 1 — Fast Daily Gates (per-stock; cheap, runs first)
  1. Price ≥ ₹20
  2. Volume today ≥ 1.6× 20-day average
  3. RSI(14) between 55 and 80
  4. ADX(14) ≥ 22
  5. Close > EMA(20) > EMA(50)
  6. Candle close in top 50% of day's High–Low range

Stage 2 — Weekly Trend Gate (per-stock; confirms multi-week uptrend)
  7. Weekly close > weekly EMA(20)
  8. Weekly RSI(14) ≥ 55

Stage 3 — Pattern Detection (per-stock; expensive, runs last)
  Detects 5 patterns on historical bars (today excluded):
    Cup & Handle · Ascending Triangle · Bull Flag · Flat Base · Rectangle
  Stock qualifies only if today's close ≥ pivot × 1.0025 (≥ 0.25% above pivot).
  Best pattern by quality score is selected when multiple match.

API routes
----------
GET  /api/breakout          — latest breakout results (auto-triggers first run)
POST /api/breakout/rescan   — force fresh breakout scan

API response includes
---------------------
stocks[]        — up to 100 qualifying stocks sorted by composite score
regime_ok       — bool: whether the market regime check passed
regime_summary  — human-readable regime description (EMA / RSI values)

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

import numpy as np
import pandas as pd
import requests
import ta
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from tickers import NIFTY500_TICKERS, NIFTY_MICROCAP250_TICKERS

logger = logging.getLogger(__name__)
router = APIRouter()

# Pattern-based swing trade gates
BK_PIVOT_MIN_PCT   = 0.25   # min % close must be above pattern pivot (was 0.10; filters false-breakout touches)
BK_VOL_RATIO_MIN   = 1.6    # today vol >= 1.6× 20D avg (was 1.4; higher conviction on breakout day)
BK_RSI_MIN         = 55.0   # minimum RSI (was 52; early-momentum zone starts at 55)
BK_RSI_MAX         = 80.0   # maximum RSI (avoid overbought)
BK_ADX_MIN         = 22.0   # minimum ADX (was 18; 22 = trend properly established, not just forming)
BK_MIN_PRICE       = 20.0   # price floor in ₹ (no penny stocks)
BK_CANDLE_BODY_MIN = 0.50   # close in top 50% of day's High-Low range (was 0.40; stronger breakout candle)
BK_WEEKLY_RSI_MIN  = 55.0   # weekly RSI(14) minimum — weekly trend must be bullish
BK_BREAKOUT_VOL_RATIO_MIN = 1.9   # breakout-day volume confirmation near pivot (filters low-energy breaks)
BK_MAX_BREAKOUT_PCT = 7.0         # avoid chasing already-extended breakouts
BK_MAX_EMA20_EXT_PCT = 12.0       # close should not be too stretched above EMA20
BK_MIN_RR_RATIO = 1.6             # minimum reward:risk for swing suitability
BK_SCORE_RAW_MAX = 173.4          # theoretical max of raw composite score before normalization
BK_MIN_SCORE = 65.0               # hide low-quality setups from Breakout tab
BK_MIN_ROWS        = 120    # minimum OHLCV rows required
BK_MAX_WORKERS     = 6


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
    "regime_ok": None,
    "regime_summary": "",
    "criteria": {
        "patterns":        ["Cup & Handle", "Ascending Triangle", "Bull Flag", "Flat Base", "Rectangle"],
        "pivot_min_pct":   BK_PIVOT_MIN_PCT,
        "vol_ratio_min":   BK_VOL_RATIO_MIN,
        "breakout_vol_ratio_min": BK_BREAKOUT_VOL_RATIO_MIN,
        "max_breakout_pct": BK_MAX_BREAKOUT_PCT,
        "rsi_range":       f"{BK_RSI_MIN}\u2013{BK_RSI_MAX}",
        "adx_min":         BK_ADX_MIN,
        "min_price":       BK_MIN_PRICE,
        "max_ema20_ext_pct": BK_MAX_EMA20_EXT_PCT,
        "min_rr_ratio":    BK_MIN_RR_RATIO,
        "min_score":       BK_MIN_SCORE,
        "weekly_rsi_min":  BK_WEEKLY_RSI_MIN,
    },
}


# ───────────────────────────────────────────────────────────────────────────
# Pattern detection helpers
# All take numpy arrays of HISTORICAL data (today’s bar excluded).
# Return: (detected, pivot, quality 0–1, depth_pct, duration_bars)
# ───────────────────────────────────────────────────────────────────────────

def _count_spaced_touches(mask: "np.ndarray", min_gap: int = 3) -> int:
    """Count distinct level touches, ignoring clustered consecutive bars."""
    idx = np.flatnonzero(mask)
    if len(idx) == 0:
        return 0
    count = 1
    last = int(idx[0])
    for i in idx[1:]:
        ii = int(i)
        if ii - last >= min_gap:
            count += 1
            last = ii
    return count

def _det_flat_base(
    h: "np.ndarray", l: "np.ndarray", c: "np.ndarray", v: "np.ndarray",
) -> "tuple[bool, float, float, float, int]":
    """Flat Base: 25-50 bar tight consolidation, today closes above the base high."""
    n = len(c)
    best: "tuple | None" = None
    for blen in (50, 40, 30, 25):
        if n < blen:
            continue
        bh = h[-blen:];  bl = l[-blen:];  bc = c[-blen:];  bv = v[-blen:]
        base_high = float(bh.max())
        base_low  = float(bl.min())
        if base_high <= 0:
            continue
        depth = (base_high - base_low) / base_high
        if depth > 0.12:          # max 12% depth
            continue
        c_mean = float(bc.mean());  c_std = float(bc.std())
        if c_mean <= 0 or c_std / c_mean > 0.038:   # tight closes required
            continue
        v1 = float(bv[:blen // 2].mean());  v2 = float(bv[blen // 2:].mean())
        vol_contr = v1 > 0 and v2 < v1
        quality = (
            (0.12 - depth) / 0.12 * 0.35
            + max(0.0, (0.038 - c_std / c_mean) / 0.038) * 0.40
            + (0.25 if vol_contr else 0.0)
        )
        quality = max(0.0, min(1.0, quality))
        if best is None or quality > best[2]:
            best = (base_high, round(depth * 100, 1), quality, blen)
    if best:
        return True, round(best[0], 2), round(best[2], 3), best[1], best[3]
    return False, 0.0, 0.0, 0.0, 0


def _det_rectangle(
    h: "np.ndarray", l: "np.ndarray", c: "np.ndarray", v: "np.ndarray",
) -> "tuple[bool, float, float, float, int]":
    """Rectangle Breakout: 2+ resistance + 2+ support touches, today above resistance."""
    n = len(c)
    best: "tuple | None" = None
    for rlen in (55, 45, 35, 25, 18):
        if n < rlen:
            continue
        rh = h[-rlen:];  rl = l[-rlen:];  rv = v[-rlen:]
        resistance = float(rh.max());  support = float(rl.min())
        if support <= 0 or resistance <= 0:
            continue
        width = (resistance - support) / resistance
        if width < 0.07 or width > 0.25:
            continue
        rtol = resistance * 0.985;  stol = support * 1.015
        r_touches = _count_spaced_touches(rh >= rtol, min_gap=3)
        s_touches = _count_spaced_touches(rl <= stol, min_gap=3)
        if r_touches < 2 or s_touches < 2:
            continue
        top_h = rh[rh >= rtol]
        if len(top_h) > 1 and float(top_h.std()) / resistance > 0.020:
            continue
        v_pre  = float(v[max(0, n - rlen - 20): n - rlen].mean()) if n > rlen + 5 else float(rv.mean()) * 1.1
        v_rect = float(rv.mean())
        vol_ok = v_pre > 0 and v_rect < v_pre * 0.85
        quality = (
            min(1.0, (r_touches + s_touches) / 6) * 0.35
            + max(0.0, 1.0 - width / 0.25) * 0.40
            + (0.25 if vol_ok else 0.0)
        )
        quality = max(0.0, min(1.0, quality))
        if best is None or quality > best[2]:
            best = (resistance, round(width * 100, 1), quality, rlen)
    if best:
        return True, round(best[0], 2), round(best[2], 3), best[1], best[3]
    return False, 0.0, 0.0, 0.0, 0


def _det_ascending_triangle(
    h: "np.ndarray", l: "np.ndarray", c: "np.ndarray",
) -> "tuple[bool, float, float, float, int]":
    """Ascending Triangle: flat resistance (2+ touches) + rising support, converging."""
    n = len(c)
    best: "tuple | None" = None
    for alen in (55, 45, 35, 25):
        if n < alen:
            continue
        ah = h[-alen:];  al = l[-alen:]
        resistance = float(ah.max())
        if resistance <= 0:
            continue
        rtol = resistance * 0.985
        r_touches = _count_spaced_touches(ah >= rtol, min_gap=3)
        if r_touches < 2:
            continue
        top_h = ah[ah >= rtol]
        res_std_pct = float(top_h.std()) / resistance if len(top_h) > 1 else 0.0
        if res_std_pct > 0.018:
            continue
        x = np.arange(alen, dtype=float)
        coeffs = np.polyfit(x, al, 1)
        slope = float(coeffs[0])
        if slope <= 0:
            continue
        support_0   = float(coeffs[1])
        support_now = support_0 + slope * (alen - 1)
        band_start  = max(0.0001, resistance - support_0)
        convergence = (support_now - support_0) / band_start
        if convergence < 0.08:
            continue
        early_low = float(np.median(al[: max(3, alen // 4)]))
        late_low  = float(np.median(al[-max(3, alen // 4):]))
        if late_low <= early_low:
            continue
        depth = (resistance - float(al.min())) / resistance
        quality = (
            min(1.0, r_touches / 4) * 0.40
            + max(0.0, (0.018 - res_std_pct) / 0.018) * 0.35
            + min(1.0, convergence / 0.4) * 0.25
        )
        quality = max(0.0, min(1.0, quality))
        if best is None or quality > best[2]:
            best = (resistance, round(depth * 100, 1), quality, alen)
    if best:
        return True, round(best[0], 2), round(best[2], 3), best[1], best[3]
    return False, 0.0, 0.0, 0.0, 0


def _det_bull_flag(
    h: "np.ndarray", l: "np.ndarray", c: "np.ndarray", v: "np.ndarray",
) -> "tuple[bool, float, float, float, int]":
    """Bull Flag: sharp pole (8%+ in ≤13 bars) + tight sideways flag + breakout."""
    n = len(c)
    best: "tuple | None" = None
    for flen in (5, 7, 10, 14, 18):
        if n < flen + 8:
            continue
        fh = h[-flen:];  fl = l[-flen:];  fc = c[-flen:];  fv = v[-flen:]
        flag_high = float(fh.max());  flag_low = float(fl.min())
        if flag_high <= 0:
            continue
        flag_range = (flag_high - flag_low) / flag_high
        if flag_range > 0.09:
            continue
        x = np.arange(flen, dtype=float)
        flag_slope = float(np.polyfit(x, fc, 1)[0]) / flag_high
        if flag_slope > 0.004:   # flag must not be sharply rising
            continue
        if flag_slope < -0.006:  # overly steep downward flags are often failed reversals
            continue
        for plen in (5, 7, 10, 13):
            ps = n - flen - plen
            if ps < 3:
                continue
            ph = h[ps: n - flen];  pl_arr = l[ps: n - flen]
            pole_high  = float(ph.max());  pole_low_v = float(pl_arr.min())
            if pole_low_v <= 0:
                continue
            pole_move = (pole_high - pole_low_v) / pole_low_v
            if pole_move < 0.08:
                continue
            if int(np.argmax(ph)) < plen // 3:   # peak must be in latter half of pole
                continue
            retrace_abs = pole_high - flag_low
            if pole_high > pole_low_v and retrace_abs > (pole_high - pole_low_v) * 0.55:
                continue
            pv_avg = float(v[ps: n - flen].mean())
            fv_avg = float(fv.mean())
            vol_dry = pv_avg > 0 and fv_avg < pv_avg * 0.75
            quality = (
                min(1.0, pole_move / 0.15) * 0.35
                + (0.09 - min(0.09, flag_range)) / 0.09 * 0.30
                + (0.20 if vol_dry else 0.0)
                + min(1.0, flen / 12) * 0.15
            )
            quality = max(0.0, min(1.0, quality))
            if best is None or quality > best[2]:
                best = (flag_high, round(flag_range * 100, 1), quality, flen)
    if best:
        return True, round(best[0], 2), round(best[2], 3), best[1], best[3]
    return False, 0.0, 0.0, 0.0, 0


def _det_cup_and_handle(
    h: "np.ndarray", l: "np.ndarray", c: "np.ndarray", v: "np.ndarray",
) -> "tuple[bool, float, float, float, int]":
    """Cup & Handle: U-shaped cup (10-45% deep) + handle pullback (3-20%), today at pivot."""
    n = len(c)
    if n < 55:
        return False, 0.0, 0.0, 0.0, 0
    best: "tuple | None" = None
    for hlen in (5, 8, 12, 16, 20):
        for clen in (40, 60, 80, 100):
            total = clen + hlen
            if total >= n:
                continue
            cs = n - total;  ce = cs + clen;  he = ce + hlen
            if he > n:
                continue
            ch = h[cs:ce];  cl = l[cs:ce];  cc = c[cs:ce];  cv = v[cs:ce]
            hh = h[ce:he];  hl = l[ce:he];  hv = v[ce:he]
            q  = max(1, clen // 4)
            lr = float(ch[:q].max());  rr = float(ch[3 * q:].max())
            cup_bot = float(cl.min())
            if lr <= 0 or rr <= 0 or cup_bot <= 0:
                continue
            if rr < lr * 0.97:      # right rim must match left rim
                continue
            depth = (lr - cup_bot) / lr
            if depth < 0.10 or depth > 0.45:
                continue
            t = max(2, clen // 3)
            outer_avg = (float(cc[:t].mean()) + float(cc[2 * t:].mean())) / 2
            mid_avg   = float(cc[t: 2 * t].mean())
            if mid_avg > outer_avg * 0.97:    # middle must be lower (U-shape)
                continue
            handle_high = float(hh.max());  handle_low = float(hl.min())
            cup_mid = cup_bot + (lr - cup_bot) * 0.5
            if handle_low < cup_mid:          # handle must stay in upper half
                continue
            retrace = (rr - handle_low) / rr
            if retrace < 0.03 or retrace > 0.20:
                continue
            hx = np.arange(hlen, dtype=float)
            handle_slope = float(np.polyfit(hx, h[ce:he], 1)[0]) / max(rr, 0.0001)
            if handle_slope > 0.003:  # handle should drift sideways/down, not trend up
                continue
            cup_vol_avg    = float(cv.mean())
            handle_vol_avg = float(hv.mean())
            vol_dry = cup_vol_avg > 0 and handle_vol_avg < cup_vol_avg * 0.85
            pivot   = max(rr, handle_high)
            quality = (
                (1.0 - abs(depth - 0.25) / 0.25) * 0.25
                + min(1.0, rr / lr) * 0.25
                + max(0.0, (0.20 - retrace) / 0.20) * 0.20
                + (0.15 if vol_dry else 0.0)
                + min(1.0, clen / 80) * 0.15
            )
            quality = max(0.0, min(1.0, quality))
            if best is None or quality > best[2]:
                best = (pivot, round(depth * 100, 1), quality, clen + hlen)
    if best:
        return True, round(best[0], 2), round(best[2], 3), best[1], best[3]
    return False, 0.0, 0.0, 0.0, 0


def _calc_score(
    vol_ratio: float, rsi: float, adx: float,
    is_52w_break: bool, candle_body_pct: float, pct_from_52w: float,
    ema200_gap_pct: float, pattern_quality: float, breakout_pct: float,
) -> float:
    """Composite score normalized to a 0-100 range for swing-trade setup quality."""
    raw_score = 0.0
    # 1. Volume surge (max 40)
    raw_score += min(max(vol_ratio - 1.0, 0.0), 4.0) * 10.0
    # 2. 52-week high (max 30)
    if is_52w_break:
        raw_score += 30.0
    else:
        raw_score += max(0.0, 10.0 + pct_from_52w * 1.5)
    # 3. RSI sweet spot 60-72 (max ~20)
    rsi_pts = max(0.0, rsi - 55.0) * 1.2
    if rsi > 72:
        rsi_pts -= (rsi - 72) * 2.5
    raw_score += max(0.0, rsi_pts)
    # 4. ADX trend strength (max 32)
    raw_score += min(max(adx - 18.0, 0.0), 32.0) * 1.0
    # 5. Candle body quality (max 10)
    raw_score += candle_body_pct * 10.0
    # 6. Pattern quality (max 25)
    raw_score += pattern_quality * 25.0
    # 7. Breakout margin above pivot (max 10)
    raw_score += min(max(breakout_pct, 0.0), 5.0) * 2.0
    # 8. EMA200 alignment (max 6)
    if ema200_gap_pct > 0:
        raw_score += min(ema200_gap_pct, 15.0) * 0.4

    if BK_SCORE_RAW_MAX <= 0:
        return 0.0
    normalized = (raw_score / BK_SCORE_RAW_MAX) * 100.0
    normalized = max(0.0, min(100.0, normalized))
    return round(normalized, 2)


def _check_bk_regime(from_date: str, to_date: str) -> "tuple[bool, str]":
    """
    Check Nifty500 market regime before running the breakout scan.

    Two conditions — both must pass:
      a. Nifty500 daily EMA(20) > EMA(50)              — medium-term uptrend
      b. Nifty500 RSI(14) > 50 on at least 2/3 recent sessions  — momentum healthy

    Tries ^CRSLDX (Yahoo Finance index), falls back to NIFTYBEES.NS ETF.
    If data is unavailable, regime is assumed OK so the scan proceeds.

    Returns (regime_ok: bool, regime_summary: str).
    """
    _BENCH_SYMS = ["^CRSLDX", "NIFTYBEES.NS", "JUNIORBEES.NS"]
    bench: "pd.DataFrame | None" = None
    used_sym = ""
    for sym in _BENCH_SYMS:
        try:
            p1  = int(datetime.fromisoformat(from_date + "T00:00:00").timestamp())
            p2  = int(datetime.fromisoformat(to_date   + "T23:59:59").timestamp())
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
            r   = _yf_session.get(
                url, params={"period1": p1, "period2": p2, "interval": "1d"}, timeout=18,
            )
            if r.status_code != 200:
                continue
            body   = r.json()
            result = (body.get("chart") or {}).get("result") or []
            if not result:
                continue
            ts_list = result[0].get("timestamp") or []
            quote   = result[0].get("indicators", {}).get("quote", [{}])[0]
            adj_cl  = result[0].get("indicators", {}).get("adjclose", [{}])
            closes  = (adj_cl[0].get("adjclose") if adj_cl else None) or quote.get("close", [])
            records = []
            for i, ts in enumerate(ts_list):
                try:
                    c = float((closes or [])[i] or 0)
                    if c > 0:
                        records.append({"Date": pd.Timestamp(ts, unit="s"), "Close": c})
                except (IndexError, TypeError, ValueError):
                    pass
            if len(records) >= 60:
                bench   = pd.DataFrame(records).sort_values("Date").reset_index(drop=True)
                used_sym = sym
                logger.info("Regime benchmark %s: %d rows", sym, len(records))
                break
        except Exception as exc:
            logger.debug("Regime fetch %s: %s", sym, exc)

    if bench is None or len(bench) < 60:
        logger.warning("Regime check skipped — benchmark data unavailable; scan proceeds.")
        return True, "Regime check skipped (no benchmark data)"

    bc    = bench["Close"].astype(float)
    ema20 = float(bc.ewm(span=20, adjust=False).mean().iloc[-1])
    ema50 = float(bc.ewm(span=50, adjust=False).mean().iloc[-1])
    rsi_s = ta.momentum.RSIIndicator(close=bc, window=14, fillna=False).rsi()
    last3 = rsi_s.iloc[-3:]
    days_above_50 = int((last3 > 50).sum())

    ema_ok = ema20 > ema50
    rsi_ok = days_above_50 >= 2

    summary = (
        f"EMA20={ema20:.0f} {'>' if ema_ok else '<='} EMA50={ema50:.0f} | "
        f"RSI>50 on {days_above_50}/3 recent sessions"
    )

    if not ema_ok:
        logger.info("Regime FAIL (%s): EMA20 %.2f <= EMA50 %.2f", used_sym, ema20, ema50)
        return False, f"Market in downtrend \u2014 {summary}"
    if not rsi_ok:
        logger.info("Regime FAIL (%s): RSI>50 only %d/3 days", used_sym, days_above_50)
        return False, f"Market momentum weak \u2014 {summary}"

    logger.info("Regime OK (%s): %s", used_sym, summary)
    return True, f"Bullish \u2014 {summary}"


def _analyze_symbol(ticker: str, from_index: str, from_date: str, to_date: str,
                    live_quote: "dict | None" = None,
                    target_date: "_date_type | None" = None) -> "dict | None":
    symbol = ticker.replace(".NS", "").upper().strip()

    df = _fetch_ohlcv(symbol, from_date, to_date)
    if len(df) < BK_MIN_ROWS:
        return None

    # ── Live candle patch (live mode only) ────────────────────────────────
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

    close = df["Close"].astype(float)
    high  = df["High"].astype(float)
    low   = df["Low"].astype(float)
    vol   = df["Volume"].astype(float)

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

    if len(close) < 55:
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

    avg_vol20   = float(vol.iloc[-21:-1].mean()) if len(vol) > 21 else 0.0
    n52         = min(252, len(high) - 1)
    high_52w    = float(high.iloc[-(n52 + 1):-1].max()) if n52 > 0 else last_close
    prev_low_50 = float(low.iloc[-51:-1].min()) if len(low) > 51 else float(low.iloc[:-1].min())

    if last_close <= 0 or avg_vol20 <= 0:
        return None

    # ── Fast quality gates (cheap — run before expensive pattern detection) ──
    if last_close < BK_MIN_PRICE:
        return None
    vol_ratio       = last_vol / avg_vol20
    candle_range    = last_high - last_low
    candle_body_pct = (last_close - last_low) / candle_range if candle_range > 0.01 else 0.5
    ema20_ext_pct   = ((last_close / last_ema20) - 1.0) * 100.0 if last_ema20 > 0 else 0.0
    if vol_ratio < BK_VOL_RATIO_MIN:
        return None
    if not (BK_RSI_MIN <= last_rsi <= BK_RSI_MAX):
        return None
    if last_adx < BK_ADX_MIN:
        return None
    if not (last_close > last_ema20 > last_ema50):
        return None
    if ema20_ext_pct > BK_MAX_EMA20_EXT_PCT:
        return None
    if candle_body_pct < BK_CANDLE_BODY_MIN:
        return None
    if last_close < float(df["Open"].iloc[-1]):  # avoid red breakout candles
        return None

    # ── Weekly trend gate ──────────────────────────────────────────────────
    # Requires: weekly close > weekly EMA20  AND  weekly RSI(14) > BK_WEEKLY_RSI_MIN
    # Ensures the stock is in a genuine weekly uptrend — not just a one-day daily spike.
    _df_w = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(_df_w["Date"]):
        _df_w["Date"] = pd.to_datetime(_df_w["Date"])
    _df_w = _df_w.set_index("Date")
    weekly = (
        _df_w.resample("W-FRI")
             .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"})
             .dropna(subset=["Close"])
    )
    if len(weekly) >= 20:
        w_close_s = weekly["Close"].astype(float)
        w_ema20   = w_close_s.ewm(span=20, adjust=False, min_periods=20).mean()
        w_e20_val = float(w_ema20.iloc[-1])
        w_cls_val = float(w_close_s.iloc[-1])
        if not pd.isna(w_e20_val) and w_cls_val <= w_e20_val:
            return None   # weekly close below weekly EMA20 — not in uptrend
        w_rsi_s   = ta.momentum.RSIIndicator(close=w_close_s, window=14, fillna=False).rsi()
        w_rsi_val = float(w_rsi_s.iloc[-1]) if pd.notna(w_rsi_s.iloc[-1]) else 0.0
        if w_rsi_val < BK_WEEKLY_RSI_MIN:
            return None   # weekly momentum insufficient

    # ── Pattern detection (expensive — runs only when all fast gates pass) ──
    h_hist = high.values[:-1]
    l_hist = low.values[:-1]
    c_hist = close.values[:-1]
    v_hist = vol.values[:-1]
    pivot_threshold = 1.0 + BK_PIVOT_MIN_PCT / 100.0

    patterns: "list[tuple]" = []
    _PATTERN_FNS = [
        ("Cup & Handle",       lambda: _det_cup_and_handle(h_hist, l_hist, c_hist, v_hist)),
        ("Ascending Triangle", lambda: _det_ascending_triangle(h_hist, l_hist, c_hist)),
        ("Bull Flag",          lambda: _det_bull_flag(h_hist, l_hist, c_hist, v_hist)),
        ("Flat Base",          lambda: _det_flat_base(h_hist, l_hist, c_hist, v_hist)),
        ("Rectangle",          lambda: _det_rectangle(h_hist, l_hist, c_hist, v_hist)),
    ]
    for pname, pfn in _PATTERN_FNS:
        try:
            det, pivot, qual, depth_pct, dur = pfn()
            if det and pivot > 0 and last_close >= pivot * pivot_threshold:
                patterns.append((pname, pivot, qual, depth_pct, dur))
        except Exception:
            pass

    if not patterns:
        return None

    # Best pattern by quality score
    patterns.sort(key=lambda x: -x[2])
    pattern_name, pivot, pattern_quality, pattern_depth_pct, pattern_duration = patterns[0]

    breakout_pct   = ((last_close / pivot) - 1.0) * 100.0
    if vol_ratio < BK_BREAKOUT_VOL_RATIO_MIN:
        return None
    if breakout_pct > BK_MAX_BREAKOUT_PCT:
        return None
    pct_from_52w   = ((last_close / high_52w) - 1.0) * 100.0 if high_52w > 0 else -999.0
    is_52w_break   = pct_from_52w >= -0.5
    ema200_gap_pct = ((last_ema20 / last_ema200) - 1.0) * 100.0 if last_ema200 > 0 else 0.0

    # ── ATR-based stop loss ───────────────────────────────────────────
    atr_stop  = (last_low - 0.5 * last_atr) if last_atr > 0 else last_close * 0.95
    ema_stop  = last_ema20 * 0.98
    stop_loss = max(atr_stop, ema_stop, prev_low_50 * 0.99)
    stop_loss = max(stop_loss, last_close * 0.90)

    risk_amt = last_close - stop_loss
    risk_pct = (risk_amt / last_close) * 100.0 if last_close > 0 else None

    # Pattern-depth projection: pivot + pattern_height gives a measured-move target.
    # This is more accurate for swing trades than a flat ATR multiple because it
    # reflects the actual energy stored in the pattern (bigger pattern → bigger move).
    # We also compute an ATR-based target and take the higher of the two.
    pattern_target = pivot * (1.0 + pattern_depth_pct / 100.0) if pattern_depth_pct > 0 else None
    atr_target     = (last_close + 2.5 * last_atr) if last_atr > 0 else None
    candidates     = [t for t in (pattern_target, atr_target) if t is not None and t > last_close]
    target_price   = max(candidates) if candidates else None
    rr_ratio       = ((target_price - last_close) / risk_amt
                      if (target_price and risk_amt > 0) else None)
    if rr_ratio is None or rr_ratio < BK_MIN_RR_RATIO:
        return None

    score = _calc_score(
        vol_ratio, last_rsi, last_adx,
        is_52w_break, candle_body_pct, pct_from_52w,
        ema200_gap_pct, pattern_quality, breakout_pct,
    )

    return {
        "ticker":            f"{symbol}.NS",
        "display_ticker":    symbol,
        "from_index":        from_index,
        "pattern":           pattern_name,
        "pattern_quality":   round(pattern_quality, 3),
        "pattern_depth_pct": round(pattern_depth_pct, 1),
        "pattern_days":      pattern_duration,
        "price":             round(last_close, 2),
        "breakout_pct":      round(breakout_pct, 2),
        "vol_ratio":         round(vol_ratio, 2),
        "rsi":               round(last_rsi, 2),
        "adx":               round(last_adx, 2),
        "ema20":             round(last_ema20, 2),
        "ema50":             round(last_ema50, 2),
        "ema200":            round(last_ema200, 2),
        "atr":               round(last_atr, 2),
        "stop_loss":         round(stop_loss, 2),
        "risk_pct":          round(risk_pct, 2) if risk_pct is not None else None,
        "target":            round(target_price, 2) if target_price else None,
        "rr_ratio":          round(rr_ratio, 2) if rr_ratio is not None else None,
        "is_52w_break":      is_52w_break,
        "pct_from_52w":      round(pct_from_52w, 2),
        "score":             score,
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

    # ── Market regime gate ────────────────────────────────────────────────
    # Abort scan early when Nifty500 is in a downtrend — pattern breakouts
    # in a bearish market have a very high failure rate.
    regime_ok, regime_summary = _check_bk_regime(from_date_s, to_date_s)
    if not regime_ok:
        logger.warning("Breakout scan aborted — bearish market regime: %s", regime_summary)
        return {
            "stocks": [], "total": 0, "n500_count": 0, "mc_count": 0,
            "source":         "Zerodha" if _zerodha.enabled else "yfinance",
            "regime_ok":      False,
            "regime_summary": regime_summary,
            "error":          None,
        }

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
    all_rows = [row for row in all_rows if float(row.get("score", 0.0)) >= BK_MIN_SCORE]
    return {
        "stocks":         all_rows[:100],
        "total":          len(all_rows),
        "n500_count":     len(n500),
        "mc_count":       len(mc),
        "source":         source,
        "regime_ok":      True,
        "regime_summary": regime_summary,
        "error":          None,
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
                "stocks":         payload.get("stocks") or [],
                "total":          payload.get("total") or 0,
                "n500_count":     payload.get("n500_count") or 0,
                "mc_count":       payload.get("mc_count") or 0,
                "error":          payload.get("error"),
                "status":         "error" if payload.get("error") else "complete",
                "scan_stage":     "Ready" if not payload.get("error") else "Configuration required",
                "last_updated":   now_ist,
                "scan_count":     _scan_state.get("scan_count", 0) + 1,
                "regime_ok":      payload.get("regime_ok"),
                "regime_summary": payload.get("regime_summary", ""),
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
