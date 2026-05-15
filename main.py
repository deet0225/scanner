"""
main.py — FastAPI application for Nifty 500 Stock Scanner
"""

# SSL bypass for corporate proxy (requests.Session.verify=False is set in scanner.py;
# these env vars cover any other http calls in the process)
import os, ssl, warnings
os.environ.setdefault("PYTHONHTTPSVERIFY", "0")
try:
    ssl._create_default_https_context = ssl._create_unverified_context  # type: ignore[attr-defined]
except Exception:
    pass
warnings.filterwarnings("ignore")
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass

import asyncio
import json
import logging
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, date as DateType

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

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
    MICROCAP_BENCHMARK_TICKER, MICROCAP_BENCHMARK_ETF_FALLBACKS,
)
from scanner import StockScanner
from tickers import NIFTY500_TICKERS, NIFTY_MICROCAP250_TICKERS
import cache as _ohlcv_cache

# ── Logging ──────────────────────────────────────────────────────────────────
# Force UTF-8 on Windows console so log messages render correctly
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

_log_handler = logging.StreamHandler(stream=sys.stdout)
_log_handler.stream = open(
    sys.stdout.fileno(), mode="w", encoding="utf-8",
    errors="replace", buffering=1, closefd=False,
)
_log_handler.setFormatter(logging.Formatter(
    fmt="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
))
logging.basicConfig(level=logging.INFO, handlers=[_log_handler], force=True)

# Route uvicorn's own loggers through the same root handler.
for _name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
    _ul = logging.getLogger(_name)
    _ul.handlers.clear()
    _ul.propagate = True

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

scanner = StockScanner(
    tickers=NIFTY500_TICKERS,
    benchmark_ticker=None,
    benchmark_etf_fallbacks=None,
    label="Nifty500",
)
scanner_mc = StockScanner(
    tickers=NIFTY_MICROCAP250_TICKERS,
    benchmark_ticker=MICROCAP_BENCHMARK_TICKER,
    benchmark_etf_fallbacks=MICROCAP_BENCHMARK_ETF_FALLBACKS,
    label="NiftyMicrocap250",
)
scheduler = AsyncIOScheduler()

# ── Shared state ─────────────────────────────────────────────────────────────
scan_state: dict = {
    "data": [],
    "last_updated": None,
    "status": "initializing",
    "scan_count": 0,
    "filters_passed": 0,
    "next_scan_ts": None,
    "error": None,
    "total_tickers": len(scanner.tickers),
    "scan_stage": "",
    "regime_ok": True,
    "regime_summary": "",
}

mc_scan_state: dict = {
    "data": [],
    "last_updated": None,
    "status": "idle",
    "scan_count": 0,
    "filters_passed": 0,
    "next_scan_ts": None,
    "error": None,
    "total_tickers": len(scanner_mc.tickers),
    "scan_stage": "",
    "regime_ok": True,
    "regime_summary": "",
}

mc_scan_ever_triggered: bool = False
n500_tab_active: bool = True
_scan_lock: asyncio.Lock = asyncio.Lock()


# ── Generic scan body ─────────────────────────────────────────────────────────
async def _do_run_generic_scan(sc: StockScanner, state: dict, label: str) -> None:
    """Inner scan body — must be called while _scan_lock is held.
    Works for both Nifty500 and Microcap250 scanners."""
    def _progress(stage: str):
        state["scan_stage"] = stage

    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, lambda: sc.scan(progress_cb=_progress))

        now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
        state.update({
            "data":           results,
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
        logger.info("=== %s Scan #%d complete -- %d qualifying stocks ===",
                    label, state["scan_count"], len(results))

    except Exception as exc:
        logger.error("%s scan failed: %s", label, exc, exc_info=True)
        state["status"] = "error"
        state["error"]  = str(exc)
        state["next_scan_ts"] = (time.time() + SCAN_INTERVAL_MINUTES * 60) if AUTO_RESCAN else None


# ── Scan tasks ───────────────────────────────────────────────────────────────
async def run_scan() -> None:
    global scan_state
    if _scan_lock.locked():
        logger.info("Nifty500 scan queued -- waiting for Microcap250 scan to finish...")
        scan_state["scan_stage"] = "Queued (waiting for Microcap250 scan to finish)..."
    async with _scan_lock:
        scan_state["status"]     = "scanning"
        scan_state["scan_stage"] = "Downloading OHLCV data..."
        scan_state["next_scan_ts"] = None
        logger.info("=== Nifty500 scan starting ===")
        await _do_run_generic_scan(scanner, scan_state, "Nifty500")


async def run_mc_scan() -> None:
    global mc_scan_state, mc_scan_ever_triggered
    mc_scan_ever_triggered = True
    if _scan_lock.locked():
        logger.info("Microcap250 scan queued -- waiting for Nifty500 scan to finish...")
        mc_scan_state["scan_stage"] = "Queued (waiting for Nifty500 scan to finish)..."
        mc_scan_state["status"] = "scanning"
    async with _scan_lock:
        mc_scan_state["status"]     = "scanning"
        mc_scan_state["scan_stage"] = "Downloading OHLCV data..."
        mc_scan_state["next_scan_ts"] = None
        logger.info("=== Microcap250 scan starting ===")
        await _do_run_generic_scan(scanner_mc, mc_scan_state, "Microcap250")


async def _maybe_run_n500_scan() -> None:
    """Scheduler wrapper for periodic Nifty500 rescan (AUTO_RESCAN=True only)."""
    if n500_tab_active and scan_state["status"] not in ("scanning",):
        await run_scan()


async def _maybe_run_mc_scan() -> None:
    """Scheduler wrapper for periodic Microcap rescan (AUTO_RESCAN=True only)."""
    if mc_scan_state["status"] not in ("scanning",):
        await run_mc_scan()


# ── Lifespan ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    if AUTO_RESCAN:
        scheduler.add_job(_maybe_run_n500_scan, "interval", minutes=SCAN_INTERVAL_MINUTES,     id="stock_scan")
        scheduler.add_job(_maybe_run_mc_scan,   "interval", minutes=SCAN_INTERVAL_MINUTES + 1, id="mc_scan")
        scheduler.start()
        logger.info("Auto-rescan ENABLED: Nifty500 every %d min, Microcap250 every %d min",
                    SCAN_INTERVAL_MINUTES, SCAN_INTERVAL_MINUTES + 1)
        asyncio.create_task(run_scan())
        asyncio.create_task(run_mc_scan())
    else:
        logger.info("Auto-rescan DISABLED: Nifty500 runs once on startup; "
                    "Microcap250 runs on first tab visit only.")
        asyncio.create_task(run_scan())
    yield
    if AUTO_RESCAN:
        scheduler.shutdown(wait=False)


app = FastAPI(title="Nifty Stock Scanner", lifespan=lifespan)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _validate_date_param(date: str):
    """Parse and validate a YYYY-MM-DD date string.
    Returns (DateType, None) on success, or (None, JSONResponse) on error."""
    try:
        target = DateType.fromisoformat(date)
    except ValueError:
        return None, JSONResponse({"error": "Invalid date format — use YYYY-MM-DD"}, status_code=400)
    today = DateType.today()
    if target > today:
        return None, JSONResponse({"error": "Date cannot be in the future"}, status_code=400)
    min_date = today.replace(year=today.year - 3)
    if target < min_date:
        return None, JSONResponse({"error": f"Date cannot be earlier than {min_date}"}, status_code=400)
    return target, None


def _make_sse_generator(state: dict):
    """Return an async generator that streams SSE events from a scan state dict."""
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
                "server_time":    datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
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
                    "status":      state["status"],
                    "server_time": datetime.now(IST).strftime("%H:%M:%S IST"),
                    "next_scan_in": nsi,
                }
                yield f"event: heartbeat\ndata: {json.dumps(hb)}\n\n"
            await asyncio.sleep(3)
    return event_generator


_SSE_HEADERS = {"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"}


# ── Routes ───────────────────────────────────────────────────────────────────
@app.get("/api/config")
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


@app.post("/api/rescan")
async def force_n500_rescan() -> JSONResponse:
    """Force a fresh Nifty500 scan immediately."""
    global n500_tab_active
    n500_tab_active = True
    asyncio.create_task(run_scan())
    return JSONResponse({"triggered": True, "status": "scanning"})


@app.post("/api/microcap/rescan")
async def force_mc_rescan() -> JSONResponse:
    """Force a fresh Microcap250 scan immediately."""
    global n500_tab_active
    n500_tab_active = False
    asyncio.create_task(run_mc_scan())
    return JSONResponse({"triggered": True, "status": "scanning"})


@app.get("/api/cache/stats")
async def cache_stats() -> JSONResponse:
    return JSONResponse(_ohlcv_cache.stats())


@app.post("/api/cache/clear")
async def cache_clear() -> JSONResponse:
    n = _ohlcv_cache.clear()
    logger.info("OHLCV cache cleared: %d files deleted", n)
    return JSONResponse({"deleted": n, "message": f"{n} cache files removed"})


@app.get("/api/results")
async def get_results() -> JSONResponse:
    return JSONResponse(scan_state)


@app.get("/api/microcap/results")
async def get_mc_results() -> JSONResponse:
    return JSONResponse(mc_scan_state)


@app.post("/api/trigger")
async def trigger_n500_scan() -> JSONResponse:
    global n500_tab_active
    n500_tab_active = True
    if AUTO_RESCAN and scan_state["status"] not in ("scanning",):
        asyncio.create_task(run_scan())
        return JSONResponse({"triggered": True, "status": "scanning"})
    return JSONResponse({"triggered": False, "status": scan_state["status"]})


@app.post("/api/microcap/trigger")
async def trigger_mc_scan() -> JSONResponse:
    global mc_scan_ever_triggered, n500_tab_active
    n500_tab_active = False
    mc_scan_ever_triggered = True

    status = mc_scan_state["status"]
    should_trigger = (
        status == "idle"
        or (AUTO_RESCAN and status not in ("scanning",))
    )
    if should_trigger:
        asyncio.create_task(run_mc_scan())
        return JSONResponse({"triggered": True, "status": "scanning"})
    return JSONResponse({"triggered": False, "status": status})


@app.get("/api/stream")
async def stream_results() -> StreamingResponse:
    """SSE stream for Nifty 500."""
    return StreamingResponse(_make_sse_generator(scan_state)(),
                             media_type="text/event-stream", headers=_SSE_HEADERS)


@app.get("/api/microcap/stream")
async def stream_mc_results() -> StreamingResponse:
    """SSE stream for Nifty Microcap 250."""
    return StreamingResponse(_make_sse_generator(mc_scan_state)(),
                             media_type="text/event-stream", headers=_SSE_HEADERS)


@app.get("/api/stock/{ticker}")
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
            None, lambda: scanner.analyze_single(ticker, target_date=target_date)
        )
        if target_date:
            result["as_of_date"] = date
        return JSONResponse(result)
    except Exception as exc:
        logger.error("Stock analysis failed for %s: %s", ticker, exc, exc_info=True)
        return JSONResponse({"error": str(exc), "ticker": ticker}, status_code=500)


# ── NSE / BSE Sector Index definitions ───────────────────────────────────────
# Each entry: (display_name, primary_yf_ticker, [etf_fallback_tickers])
# Tickers that fail to download are silently skipped — no error, just absent
# from the heat map. This makes it safe to include many candidates.
_NSE_SECTOR_INDICES = [
    # ── Broad NSE sectors (original set) ─────────────────────────────────────
    ("Nifty Bank",            "^NSEBANK",       ["BANKBEES.NS",    "BANKNIFTY.NS"]),
    ("Nifty IT",              "^CNXIT",         ["ITBEES.NS",      "ITETF.NS"]),
    ("Nifty Auto",            "^CNXAUTO",       ["AUTOBEES.NS"]),
    ("Nifty Pharma",          "^CNXPHARMA",     ["PHARMABEES.NS"]),
    ("Nifty FMCG",            "^CNXFMCG",       ["FMCGIETF.NS"]),
    ("Nifty Metal",           "^CNXMETAL",      ["METALBEES.NS",   "METAL.NS"]),
    ("Nifty Realty",          "^CNXREALTY",     ["REALTYBEES.NS"]),
    ("Nifty Energy",          "^CNXENERGY",     ["ENERGYBEES.NS",  "ENERGIETF.NS"]),
    ("Nifty PSU Bank",        "^CNXPSUBANK",    ["PSUBNKBEES.NS"]),
    ("Nifty Healthcare",      "^CNXHEALTH",     ["HEALTHIETF.NS"]),
    ("Nifty Financial Svc",   "^CNXFIN",        ["FINIETF.NS"]),
    ("Nifty Media",           "^CNXMEDIA",      []),
    ("Nifty Infra",           "^CNXINFRA",      ["INFRABEES.NS",   "INFRAIETF.NS"]),
    ("Nifty Oil & Gas",       "^CNXOIL",        ["OILIETF.NS"]),
    ("Nifty Consumer Dur",    "^CNXCONSUMER",   ["CONSUMBEES.NS"]),
    ("Nifty Commodities",     "^CNXCMDT",       []),

    # ── Granular NSE sub-sectors ──────────────────────────────────────────────
    ("Nifty Private Bank",    "^CNXPVTBANK",    ["PVTBANKETF.NS",  "PBANKETF.NS"]),
    ("Nifty Capital Goods",   "^CNXCAPGOODS",   ["CAPGOODS.NS"]),
    ("Nifty Defence",         "^CNXDEFENCE",    ["DEFENIETF.NS",   "MAFDEF.NS"]),
    ("Nifty Mfg",             "^CNXMFG",        ["MFGETF.NS"]),
    ("Nifty MNC",             "^CNXMNC",        []),
    ("Nifty CPSE",            "^CNXCPSE",       ["CPSE.NS"]),
    ("Nifty PSE",             "^CNXPSE",        []),
    ("Nifty Services",        "^CNXSERVICE",    []),
    ("Nifty Midcap 100",      "^CNXMIDCAP",     ["MID150BEES.NS",  "NIFMID50.NS"]),
    ("Nifty Smallcap 100",    "^CNXSC",         ["NIFTYSML.NS",    "SMALLCAP.NS"]),
    ("Nifty India Digital",   "^CNXDIGITALIA",  ["DIGIT.NS"]),
    ("Nifty Chemicals",       "^CNXCHEM",       []),

    # ── BSE Sector Indices ────────────────────────────────────────────────────
    ("BSE Bankex",            "^BSEBANKEX",     ["BANKBEES.NS"]),
    ("BSE Power",             "^BSEPOW",        ["POWERIETF.NS",   "POWERBEES.NS"]),
    ("BSE Capital Goods",     "^BSECPGS",       ["CAPGOODS.NS"]),
    ("BSE Teck",              "^BSETECK",       ["ITBEES.NS"]),
    ("BSE Consumer Dur",      "^BSECD",         ["CONSUMBEES.NS"]),
    ("BSE FMCG",              "^BSEFMCG",       ["FMCGIETF.NS"]),
    ("BSE Healthcare",        "^BSEHC",         ["HEALTHIETF.NS"]),
    ("BSE Oil & Gas",         "^BSEOIL",        ["OILIETF.NS"]),
    ("BSE Realty",            "^BSERET",        ["REALTYBEES.NS"]),
    ("BSE Metal",             "^BSEMET",        ["METALBEES.NS"]),
    ("BSE Auto",              "^BSEAUTO",       ["AUTOBEES.NS"]),
    ("BSE IT",                "^BSEIT",         ["ITBEES.NS"]),
]

# In-memory cache (short TTL — disk cache handles real freshness)
_sec_mom_cache: dict = {"data": None, "ts": 0.0, "ttl": 300}


def _compute_sector_momentum() -> dict:
    """Download NSE & BSE sector index OHLCV (~44 indices), compute momentum,
    with disk-cache strategy. Indices that fail to download are skipped silently."""
    import pandas as _pd
    from config import (MARKET_BENCHMARK_TICKER, MARKET_BENCHMARK_ETF_FALLBACKS,
                        CACHE_UPDATE_DAYS)

    FULL_DAYS = 100  # slightly more history for the larger index set

    def _cached_fetch(name: str, primary: str, fallbacks: list):
        cached = _ohlcv_cache.load(primary)
        if _ohlcv_cache.is_fresh(cached):
            logger.debug("Sector cache HIT (fresh): %s", name)
            return cached
        if cached is not None:
            logger.debug("Sector cache STALE -- incremental update: %s", name)
            df_new = scanner._fetch_index(primary, days=CACHE_UPDATE_DAYS, etf_fallbacks=fallbacks)
            if df_new is not None and not df_new.empty:
                merged = _ohlcv_cache.merge(cached, df_new, max_rows=FULL_DAYS + 60)
                _ohlcv_cache.save(primary, merged)
                return merged
            logger.debug("Sector incremental failed for %s -- serving stale cache", name)
            return cached
        logger.debug("Sector cache MISS -- full download: %s", name)
        df = scanner._fetch_index(primary, days=FULL_DAYS, etf_fallbacks=fallbacks)
        if df is not None and not df.empty:
            _ohlcv_cache.save(primary, df)
        return df

    bench_df = _cached_fetch("Nifty500 Benchmark",
                             MARKET_BENCHMARK_TICKER, MARKET_BENCHMARK_ETF_FALLBACKS)
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
            if df is None or len(df) < 25:
                continue
            c = df["Close"].dropna()
            if len(c) < 25:
                continue

            ret_5d  = round(float((c.iloc[-1] / c.iloc[-6]  - 1) * 100), 2) if len(c) >= 6  else 0.0
            ret_20d = round(float((c.iloc[-1] / c.iloc[-21] - 1) * 100), 2) if len(c) >= 21 else 0.0
            ret_50d = round(float((c.iloc[-1] / c.iloc[-51] - 1) * 100), 2) if len(c) >= 51 else 0.0

            rsi = round(float(StockScanner._rsi(c, 14).iloc[-1]), 1)

            ema20 = float(c.ewm(span=20, adjust=False, min_periods=20).mean().iloc[-1])
            ema50 = float(c.ewm(span=50, adjust=False, min_periods=50).mean().iloc[-1])
            pct_above_ema20 = round((float(c.iloc[-1]) / ema20 - 1) * 100, 2) if ema20 > 0 else 0.0

            rs_vs_market = round(ret_20d - bench_ret20, 2)
            score = round(ret_20d * 0.5 + rs_vs_market * 0.3 + ret_5d * 0.2, 3)

            results.append({
                "sector":        name,
                "ticker":        primary,
                "price":         round(float(c.iloc[-1]), 2),
                "last_date":     str(c.index[-1].date()),
                "ret_5d":        ret_5d,
                "ret_20d":       ret_20d,
                "ret_50d":       ret_50d,
                "rsi":           rsi,
                "above_ema":     bool(ema20 > ema50),
                "pct_above_ema": pct_above_ema20,
                "rs_vs_market":  rs_vs_market,
                "score":         score,
            })
            time.sleep(0.05)
        except Exception as exc:
            logger.debug("Sector momentum %s (%s): %s", name, primary, exc)

    results.sort(key=lambda x: x["score"], reverse=True)
    return {
        "sectors":       results[:5],
        "all_sectors":   results,
        "total_sectors": len(results),
        "bench_ret20d":  round(bench_ret20, 2),
        "bench_ret5d":   round(bench_ret5,  2),
        "last_updated":  datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
        "status":        "complete" if results else "no_data",
    }


async def _sector_momentum_response(bust_cache: bool) -> JSONResponse:
    """Shared implementation for both sector-momentum endpoints."""
    global _sec_mom_cache
    if bust_cache:
        _sec_mom_cache["ts"] = 0.0

    if (_sec_mom_cache["data"] is not None and
            time.time() - _sec_mom_cache["ts"] < _sec_mom_cache["ttl"]):
        return JSONResponse(_sec_mom_cache["data"])

    try:
        loop = asyncio.get_event_loop()
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


@app.get("/api/sector-momentum")
async def get_sector_momentum(refresh: int = 0) -> JSONResponse:
    """NSE sector index momentum rankings. Pass ?refresh=1 to bust the in-memory cache."""
    return await _sector_momentum_response(bust_cache=bool(refresh))


@app.get("/api/sectors")
async def get_sectors() -> JSONResponse:
    """Top sectors derived from Nifty500 + Microcap250 scan results."""
    from collections import defaultdict

    all_stocks = list(scan_state.get("data") or []) + list(mc_scan_state.get("data") or [])

    agg: dict = defaultdict(lambda: {
        "count": 0, "total_return": 0.0, "total_rsi": 0.0,
        "total_rs_outperf": 0.0, "total_score": 0.0,
        "total_vs_sector": 0.0, "vs_sector_count": 0, "tickers": [],
    })

    for stock in all_stocks:
        sec = stock.get("sector") or "Unknown"
        if sec in ("Unknown", ""):
            continue
        a = agg[sec]
        a["count"] += 1
        a["total_return"]     += stock.get("return_20d") or stock.get("return_1m") or 0.0
        a["total_rsi"]        += stock.get("rsi") or 0.0
        a["total_rs_outperf"] += stock.get("rs_outperf_pct") or 0.0
        a["total_score"]      += stock.get("score") or 0.0
        vs = stock.get("stock_vs_sector")
        if vs is not None:
            a["total_vs_sector"] += vs
            a["vs_sector_count"] += 1
        ticker = stock.get("display_ticker") or stock.get("ticker", "")
        if ticker:
            a["tickers"].append(ticker)

    result = []
    for sec, a in agg.items():
        n = a["count"]
        if n == 0:
            continue
        avg_vs_sector = (round(a["total_vs_sector"] / a["vs_sector_count"], 2)
                         if a["vs_sector_count"] > 0 else None)
        result.append({
            "sector":        sec,
            "stock_count":   n,
            "avg_return_20d":  round(a["total_return"] / n, 2),
            "avg_rsi":         round(a["total_rsi"] / n, 1),
            "avg_rs_outperf":  round(a["total_rs_outperf"] / n, 2),
            "avg_score":       round(a["total_score"] / n, 4),
            "avg_vs_sector":   avg_vs_sector,
            "top_tickers":     a["tickers"][:5],
        })

    result.sort(key=lambda x: x["avg_rs_outperf"] + x["avg_return_20d"], reverse=True)
    return JSONResponse({
        "sectors":            result[:5],
        "all_sectors":        result,
        "total_sectors":      len(result),
        "last_updated_n500":  scan_state.get("last_updated"),
        "last_updated_mc250": mc_scan_state.get("last_updated"),
        "n500_status":        scan_state.get("status"),
        "mc250_status":       mc_scan_state.get("status"),
    })


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    with open("templates/index.html", "r", encoding="utf-8-sig") as fh:
        content = fh.read()
    return HTMLResponse(content=content, media_type="text/html; charset=utf-8")


@app.get("/api/snapshot")
async def historical_snapshot(
    date: str = Query(..., description="Date in YYYY-MM-DD format")
) -> JSONResponse:
    """Run a one-time Nifty500 scan AS-OF a specific historical date."""
    target, err = _validate_date_param(date)
    if err:
        return err
    logger.info("Historical snapshot (Nifty500) requested for %s", date)
    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, lambda: scanner.scan(target_date=target))
        return JSONResponse({
            "data": results, "date": date,
            "filters_passed": len(results),
            "total_tickers":  len(scanner.tickers),
            "status":         "complete",
            "last_updated":   f"Historical data as of {date}",
        })
    except Exception as exc:
        logger.error("Historical Nifty500 scan failed: %s", exc, exc_info=True)
        return JSONResponse({"error": str(exc), "status": "error"}, status_code=500)


@app.get("/api/microcap/snapshot")
async def mc_historical_snapshot(
    date: str = Query(..., description="Date in YYYY-MM-DD format")
) -> JSONResponse:
    """Run a one-time Nifty Microcap 250 scan AS-OF a specific historical date."""
    target, err = _validate_date_param(date)
    if err:
        return err
    logger.info("Historical snapshot (Microcap250) requested for %s", date)
    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, lambda: scanner_mc.scan(target_date=target))
        return JSONResponse({
            "data": results, "date": date,
            "filters_passed": len(results),
            "total_tickers":  len(scanner_mc.tickers),
            "status":         "complete",
            "last_updated":   f"Historical data as of {date}",
        })
    except Exception as exc:
        logger.error("Historical Microcap250 scan failed: %s", exc, exc_info=True)
        return JSONResponse({"error": str(exc), "status": "error"}, status_code=500)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        log_config={
            "version": 1,
            "disable_existing_loggers": False,
            "loggers": {
                "uvicorn":        {"level": "INFO",    "propagate": True},
                "uvicorn.error":  {"level": "WARNING", "propagate": True},
                "uvicorn.access": {"level": "INFO",    "propagate": True},
            },
        },
    )

