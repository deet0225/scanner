"""
main.py  -  FastAPI application for Nifty 500 Stock Scanner
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
import threading
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
from scanner import (
    StockScanner,
    MOM_RSI_MIN, MOM_WRSI_MIN, MOM_ADX_MIN,
    MOM_VOLZ_MIN, MOM_RS_MIN, MOM_RET20_MIN, MOM_RET5_MIN, MOM_TV_MIN_CR,
)
from tickers import NIFTY500_TICKERS, NIFTY_MICROCAP250_TICKERS
import cache as _ohlcv_cache

# -- Logging ------------------------------------------------------------------
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

# -- Shared state -------------------------------------------------------------
scan_state: dict = {
    "data": [],
    "momentum_data": [],
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
    "momentum_data": [],
    "last_updated": None,
    "status": "initializing",
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
_mom_scan_lock: asyncio.Lock = asyncio.Lock()   # independent lock for momentum-only scans

# Per-tab cancellation events  -  set when user switches away from a tab.
# BG workers check these after every ticker and exit early if set.
_fund_cancel: threading.Event = threading.Event()
_sme_cancel:  threading.Event = threading.Event()
_active_tab:  str             = "swing"   # tracks the currently visible tab

# Generation counters — incremented on each Force-Live-Data / cache-clear.
# Each BG worker captures the generation at launch and self-terminates when it
# detects a newer generation, preventing two workers racing on the same dict.
_fund_generation:        int   = 0
_fund_last_completed_ts: float = 0.0   # epoch when last FULL BG worker run finished
_sme_generation:         int   = 0
_sme_last_completed_ts:  float = 0.0   # epoch when last FULL SME BG worker run finished

# ---------------------------------------------------------------------------
# Dedicated Stock Momentum scan state  (independent of Swing Trade)
# ---------------------------------------------------------------------------
# Populated by run_n500_momentum_scan() / run_mc250_momentum_scan() which
# call scanner.scan(momentum_only=True) — applies ONLY the 6 momentum
# criteria, NO fundamentals, NO swing-trade entry conditions.
mom_scan_state: dict = {
    "data":           [],
    "last_updated":   None,
    "status":         "initializing",
    "scan_count":     0,
    "filters_passed": 0,
    "next_scan_ts":   None,
    "error":          None,
    "total_tickers":  len(scanner.tickers),
    "scan_stage":     "",
    "regime_ok":      True,
    "regime_summary": "",
}

mc_mom_scan_state: dict = {
    "data":           [],
    "last_updated":   None,
    "status":         "initializing",
    "scan_count":     0,
    "filters_passed": 0,
    "next_scan_ts":   None,
    "error":          None,
    "total_tickers":  len(scanner_mc.tickers),
    "scan_stage":     "",
    "regime_ok":      True,
    "regime_summary": "",
}

# ---------------------------------------------------------------------------
# Dedicated Morning Star scan state (all 750 tickers, pattern only)
# ---------------------------------------------------------------------------
# Populated by run_n500_ms_scan() / run_mc250_ms_scan() which call
# scanner.scan_morning_star() — no momentum/swing criteria, just checks
# the 3-candle Morning Star pattern on every ticker using cache-first OHLCV.
_ms_scan_lock: asyncio.Lock = asyncio.Lock()

ms_scan_state: dict = {
    "data":           [],
    "last_updated":   None,
    "status":         "initializing",
    "scan_count":     0,
    "filters_passed": 0,
    "next_scan_ts":   None,
    "error":          None,
    "total_tickers":  len(scanner.tickers),
    "scan_stage":     "",
}

mc_ms_scan_state: dict = {
    "data":           [],
    "last_updated":   None,
    "status":         "initializing",
    "scan_count":     0,
    "filters_passed": 0,
    "next_scan_ts":   None,
    "error":          None,
    "total_tickers":  len(scanner_mc.tickers),
    "scan_stage":     "",
}
async def _do_run_generic_scan(sc: StockScanner, state: dict, label: str) -> None:
    """Inner scan body  -  must be called while _scan_lock is held.
    Works for both Nifty500 and Microcap250 scanners."""
    def _progress(stage: str):
        state["scan_stage"] = stage

    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, lambda: sc.scan(progress_cb=_progress))

        now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
        state.update({
            "data":           results,
            "momentum_data":  sc.last_momentum_results,   # momentum-only filtered list
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


# -- Scan tasks ---------------------------------------------------------------
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


# -- Dedicated momentum-only scan tasks ----------------------------------------


async def _do_run_momentum_scan(sc: StockScanner, state: dict, label: str) -> None:
    """Run scanner.scan(momentum_only=True) and store results in *state*.
    Must be called while _mom_scan_lock is held."""
    def _progress(stage: str):
        state["scan_stage"] = stage

    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            None, lambda: sc.scan(progress_cb=_progress, momentum_only=True)
        )
        now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
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
        logger.info("=== %s Momentum Scan #%d complete -- %d qualifying stocks ===",
                    label, state["scan_count"], len(results))

    except Exception as exc:
        logger.error("%s momentum scan failed: %s", label, exc, exc_info=True)
        state["status"] = "error"
        state["error"]  = str(exc)
        state["next_scan_ts"] = (time.time() + SCAN_INTERVAL_MINUTES * 60) if AUTO_RESCAN else None


async def run_n500_momentum_scan() -> None:
    """Run a momentum-only scan for the Nifty 500 universe."""
    # Set status to "scanning" IMMEDIATELY — before waiting for the lock.
    # If we only set it inside `async with _mom_scan_lock:`, the status stays
    # "initializing" while this coroutine is queued, and polling callers
    # (get_stock_momentum, get_morning_momentum) keep creating duplicate tasks.
    mom_scan_state["status"] = "scanning"
    if _mom_scan_lock.locked():
        logger.info("N500 momentum scan queued -- waiting for MC250 momentum scan...")
        mom_scan_state["scan_stage"] = "Queued (waiting for MC250 momentum scan)..."
    async with _mom_scan_lock:
        mom_scan_state["scan_stage"] = "Loading OHLCV data..."
        mom_scan_state["next_scan_ts"] = None
        logger.info("=== Nifty500 Momentum scan starting ===")
        await _do_run_momentum_scan(scanner, mom_scan_state, "Nifty500-Momentum")


async def run_mc250_momentum_scan() -> None:
    """Run a momentum-only scan for the Microcap 250 universe."""
    # Set status to "scanning" IMMEDIATELY — before waiting for the lock.
    # Same reasoning as run_n500_momentum_scan(): prevents duplicate task
    # creation from polling endpoints that check mc250_status == "initializing".
    mc_mom_scan_state["status"] = "scanning"
    if _mom_scan_lock.locked():
        logger.info("MC250 momentum scan queued -- waiting for N500 momentum scan...")
        mc_mom_scan_state["scan_stage"] = "Queued (waiting for N500 momentum scan)..."
    async with _mom_scan_lock:
        mc_mom_scan_state["scan_stage"] = "Loading OHLCV data..."
        mc_mom_scan_state["next_scan_ts"] = None
        logger.info("=== Microcap250 Momentum scan starting ===")
        await _do_run_momentum_scan(scanner_mc, mc_mom_scan_state, "Microcap250-Momentum")


async def _maybe_run_n500_momentum_scan() -> None:
    """Scheduler wrapper for periodic N500 momentum rescan."""
    if mom_scan_state["status"] not in ("scanning",):
        await run_n500_momentum_scan()


async def _maybe_run_mc250_momentum_scan() -> None:
    """Scheduler wrapper for periodic MC250 momentum rescan."""
    if mc_mom_scan_state["status"] not in ("scanning",):
        await run_mc250_momentum_scan()


# -- Dedicated Morning Star scan tasks -----------------------------------------

async def _do_run_ms_scan(sc: StockScanner, state: dict, label: str) -> None:
    """Run scanner.scan_morning_star() and store results in *state*.
    Must be called while _ms_scan_lock is held."""
    def _progress(stage: str):
        state["scan_stage"] = stage

    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            None, lambda: sc.scan_morning_star(progress_cb=_progress)
        )
        now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
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
        logger.info("=== %s Morning Star Scan #%d complete -- %d qualifying stocks ===",
                    label, state["scan_count"], len(results))

    except Exception as exc:
        logger.error("%s morning-star scan failed: %s", label, exc, exc_info=True)
        state["status"] = "error"
        state["error"]  = str(exc)
        state["next_scan_ts"] = (time.time() + SCAN_INTERVAL_MINUTES * 60) if AUTO_RESCAN else None


async def run_n500_ms_scan() -> None:
    """Run a Morning Star pattern scan for the Nifty 500 universe."""
    ms_scan_state["status"] = "scanning"
    if _ms_scan_lock.locked():
        logger.info("N500 Morning Star scan queued -- waiting for MC250...")
        ms_scan_state["scan_stage"] = "Queued (waiting for MC250 morning-star scan)..."
    async with _ms_scan_lock:
        ms_scan_state["scan_stage"] = "Loading OHLCV data..."
        ms_scan_state["next_scan_ts"] = None
        logger.info("=== Nifty500 Morning Star scan starting ===")
        await _do_run_ms_scan(scanner, ms_scan_state, "Nifty500-MS")


async def run_mc250_ms_scan() -> None:
    """Run a Morning Star pattern scan for the Microcap 250 universe."""
    mc_ms_scan_state["status"] = "scanning"
    if _ms_scan_lock.locked():
        logger.info("MC250 Morning Star scan queued -- waiting for N500...")
        mc_ms_scan_state["scan_stage"] = "Queued (waiting for N500 morning-star scan)..."
    async with _ms_scan_lock:
        mc_ms_scan_state["scan_stage"] = "Loading OHLCV data..."
        mc_ms_scan_state["next_scan_ts"] = None
        logger.info("=== Microcap250 Morning Star scan starting ===")
        await _do_run_ms_scan(scanner_mc, mc_ms_scan_state, "Microcap250-MS")


async def _maybe_run_n500_ms_scan() -> None:
    """Scheduler wrapper for periodic N500 morning-star rescan."""
    if ms_scan_state["status"] not in ("scanning",):
        await run_n500_ms_scan()


async def _maybe_run_mc250_ms_scan() -> None:
    """Scheduler wrapper for periodic MC250 morning-star rescan."""
    if mc_ms_scan_state["status"] not in ("scanning",):
        await run_mc250_ms_scan()
@asynccontextmanager
async def lifespan(app: FastAPI):
    _fund_cache_load()   # load fundamentals disk cache into memory (no network)
    _sme_cache_load()    # load SME fundamentals disk cache into memory (no network)

    if AUTO_RESCAN:
        # Periodic rescan mode  -  scheduler triggers both scanners on a fixed interval
        scheduler.add_job(_maybe_run_n500_scan,           "interval", minutes=SCAN_INTERVAL_MINUTES,     id="stock_scan")
        scheduler.add_job(_maybe_run_mc_scan,             "interval", minutes=SCAN_INTERVAL_MINUTES + 1, id="mc_scan")
        scheduler.add_job(_maybe_run_n500_momentum_scan,  "interval", minutes=SCAN_INTERVAL_MINUTES,     id="n500_mom_scan")
        scheduler.add_job(_maybe_run_mc250_momentum_scan, "interval", minutes=SCAN_INTERVAL_MINUTES + 1, id="mc250_mom_scan")
        # Morning Star scans run offset so they don't pile up with momentum scans
        scheduler.add_job(_maybe_run_n500_ms_scan,        "interval", minutes=SCAN_INTERVAL_MINUTES + 2, id="n500_ms_scan")
        scheduler.add_job(_maybe_run_mc250_ms_scan,       "interval", minutes=SCAN_INTERVAL_MINUTES + 3, id="mc250_ms_scan")
        scheduler.start()
        logger.info("Auto-rescan ENABLED: Nifty500 every %d min, Microcap250 every %d min",
                    SCAN_INTERVAL_MINUTES, SCAN_INTERVAL_MINUTES + 1)
        asyncio.create_task(run_scan())
        asyncio.create_task(run_mc_scan())
        asyncio.create_task(run_n500_momentum_scan())
        asyncio.create_task(run_mc250_momentum_scan())
        asyncio.create_task(run_n500_ms_scan())
        asyncio.create_task(run_mc250_ms_scan())
    else:
        # On-demand mode  -  no background downloads at startup.
        # Scans start when the user visits each tab:
        #   Swing Trades -> /api/trigger  -> run_scan() + run_mc_scan()
        #   Fundamentals -> /api/fundamentals  -> _fund_bg_worker (delta only)
        #   SME          -> /api/sme/fundamentals -> _sme_bg_worker (delta only)
        # Switching tabs sets a cancellation event to stop the previous worker.
        logger.info(
            "On-demand mode: caches loaded from disk "
            "(N500=%d, MC250=%d tickers). All network downloads start on tab visit.",
            len(scanner.tickers), len(scanner_mc.tickers),
        )

    yield
    if AUTO_RESCAN:
        scheduler.shutdown(wait=False)


async def _kick_fund_bg() -> None:
    """On-demand fundamentals refresh utility  -  NOT called at startup.

    Use this to proactively warm the fundamentals cache (e.g. after a
    cold deploy with an empty cache).  In normal operation the lazy path
    inside get_fundamentals() / get_sme_fundamentals() is sufficient.
    """
    global _fund_bg_running
    if _fund_bg_running:
        return
    all_tickers = list(dict.fromkeys(NIFTY500_TICKERS + NIFTY_MICROCAP250_TICKERS))
    stale_count = sum(
        1 for t in all_tickers
        if time.time() - _fund_data.get(t, {}).get("_ts", 0)
           >= (_FUND_FAIL_TTL if _fund_data.get(t, {}).get("_gf") else _FUND_CACHE_TTL)
    )
    if stale_count == 0:
        logger.info("Fundamentals cache fully fresh  -  nothing to refresh")
        return
    logger.info("Fundamentals on-demand kick: %d/%d stale  -  starting background refresh",
                stale_count, len(all_tickers))
    _fund_bg_running = True
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _fund_bg_worker, all_tickers)


app = FastAPI(title="Nifty Stock Scanner", lifespan=lifespan)


# -- Helpers -------------------------------------------------------------------
def _validate_date_param(date: str):
    """Parse and validate a YYYY-MM-DD date string.
    Returns (DateType, None) on success, or (None, JSONResponse) on error."""
    try:
        target = DateType.fromisoformat(date)
    except ValueError:
        return None, JSONResponse({"error": "Invalid date format  -  use YYYY-MM-DD"}, status_code=400)
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


# -- Routes -------------------------------------------------------------------
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


@app.post("/api/stock-momentum/rescan")
async def force_momentum_rescan() -> JSONResponse:
    """Force fresh momentum-only scans for both N500 and MC250 universes."""
    tasks_triggered = 0
    if mom_scan_state["status"] not in ("scanning",):
        asyncio.create_task(run_n500_momentum_scan())
        tasks_triggered += 1
    if mc_mom_scan_state["status"] not in ("scanning",):
        asyncio.create_task(run_mc250_momentum_scan())
        tasks_triggered += 1
    return JSONResponse({"triggered": tasks_triggered > 0, "status": "scanning"})


@app.post("/api/morning-momentum/rescan")
async def force_ms_rescan() -> JSONResponse:
    """Force fresh Morning Star pattern scans for both N500 and MC250 universes."""
    tasks_triggered = 0
    if ms_scan_state["status"] not in ("scanning",):
        asyncio.create_task(run_n500_ms_scan())
        tasks_triggered += 1
    if mc_ms_scan_state["status"] not in ("scanning",):
        asyncio.create_task(run_mc250_ms_scan())
        tasks_triggered += 1
    return JSONResponse({"triggered": tasks_triggered > 0, "status": "scanning"})


@app.post("/api/microcap/rescan")
async def force_mc_rescan() -> JSONResponse:
    """Force a fresh Microcap250 scan immediately."""
    global n500_tab_active
    n500_tab_active = False
    asyncio.create_task(run_mc_scan())
    return JSONResponse({"triggered": True, "status": "scanning"})


@app.post("/api/stop-all-scans")
async def stop_all_scans() -> JSONResponse:
    """Immediately signal all running scans and background workers to stop.

    Called by the frontend *before* clearing caches so that in-flight workers
    don't race with the clear and re-populate the just-wiped cache.

    Actions taken:
      • Fund + SME BG workers: generation is incremented (workers self-terminate
        at the next batch boundary) AND cancel events are set.
      • _fund_bg_running / _sme_bg_running reset to False so a fresh run can
        start cleanly after caches are cleared.
      • All price-scan states (swing/momentum/morning-star for both N500 and
        MC250) are marked "idle" so pending asyncio tasks know the slot is free
        and the UI shows a neutral state while the cache clear happens.
    """
    global _fund_bg_running, _sme_bg_running, _fund_generation, _sme_generation

    # ── 1. Increment generations → BG thread-workers self-terminate ──────────
    _fund_generation += 1
    _sme_generation  += 1

    # ── 2. Set cancel events (checked at every batch boundary in BG workers) ──
    _fund_cancel.set()
    _sme_cancel.set()

    # ── 3. Reset running flags so a fresh worker can start after cache clear ──
    _fund_bg_running = False
    _sme_bg_running  = False

    # ── 4. Mark all price-scan states as idle ─────────────────────────────────
    for state in (scan_state, mc_scan_state,
                  mom_scan_state, mc_mom_scan_state,
                  ms_scan_state,  mc_ms_scan_state):
        if state.get("status") == "scanning":
            state["status"]     = "idle"
            state["scan_stage"] = "Stopped — awaiting fresh scan"

    logger.info(
        "stop-all-scans: fund_gen=%d sme_gen=%d — all workers signalled to stop",
        _fund_generation, _sme_generation,
    )
    return JSONResponse({
        "stopped": True,
        "fund_generation": _fund_generation,
        "sme_generation":  _sme_generation,
        "message": "All background workers signalled to stop; price scans marked idle",
    })


@app.get("/api/cache/stats")
async def cache_stats() -> JSONResponse:
    return JSONResponse(_ohlcv_cache.stats())


@app.post("/api/cache/clear")
async def cache_clear() -> JSONResponse:
    n = _ohlcv_cache.clear()
    logger.info("OHLCV cache cleared: %d files deleted", n)
    return JSONResponse({"deleted": n, "message": f"{n} cache files removed"})


@app.post("/api/fundamentals/clear-cache")
async def fundamentals_cache_clear() -> JSONResponse:
    """Force a full re-download of all fundamentals data on the next request.

    Completely wipes the in-memory cache dict (not just resetting _ts) AND
    deletes the on-disk JSON file so that all tickers are re-fetched from
    Screener.in from scratch — identical semantics to /api/cache/clear which
    physically deletes OHLCV pkl files.

    Also clears the ScreenerClient 1-hour in-memory HTML-scrape cache so that
    the background worker actually fetches fresh data from Screener.in instead
    of returning its own stale cached responses.  This is the primary reason
    "Force Live Data" used to produce different results between local and Render
    without a full process restart.

    Useful when results differ between local and Render (e.g. stale disk cache).
    """
    global _fund_result_cache_valid, _fund_result_cache_body, _fund_bg_running, _fund_generation
    # ── Increment generation FIRST so any currently-running BG worker sees it
    # and self-terminates at the next batch boundary, preventing two workers from
    # racing on the same _fund_data dict (the double-worker race condition).
    _fund_generation += 1
    with _fund_data_lock:
        deleted_count = len(_fund_data)
        _fund_data.clear()   # wipe ALL entries (data + timestamps), not just reset _ts
    # Delete the JSON file from disk so a restart doesn't reload stale data
    disk_deleted = False
    try:
        if _FUND_CACHE_FILE.exists():
            _FUND_CACHE_FILE.unlink()
            disk_deleted = True
    except Exception as exc:
        logger.warning("Could not delete fundamentals cache file: %s", exc)
    _fund_result_cache_valid = False
    _fund_result_cache_body  = None
    _fund_bg_running         = False   # allow a fresh BG run to start immediately

    # ── CRITICAL: also clear the ScreenerClient 1-hour in-memory HTML-scrape cache.
    # Without this, the background worker that re-populates _fund_data calls
    # _fc.get_extra_fundamentals() → ScreenerClient.get() which returns stale cached
    # HTML-scraped values from its own in-process dict (TTL 1 hour). The _fund_data
    # dict appears to be cleared but gets immediately re-populated with the SAME old
    # values from the ScreenerClient cache — making "Force Live Data" a no-op for
    # any ticker fetched within the past hour.
    screener_cleared = 0
    try:
        from data_sources import ScreenerClient as _SC
        screener_cleared = len(_SC._cache)
        _SC._cache.clear()
        _SC._cache_ts.clear()
    except Exception as exc:
        logger.warning("Could not clear ScreenerClient cache: %s", exc)

    logger.info(
        "Fundamentals cache CLEARED: %d in-memory entries removed, "
        "%d ScreenerClient HTML-cache entries cleared, disk file %s",
        deleted_count, screener_cleared,
        "deleted" if disk_deleted else "delete-failed (check permissions)",
    )
    return JSONResponse({
        "reset":             deleted_count,
        "screener_cleared":  screener_cleared,
        "disk_deleted":      disk_deleted,
        "message": (
            f"{deleted_count} fundamentals entries cleared, "
            f"{screener_cleared} Screener.in HTML-cache entries cleared, "
            f"JSON file {'deleted' if disk_deleted else 'could not be deleted'} — "
            "full re-download will start on next tab visit"
        ),
    })


@app.post("/api/sme/fundamentals/clear-cache")
async def sme_fundamentals_cache_clear() -> JSONResponse:
    """Force a full re-download of all SME fundamentals data on the next request.

    Same semantics as /api/fundamentals/clear-cache but for the SME universe
    (NSE Emerge + BSE SME stocks).  Wipes the in-memory dict AND deletes the
    on-disk JSON file so stale data cannot be reloaded on a restart.

    Also clears the ScreenerClient 1-hour HTML-scrape cache (same fix as the
    main fundamentals clear endpoint) so that get_extra_sme_fundamentals()
    actually fetches new data from Screener.in instead of returning stale
    cached responses from the prior refresh cycle.
    """
    global _sme_result_cache_valid, _sme_result_cache_body, _sme_bg_running, _sme_generation
    # ── Increment generation so any running SME BG worker self-terminates
    _sme_generation += 1
    with _sme_fund_lock:
        deleted_count = len(_sme_fund_data)
        _sme_fund_data.clear()   # wipe ALL entries, not just reset _ts
    disk_deleted = False
    try:
        if _SME_FUND_CACHE_FILE.exists():
            _SME_FUND_CACHE_FILE.unlink()
            disk_deleted = True
    except Exception as exc:
        logger.warning("Could not delete SME fundamentals cache file: %s", exc)
    _sme_result_cache_valid = False
    _sme_result_cache_body  = None
    _sme_bg_running         = False   # allow a fresh BG run to start immediately

    # ── Clear ScreenerClient HTML-scrape cache (see fundamentals_cache_clear comment)
    screener_cleared = 0
    try:
        from data_sources import ScreenerClient as _SC
        screener_cleared = len(_SC._cache)
        _SC._cache.clear()
        _SC._cache_ts.clear()
    except Exception as exc:
        logger.warning("Could not clear ScreenerClient cache (SME): %s", exc)

    logger.info(
        "SME fundamentals cache CLEARED: %d in-memory entries removed, "
        "%d ScreenerClient HTML-cache entries cleared, disk file %s",
        deleted_count, screener_cleared,
        "deleted" if disk_deleted else "delete-failed (check permissions)",
    )
    return JSONResponse({
        "reset":             deleted_count,
        "screener_cleared":  screener_cleared,
        "disk_deleted":      disk_deleted,
        "message": (
            f"{deleted_count} SME fundamentals entries cleared, "
            f"{screener_cleared} Screener.in HTML-cache entries cleared, "
            f"JSON file {'deleted' if disk_deleted else 'could not be deleted'} — "
            "full re-download will start on next tab visit"
        ),
    })


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
    if scan_state["status"] not in ("scanning",):
        asyncio.create_task(run_scan())
        return JSONResponse({"triggered": True, "status": "scanning"})
    return JSONResponse({"triggered": False, "status": scan_state["status"]})


@app.post("/api/microcap/trigger")
async def trigger_mc_scan() -> JSONResponse:
    global mc_scan_ever_triggered, n500_tab_active
    n500_tab_active = True
    mc_scan_ever_triggered = True
    status = mc_scan_state["status"]
    if status not in ("scanning",):
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


@app.post("/api/tab-active")
async def set_active_tab(tab: str = Query(..., description="Active tab name")) -> JSONResponse:
    """Signal which tab is currently visible.
    Cancels any BG workers belonging to tabs other than the active one."""
    global _active_tab
    prev = _active_tab
    _active_tab = tab

    # Cancel workers whose tab is no longer active
    if tab != "fund":
        _fund_cancel.set()
    else:
        _fund_cancel.clear()   # allow fund worker to proceed

    if tab != "sme":
        _sme_cancel.set()
    else:
        _sme_cancel.clear()    # allow sme worker to proceed

    logger.debug("Tab active: %s (prev: %s)", tab, prev)
    return JSONResponse({"tab": tab, "prev": prev})


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


# -- NSE / BSE Sector Index definitions ---------------------------------------
# Each entry: (display_name, primary_yf_ticker, [etf_fallback_tickers])
#
# NOTE: tvDatafeed (TradingView library) is no longer on PyPI and cannot be
# installed. Yahoo Finance is also unreachable live due to corporate SSL proxy.
# ALL data is served from disk cache (cache/ohlcv/IDX_*.pkl).
#
# Cache key: ^NSEBANK -> IDX_NSEBANK.pkl,  ^BSEBANKEX -> IDX_BSEBANKEX.pkl etc.
# As long as the cache file exists and is fresh, Live download is not needed.
#
# Cached & serving (as of May 2026):
#   NSE: ^NSEBANK ^CNXIT ^CNXAUTO ^CNXPHARMA ^CNXFMCG ^CNXMETAL ^CNXREALTY
#        ^CNXENERGY ^CNXPSUBANK ^CNXHEALTH ^CNXFIN ^CNXMEDIA ^CNXINFRA
#        ^CNXOIL ^CNXCONSUMER ^CNXCMDT ^CNXMNC ^CNXPSE ^CNXSERVICE
#        ^CNXMIDCAP ^CNXSC
#   BSE: ^BSEAUTO ^BSEBANKEX ^BSECD ^BSEFMCG ^BSEHC ^BSEIT ^BSEOIL ^BSETECK
#
# No cache (will be silently skipped until downloadable):
#   ^CNXPVTBANK ^CNXCAPGOODS ^CNXDEFENCE ^CNXPOWER ^CNXMFG
_NSE_SECTOR_INDICES = [
    # -- Broad NSE sectors (all have fresh disk cache) ------------------------
    ("Nifty Bank",            "^NSEBANK",    ["BANKBEES.NS"]),
    ("Nifty IT",              "^CNXIT",      ["ITBEES.NS",     "ITETF.NS"]),
    ("Nifty Auto",            "^CNXAUTO",    ["AUTOBEES.NS"]),
    ("Nifty Pharma",          "^CNXPHARMA",  ["PHARMABEES.NS"]),
    ("Nifty FMCG",            "^CNXFMCG",    ["FMCGIETF.NS"]),
    ("Nifty Metal",           "^CNXMETAL",   ["METALBEES.NS"]),
    ("Nifty Realty",          "^CNXREALTY",  ["REALTYBEES.NS"]),
    ("Nifty Energy & Power",  "^CNXENERGY",  ["ENERGYBEES.NS"]),  # includes NTPC, PowerGrid, Tata Power
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

    # -- BSE sector indices (all have fresh disk cache from today) -------------
    ("BSE Bankex",            "^BSEBANKEX",  ["BANKBEES.NS"]),
    ("BSE IT",                "^BSEIT",      ["ITBEES.NS"]),
    ("BSE Teck",              "^BSETECK",    ["ITETF.NS"]),
    ("BSE Healthcare",        "^BSEHC",      ["HEALTHIETF.NS"]),
    ("BSE FMCG",              "^BSEFMCG",    ["FMCGIETF.NS"]),
    ("BSE Auto",              "^BSEAUTO",    ["AUTOBEES.NS"]),
    ("BSE Consumer Dur",      "^BSECD",      ["CONSUMBEES.NS"]),
    ("BSE Oil & Gas",         "^BSEOIL",     ["OILIETF.NS"]),

    # -- No cache yet  -  silently skipped until network is available ------------
    ("Nifty Private Bank",    "^CNXPVTBANK", ["PVTBANKETF.NS"]),
    ("Nifty Capital Goods",   "^CNXCAPGOODS",["CAPGOODS.NS"]),
    ("Nifty Defence",         "^CNXDEFENCE", ["DEFENIETF.NS"]),
    ("Nifty Power",           "^CNXPOWER",   ["POWERIETF.NS", "POWERBEES.NS"]),
    ("Nifty Mfg",             "^CNXMFG",     ["MFGETF.NS"]),
]

# In-memory cache (short TTL  -  disk cache handles real freshness)
_sec_mom_cache: dict = {"data": None, "ts": 0.0, "ttl": 300}


def _compute_sector_momentum(as_of_date=None) -> dict:
    """Compute momentum metrics for NSE/BSE sector indices.

    All data is served from disk cache (cache/ohlcv/IDX_*.pkl).
    Pass as_of_date (datetime.date) to compute metrics as-of a historical date.
    Live download is attempted for incremental updates but falls back to
    stale cache gracefully when Yahoo Finance is unreachable (e.g. corporate
    SSL proxy).  As of May 2026, 29 sector indices have fresh cached data.

    Note: tvDatafeed (TradingView) is no longer on PyPI and is not used.
    """
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

    # Slice to historical date if requested
    if as_of_date is not None:
        import pandas as _pd2
        def _slice_to_date(df):
            if df is None:
                return None
            mask = _pd2.to_datetime(df.index).date <= as_of_date
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

            # 5-day and 20-day relative strength vs benchmark
            rs_5d  = round(ret_5d  - bench_ret5,  2)
            rs_20d = round(ret_20d - bench_ret20, 2)

            # MACD(12,26,9) histogram as % of price -- positive = building momentum
            _ema12 = c.ewm(span=12, adjust=False, min_periods=12).mean()
            _ema26 = c.ewm(span=26, adjust=False, min_periods=26).mean()
            _macd  = _ema12 - _ema26
            _sig   = _macd.ewm(span=9, adjust=False, min_periods=9).mean()
            _price = float(c.iloc[-1])
            macd_hist_pct = round(float((_macd - _sig).iloc[-1]) / _price * 100, 4) if _price > 0 else 0.0

            # RSI-9 swing sweet-spot score: peaks at RSI=65, decays outside 45-85 band
            rsi_swing = max(0.0, 2.0 - abs(rsi_9 - 65) * 0.10)

            # Swing score -- optimised for 1-10 day momentum trades
            # Weights: 5D return  30% | 5D RS vs Nifty  25% | 3D momentum  20%
            #          RSI-9 zone 10% | above EMA9       8% | MACD hist    7%
            score = round(
                ret_5d         * 0.30 +
                rs_5d          * 0.25 +
                ret_3d         * 0.20 +
                rsi_swing      * 0.50 +   # [0, 2] scaled -> max +1.0
                pct_above_ema9 * 0.08 +
                macd_hist_pct  * 3.50,    # MACD hist is small %; amplify
                3,
            )

            results.append({
                "sector":          name,
                "ticker":          primary,
                "price":           round(_price, 2),
                "last_date":       str(c.index[-1].date()),
                "ret_3d":          ret_3d,
                "ret_5d":          ret_5d,
                "ret_20d":         ret_20d,
                "ret_50d":         ret_50d,
                "rsi":             rsi_14,        # RSI-14 kept for backward compat
                "rsi_9":           rsi_9,
                "above_ema":       bool(ema9 > ema20),   # EMA9 > EMA20 (swing trend)
                "above_ema_slow":  bool(ema20 > ema50),  # medium-term trend
                "pct_above_ema":   pct_above_ema9,       # vs EMA9
                "pct_above_ema20": pct_above_ema20,
                "rs_vs_market":    rs_20d,         # 20D RS kept for backward compat
                "rs_5d":           rs_5d,
                "macd_hist_pct":   macd_hist_pct,
                "score":           score,
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
        "last_updated":  (str(as_of_date) + " (historical)") if as_of_date else datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
        "as_of_date":    str(as_of_date) if as_of_date else None,
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
async def get_sector_momentum(
    refresh: int = 0,
    date: str = Query(None, description="Historical date YYYY-MM-DD; omit for live data"),
) -> JSONResponse:
    """NSE sector index momentum rankings. Pass ?refresh=1 to bust the in-memory cache.
    Pass ?date=YYYY-MM-DD for historical results computed as-of that date."""
    if date:
        target, err = _validate_date_param(date)
        if err:
            return err
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, lambda: _compute_sector_momentum(as_of_date=target)
            )
            return JSONResponse(result)
        except Exception as exc:
            logger.error("Historical sector momentum failed: %s", exc, exc_info=True)
            return JSONResponse({"error": str(exc), "sectors": [], "all_sectors": [],
                                 "total_sectors": 0, "status": "error"}, status_code=500)
    return await _sector_momentum_response(bust_cache=bool(refresh))


# -- Fundamentals disk cache ---------------------------------------------------
# Persists D/E, market cap, ROE, promoter holding, sales growth across restarts.
# Cache file: cache/fundamentals_data.json  (one entry per NSE ticker)
# TTL: 24 hours per ticker (re-fetched in background on next tab visit)

from pathlib import Path as _PL

_FUND_CACHE_FILE        = _PL("cache/fundamentals_data.json")
_FUND_CACHE_TTL         = 48 * 3600   # 48 h per ticker (fundamentals are quarterly — no need to refresh daily)
_FUND_FAIL_TTL          = 72 * 3600   # 72 h for tickers that clearly fail hard gates (skip re-download)
_FUND_FORCE_REFRESH_TTL =  4 * 3600   # on manual Refresh, only re-fetch entries older than 4 h
_FUND_MIN_SCORE         = 50.0        # hide stocks with fund_score < 50 from the table
_fund_data: dict = {}               # {ticker: {sector, debt_equity, ...}, ...}
_fund_data_lock  = threading.Lock() # protects concurrent writes from parallel BG workers
_fund_bg_running: bool = False

# ---------------------------------------------------------------------------
# Result cache for /api/fundamentals
# Invalidated whenever _fund_cache_save() writes new data to disk.
# This ensures every page-open within the same batch cycle returns identical
# results, eliminating the "different results each visit" issue caused by
# reading _fund_data while the background worker is mid-refresh.
# ---------------------------------------------------------------------------
_fund_result_cache_body:  "dict | None" = None   # last computed response body
_fund_result_cache_valid: bool          = False   # False = must recompute on next request


def _fund_cache_load() -> None:
    """Load fundamentals disk cache into memory (called once at startup).
    Automatically invalidates entries that are missing key fields so they get
    re-fetched by the background worker.
    """
    global _fund_data
    try:
        if _FUND_CACHE_FILE.exists():
            _fund_data = json.loads(_FUND_CACHE_FILE.read_text(encoding="utf-8"))
            invalidated = 0
            for entry in _fund_data.values():
                if isinstance(entry, dict) and entry.get("_ts", 0) > 0:
                    missing_basic    = "roce" not in entry
                    missing_enhanced = not any(k in entry for k in
                                               ("fii_holding", "current_price", "peg_ratio"))
                    # Invalidate if missing new debt/cashflow fields (v3 schema)
                    missing_cashflow = not any(k in entry for k in
                                               ("current_ratio", "cash_from_operations", "cfo_yield"))
                    # Invalidate if missing OPM/NPM fields (added in later fix)
                    missing_margins  = not any(k in entry for k in ("opm", "net_profit_margin"))
                    if missing_basic or missing_enhanced or missing_cashflow or missing_margins:
                        entry["_ts"] = 0   # force re-fetch without deleting
                        invalidated += 1
            logger.info(
                "Fundamentals disk cache loaded: %d entries (%d invalidated  -  stale/missing fields)",
                len(_fund_data), invalidated,
            )
        else:
            _fund_data = {}
    except Exception as exc:
        logger.warning("Could not load fundamentals cache: %s", exc)
        _fund_data = {}


def _fund_cache_save() -> None:
    """Persist in-memory fundamentals cache to disk."""
    global _fund_result_cache_valid
    _fund_result_cache_valid = False   # new data → force result recompute on next request
    try:
        _FUND_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _FUND_CACHE_FILE.write_text(
            json.dumps(_fund_data, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("Fundamentals cache save failed: %s", exc)


def _fund_refresh_ticker(ticker: str) -> tuple:
    """Fetch & cache fundamentals for one ticker, including derived metrics.

    Returns (result, content_changed) where:
      result           -  the full data dict stored in _fund_data
      content_changed  -  True only when at least one data field (excluding _ts)
                        actually changed vs the previously cached value.

    _ts is always updated in memory so the TTL clock resets, preventing
    unnecessary re-downloads on the next cycle.  The caller decides whether
    to persist the cache to disk based on content_changed.
    """
    from data_sources import fundamentals as _fc
    import math

    result: dict = {}
    try:
        sector, de_x100, mc_inr = _fc.get(ticker)
        if sector:
            result["sector"] = sector
        if de_x100 is not None:
            result["debt_equity"]   = round(float(de_x100) / 100, 2)
        if mc_inr is not None:
            result["market_cap_cr"] = round(float(mc_inr) / 1e7, 0)
    except Exception:
        pass

    try:
        extra = _fc.get_extra_fundamentals(ticker)
        for k in ("roce", "roe", "roe_5y", "roe_10y",
                  "promoter_holding", "fii_holding", "dii_holding",
                  "sales_growth_pct", "sales_growth_5y", "sales_growth_10y",
                  "profit_growth_3y", "profit_growth_5y", "profit_growth_10y",
                  "sales_growth_ttm", "profit_growth_ttm",
                  "pe_ratio", "book_value", "dividend_yield",
                  "debt_equity", "market_cap_cr", "current_price",
                  "current_ratio", "cash_from_operations",
                  "opm", "net_profit_margin"):
            if k in extra and extra[k] is not None:
                result[k] = extra[k]
    except Exception:
        pass

    # -- Compute derived fundamental metrics ---------------------------------
    pe   = result.get("pe_ratio")
    bv   = result.get("book_value")
    cp   = result.get("current_price")
    pg3  = result.get("profit_growth_3y")
    pg5  = result.get("profit_growth_5y")
    mc   = result.get("market_cap_cr")
    cfo  = result.get("cash_from_operations")

    eps = None
    if cp and pe and pe > 0:
        eps = cp / pe

    growth_for_peg = pg3 if (pg3 is not None and pg3 > 0) else (pg5 if pg5 and pg5 > 0 else None)
    if pe and pe > 0 and growth_for_peg and growth_for_peg > 0:
        result["peg_ratio"] = round(pe / growth_for_peg, 2)

    if pe and pe > 0:
        result["earnings_yield"] = round(100.0 / pe, 2)

    if cp and bv and bv > 0:
        result["pb_ratio"] = round(cp / bv, 2)

    if eps and eps > 0 and bv and bv > 0:
        try:
            graham = math.sqrt(22.5 * eps * bv)
            result["graham_number"] = round(graham, 2)
            if cp and cp > 0:
                result["graham_mos"] = round((graham - cp) / cp * 100, 1)
        except Exception:
            pass

    if cfo is not None and mc and mc > 0:
        result["cfo_yield"] = round(cfo / mc * 100, 2)

    fii = result.get("fii_holding")
    dii = result.get("dii_holding")
    if fii is not None and dii is not None:
        result["inst_holding"] = round(fii + dii, 2)
    elif fii is not None:
        result["inst_holding"] = fii

    sg3  = result.get("sales_growth_pct")
    sg10 = result.get("sales_growth_10y")
    if sg3 is not None and sg10 is not None:
        result["sales_growth_avg"] = round((sg3 + sg10) / 2, 1)

    # -- Content-change detection (exclude _ts from comparison) --------------
    existing = _fund_data.get(ticker, {})
    content_changed = False
    # Check if any new field differs from cached value
    for k, v in result.items():
        if existing.get(k) != v:
            content_changed = True
            break
    # Check if any cached field was dropped in the new fetch
    if not content_changed:
        for k in existing:
            if k == "_ts":
                continue
            if k not in result:
                content_changed = True
                break

    result["_ts"] = time.time()   # always refresh TTL in memory
    with _fund_data_lock:
        _fund_data[ticker] = result
    return result, content_changed


def _fund_bg_worker(tickers: list, generation: int = 0) -> None:
    """Parallel background worker  -  runs via run_in_executor.

    `generation` is captured from _fund_generation at launch time.  If the
    global counter advances while this worker is running (because Force Live
    Data was clicked again), the worker detects it at the next batch boundary
    and exits cleanly, preventing two workers from racing on the same dict.

    Performance optimisations vs the previous sequential version:
      1. ThreadPoolExecutor(5 workers): ~5× speedup over serial downloads.
      2. Known-fail skip (_gf flag): tickers that clearly failed hard gates
         during the last refresh use _FUND_FAIL_TTL (72 h) instead of the
         normal _FUND_CACHE_TTL (48 h). A stock that fails ROCE/D/E gates
         doesn't need to be re-checked every 48 h.
      3. Disk saves only when content actually changed mid-run; one final
         save always written so _ts persists across restarts.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    global _fund_bg_running, _fund_last_completed_ts
    MAX_WORKERS  = 12  # 12 parallel screener.in requests (raised from 10)
    BATCH_SIZE   = 50  # save checkpoint every N tickers
    total_refreshed = 0
    total_changed   = 0
    total_skipped   = 0
    now = time.time()

    # Separate stale tickers using per-ticker TTL:
    #   known-fail tickers (_gf flag) → use the longer FAIL TTL (skip them longer)
    #   normal tickers                → use standard CACHE TTL
    stale: list = []
    for t in tickers:
        entry  = _fund_data.get(t, {})
        ttl    = _FUND_FAIL_TTL if entry.get("_gf") else _FUND_CACHE_TTL
        if now - entry.get("_ts", 0) >= ttl:
            stale.append(t)
        else:
            total_skipped += 1

    if not stale:
        logger.info(
            "Fundamentals BG worker: all %d tickers fresh (%d known-fail skipped)  -  nothing to download",
            len(tickers), total_skipped,
        )
        _fund_bg_running = False
        return

    known_fail_count = sum(1 for t in stale if _fund_data.get(t, {}).get("_gf"))
    logger.info(
        "Fundamentals BG worker: %d stale (incl. %d known-fail) / %d total  -  "
        "parallel refresh with %d workers",
        len(stale), known_fail_count, len(tickers), MAX_WORKERS,
    )

    def _worker(t: str):
        """Download one ticker; returns (ticker, result, changed)."""
        result, changed = _fund_refresh_ticker(t)
        return t, result, changed

    changed_total = 0
    for batch_start in range(0, len(stale), BATCH_SIZE):
        if _fund_cancel.is_set() or _fund_generation != generation:
            logger.info("Fundamentals BG worker (gen %d): %s after %d/%d tickers",
                        generation,
                        "superseded by newer worker" if _fund_generation != generation else "cancelled (tab switched)",
                        total_refreshed, len(stale))
            _fund_cache_save()
            _fund_bg_running = False
            return

        chunk = stale[batch_start: batch_start + BATCH_SIZE]
        changed_in_batch = 0
        fetched_in_batch = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(_worker, t): t for t in chunk}
            for future in as_completed(futures):
                if _fund_cancel.is_set() or _fund_generation != generation:
                    # Cancel remaining futures and exit
                    for f in futures:
                        f.cancel()
                    break
                t = futures[future]
                try:
                    _, result, changed = future.result()

                    # Mark gate outcome on the cached entry so next cycle
                    # can apply the appropriate (longer) TTL for failures.
                    key_coverage = sum(
                        1 for v in [
                            result.get("roce"),
                            result.get("roe") or result.get("roe_5y"),
                            result.get("profit_growth_3y") or result.get("profit_growth_5y"),
                            result.get("sales_growth_pct") or result.get("sales_growth_5y"),
                        ]
                        if v is not None
                    )
                    with _fund_data_lock:
                        entry = _fund_data.get(t, {})
                        if key_coverage >= 3 and not _passes_fund_gates(result):
                            entry["_gf"] = True    # clearly failing — use long TTL next cycle
                        else:
                            entry.pop("_gf", None)  # passing or insufficient data — normal TTL

                    if any(k in result for k in ("roce", "roe", "promoter_holding")):
                        fetched_in_batch += 1
                    if changed:
                        changed_in_batch += 1

                except Exception as exc:
                    logger.debug("Fund BG worker %s: %s", t, exc)

        total_refreshed += fetched_in_batch
        total_changed   += changed_in_batch
        changed_total   += changed_in_batch

        end = min(batch_start + BATCH_SIZE, len(stale))
        if changed_in_batch:
            _fund_cache_save()
            logger.info("Fundamentals BG: %d/%d  -  %d fetched, %d changed -> saved",
                        end, len(stale), fetched_in_batch, changed_in_batch)
        else:
            logger.info("Fundamentals BG: %d/%d  -  %d fetched, no content changes",
                        end, len(stale), fetched_in_batch)

    # Always final save to persist updated _ts / _gf flags
    _fund_cache_save()
    _fund_last_completed_ts = time.time()
    logger.info(
        "Fundamentals BG worker done (gen %d): %d fetched, %d content-changed, "
        "%d known-fail skipped, final save written",
        generation, total_refreshed, total_changed, total_skipped,
    )
    _fund_bg_running = False


# ---------------------------------------------------------------------------
# Fundamentals hard-gate thresholds  (independent of swing-trade config.py)
# These are STRICT quality gates — NOT the liberal swing-trade values.
# ---------------------------------------------------------------------------
_FUND_ROCE_MIN        = 12.0   # ROCE ≥ 12%  (efficient use of capital)
_FUND_ROE_MIN         = 12.0   # ROE ≥ 12%   (return on shareholders equity)
_FUND_DE_MAX          =  1.0   # D/E ≤ 1.0   (conservative; swing trade uses 3.0 — NOT used here)
_FUND_PROFIT_GROW_MIN =  8.0   # 3Y profit CAGR ≥ 8%  (no loss-makers or stagnant earners)
_FUND_SALES_GROW_MIN  =  5.0   # 3Y revenue CAGR ≥ 5% (growing business)
_FUND_CFO_POSITIVE    = True   # Cash from Operations must be > 0  (real earnings, not just accounting profit)
_FUND_MIN_KEY_FIELDS  =  2     # require ≥ 2 key fields present or skip (avoid data-empty stubs)


def _passes_fund_gates(rec: dict) -> bool:
    """Hard fundamental quality gates — applied BEFORE scoring.

    Returns False if:
      - Fewer than _FUND_MIN_KEY_FIELDS key fields are present (data pending)
      - Any available field fails its strict threshold.

    Active gates:
      ROCE ≥ 12%          Capital efficiency
      ROE ≥ 12%           Return on equity
      D/E ≤ 1.0           Low financial leverage
      Profit Growth ≥ 8%  No loss-makers / stagnant earners
      Sales Growth ≥ 5%   Growing business
      CFO > 0             Positive operating cash flow (real earnings quality)

    NULL values = data not yet downloaded → gate not applied for that field
    (avoids incorrectly rejecting stocks currently being refreshed).
    """
    # Count present key fields; exclude stubs with no data at all
    key_vals = [
        rec.get("roce"),
        rec.get("roe") or rec.get("roe_5y") or rec.get("roe_10y"),
        rec.get("profit_growth_3y") or rec.get("profit_growth_5y"),
        rec.get("sales_growth_pct") or rec.get("sales_growth_5y"),
    ]
    if sum(1 for v in key_vals if v is not None) < _FUND_MIN_KEY_FIELDS:
        return False   # too little data — skip

    # ROCE gate
    roce = rec.get("roce")
    if roce is not None and float(roce) < _FUND_ROCE_MIN:
        return False

    # ROE gate (prefer longest history)
    roe = rec.get("roe_10y") or rec.get("roe_5y") or rec.get("roe")
    if roe is not None and float(roe) < _FUND_ROE_MIN:
        return False

    # D/E gate — STRICT (1.0 max, not the liberal 3.0 used for swing trade)
    de = rec.get("debt_equity")
    if de is not None and float(de) > _FUND_DE_MAX:
        return False

    # Profit growth gate — rejects loss-makers and stagnant earnings
    pg = rec.get("profit_growth_3y") or rec.get("profit_growth_5y")
    if pg is not None and float(pg) < _FUND_PROFIT_GROW_MIN:
        return False

    # Sales (revenue) growth gate — rejects contracting / stagnant businesses
    sg = rec.get("sales_growth_pct") or rec.get("sales_growth_5y")
    if sg is not None and float(sg) < _FUND_SALES_GROW_MIN:
        return False

    # Cash Flow gate — rejects cash-burning businesses (negative operating cash flow)
    # A company showing accounting profit but generating negative operating cash is
    # a warning sign; only companies with CFO > 0 demonstrate real earnings quality.
    if _FUND_CFO_POSITIVE:
        cfo = rec.get("cash_from_operations")
        if cfo is not None and float(cfo) <= 0:
            return False

    return True


def _fund_quality_score(rec: dict) -> float:
    """12-factor fundamental quality score for high-quality growth stocks.

    Hard gates (_passes_fund_gates) are applied first; only passing stocks
    reach this function, so neutral defaults for missing data are set to 0
    (no free credits — missing data contributes nothing to the rank).

    +- Quality (28 pts) -----------------------------------------------------+
    |  ROCE             15 pts  Capital efficiency (penalises high-debt firms)|
    |  ROE consistency   8 pts  Long-term avg ROE shows durable moat          |
    |  Promoter holding  5 pts  Management skin in the game                   |
    +- Debt & Liquidity (15 pts) -------------------------------------------- |
    |  Debt / Equity    10 pts  Low debt = financial resilience (max 1.0)     |
    |  Current Ratio     5 pts  Short-term solvency (>2.0 = healthy)          |
    +- Cash Flow (12 pts) ---------------------------------------------------- |
    |  CFO Yield        12 pts  Operating cash as % of Mkt Cap (real earnings)|
    +- Growth (20 pts) -------------------------------------------------------- |
    |  Revenue growth   10 pts  Avg of 3Y + 10Y sales CAGR                   |
    |  Profit growth     5 pts  3Y earnings CAGR                             |
    |  Inst. confidence  5 pts  FII + DII combined (smart money)             |
    +- Value (15 pts) ------------------------------------------------------- |
    |  PEG ratio        10 pts  Growth At Reasonable Price (< 1 = cheap)     |
    |  Earnings yield    5 pts  Inverse of PE (intrinsic cheapness)          |
    +- Intrinsic Value (10 pts) ---------------------------------------------|
    |  Graham MOS        7 pts  Price vs Graham Number margin of safety      |
    |  Market cap        3 pts  Size / stability proxy                       |
    +------------------------------------------------------------------------+
    Max base = 100 pts   Bonus: dividend yield (+2) + 10Y profit growth (+2)
    """
    score = 0.0

    # -- Quality block ---------------------------------------------------------

    # ROCE (Return on Capital Employed)  -  15 pts
    # Gate ensures ROCE ≥ 12%; scoring starts meaningfully at that floor.
    # 12% → ~3.6 pts, 25% → 7.5 pts, 50% → 15 pts (full)
    roce = rec.get("roce")
    if roce is not None:
        score += min(15.0, max(0.0, float(roce) * 0.30))
    # missing ROCE: 0 pts (hard gate already required it if available)

    # ROE consistency: prefer 10-year avg, fallback to current ROE  -  8 pts
    # 12% → ~3.8 pts, 20% → 6.4 pts, 25% → 8 pts (full)
    roe_ref = rec.get("roe_10y") or rec.get("roe_5y") or rec.get("roe")
    if roe_ref is not None:
        score += min(8.0, max(0.0, float(roe_ref) * 0.32))
    # missing ROE: 0 pts

    # Promoter holding  -  5 pts
    ph = rec.get("promoter_holding")
    if ph is not None:
        ph = float(ph)
        if ph >= 65:    score += 5.0
        elif ph >= 55:  score += 4.0
        elif ph >= 45:  score += 3.0
        elif ph >= 35:  score += 2.0
        elif ph >= 25:  score += 1.0
    # missing promoter: 0 pts

    # -- Debt & Liquidity block ------------------------------------------------

    # Debt / Equity ratio  -  10 pts
    # Gate ensures D/E ≤ 1.0; scoring rewards lower debt heavily.
    # 0 (debt-free) = 10 pts, 0.25 = 9, 0.50 = 7.5, 0.75 = 6, 1.0 = 4.5
    de = rec.get("debt_equity")
    if de is not None:
        de = float(de)
        if de == 0.0:       score += 10.0   # truly debt-free
        elif de <= 0.25:    score += 9.0    # very low debt
        elif de <= 0.50:    score += 7.5    # low debt
        elif de <= 0.75:    score += 6.0    # moderate
        elif de <= 1.00:    score += 4.5    # at gate threshold
        # > 1.0: 0 pts (gate rejects these)
    # missing D/E: 0 pts — let ROCE/ROE carry the quality signal

    # Current Ratio  -  5 pts  (>2 = healthy, <1 = risk)
    cr = rec.get("current_ratio")
    if cr is not None:
        cr = float(cr)
        if cr >= 2.5:    score += 5.0    # very liquid
        elif cr >= 2.0:  score += 4.0
        elif cr >= 1.5:  score += 3.0
        elif cr >= 1.0:  score += 1.5    # just above water
        # < 1.0: 0 pts
    # missing CR: 0 pts

    # -- Cash Flow block -------------------------------------------------------

    # CFO Yield = Cash from Operations / Market Cap x 100  -  12 pts
    cfo_y = rec.get("cfo_yield")
    if cfo_y is not None:
        cfo_y = float(cfo_y)
        if cfo_y >= 10.0:   score += 12.0   # exceptional cash generation
        elif cfo_y >= 6.0:  score += 10.0
        elif cfo_y >= 4.0:  score += 8.0
        elif cfo_y >= 2.0:  score += 6.0
        elif cfo_y >= 0.5:  score += 3.0    # marginally positive
        elif cfo_y >= 0.0:  score += 1.0    # breakeven
        # < 0: 0 pts  -  cash-burning
    # missing CFO: 0 pts

    # -- Growth block ----------------------------------------------------------

    # Revenue growth: composite of 3Y and 10Y CAGR  -  10 pts
    # Gate ensures sg ≥ 5%; scoring starts meaningfully above that floor.
    sg   = rec.get("sales_growth_pct") or rec.get("sales_growth_ttm")
    sg10 = rec.get("sales_growth_10y")
    if sg is not None and sg10 is not None:
        sg_composite = (float(sg) * 0.6 + float(sg10) * 0.4)
        score += min(10.0, max(0.0, sg_composite * 0.40))        # 25% avg = 10 pts
    elif sg is not None:
        score += min(8.0, max(0.0, float(sg) * 0.32))
    elif sg10 is not None:
        score += min(6.0, max(0.0, float(sg10) * 0.24))
    # missing sales growth: 0 pts

    # Profit growth 3Y  -  5 pts
    # Gate ensures pg ≥ 8%; scoring rewards higher growth more.
    pg = rec.get("profit_growth_3y") or rec.get("profit_growth_5y")
    if pg is not None:
        score += min(5.0, max(0.0, float(pg) * 0.20))           # 25% = 5 pts
    # missing profit growth: 0 pts

    # Institutional confidence (FII + DII)  -  5 pts
    inst = rec.get("inst_holding")
    if inst is not None:
        if inst >= 30:    score += 5.0
        elif inst >= 20:  score += 4.0
        elif inst >= 10:  score += 3.0
        elif inst >= 5:   score += 2.0
        else:             score += 1.0
    # missing inst: 0 pts

    # -- Value block -----------------------------------------------------------

    # PEG ratio  -  10 pts   (PEG < 1 = undervalued growth)
    peg = rec.get("peg_ratio")
    if peg is not None:
        peg = float(peg)
        if peg <= 0.5:    score += 10.0
        elif peg <= 1.0:  score += 8.0
        elif peg <= 1.5:  score += 5.5
        elif peg <= 2.0:  score += 3.0
        elif peg <= 3.0:  score += 1.0
        # > 3.0: 0 pts
    # missing PEG: 0 pts (PEG often unavailable; ROCE/ROE still rank quality)

    # Earnings yield  -  5 pts
    ey = rec.get("earnings_yield")
    if ey is not None:
        score += min(5.0, max(0.0, float(ey) * 0.33))   # 15% ey = 5 pts
    # missing EY: 0 pts

    # -- Intrinsic value block -------------------------------------------------

    # Graham Margin of Safety  -  7 pts
    mos = rec.get("graham_mos")
    if mos is not None:
        mos = float(mos)
        if mos >= 60:     score += 7.0
        elif mos >= 40:   score += 5.5
        elif mos >= 25:   score += 4.0
        elif mos >= 10:   score += 2.5
        elif mos >= 0:    score += 1.0
        elif mos >= -20:  score += 0.5
    # missing MOS: 0 pts

    # Market cap stability  -  3 pts
    mc = float(rec.get("market_cap_cr") or 0)
    if mc >= 1_00_000:  score += 3.0
    elif mc >= 50_000:  score += 2.5
    elif mc >= 20_000:  score += 2.0
    elif mc >= 5_000:   score += 1.5
    elif mc >= 1_000:   score += 1.0
    else:               score += 0.5

    # -- Bonus points ---------------------------------------------------------

    # Dividend yield bonus (up to +2 pts)
    dy = rec.get("dividend_yield")
    if dy is not None and float(dy) > 0:
        score += min(2.0, float(dy) * 0.5)   # 4% div = +2 pts

    # Long-term profit growth bonus (10Y CAGR)
    pg10 = rec.get("profit_growth_10y")
    if pg10 is not None and float(pg10) > 0:
        score += min(2.0, float(pg10) * 0.08)  # 25% 10Y CAGR = +2 pts

    return round(min(100.0, max(0.0, score)), 2)


def _compute_stop_loss_from_cache(ticker: str, cp: float):
    """Compute ATR14-based structural stop loss from OHLCV disk cache.

    Uses the same bounded formula as _analyze() / _analyze_momentum():
        stop = clamp(max(last_low, 3-bar_low, cp − 1×ATR14),
                     [cp − 1.5×ATR14, cp − 0.5×ATR14])

    Returns (stop_loss, atr14) rounded to 2 dp, or (None, None) on any failure.
    No network calls — reads only from the local .pkl cache on disk.
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
        cl         = float(lo.iloc[-1])
        sl_recent  = float(lo.iloc[-4:-1].min()) if len(lo) >= 4 else cl
        sl_struct  = max(cl, sl_recent)
        sl_cand    = max(sl_struct, cp - 1.0 * atr14_val)
        stop_loss  = round(min(max(sl_cand, cp - 1.5 * atr14_val), cp - 0.5 * atr14_val), 2)
        return stop_loss, round(atr14_val, 2)
    except Exception:
        return None, None


@app.get("/api/fundamentals")
async def get_fundamentals(refresh: int = 0) -> JSONResponse:
    """
    Top 30 stocks by strict fundamental quality from Nifty500 + Microcap250 tickers.

    Hard gates (applied before scoring):
      ROCE ≥ 12%   ROE ≥ 12%   D/E ≤ 1.0   ProfitGrowth3Y ≥ 8%   SalesGrowth ≥ 5%
      CashFromOperations > 0  (positive operating cash flow — real earnings quality)

    Performance notes:
      - Universe: Nifty500 + Microcap250 = 750 tickers (full combined list).
      - Background worker uses 5 parallel threads (was sequential) → ~5–8× speedup.
      - Known-fail tickers (_gf flag) skip re-download for 72 h instead of 48 h;
        stocks that clearly fail gates (ROCE/D/E/growth) are not checked again for 3 days.
      - Result is served instantly from disk cache; stale entries refresh in background.

    Uses a disk cache (cache/fundamentals_data.json) that persists across restarts.
    Pass ?refresh=1 to force an immediate background refresh.
    """
    global _fund_bg_running, _fund_result_cache_body, _fund_result_cache_valid, _fund_last_completed_ts

    # Fundamentals tab covers Nifty500 + Microcap250 (combined de-duped universe).
    # Performance is maintained via 10-worker parallel downloads + known-fail skip TTL
    # so the extra 250 MC tickers no longer cause a 1-hour scan.
    all_tickers: list = list(dict.fromkeys(NIFTY500_TICKERS + NIFTY_MICROCAP250_TICKERS))
    now = time.time()

    # If ?refresh=1  -  only reset entries that are older than _FUND_FORCE_REFRESH_TTL (4 h).
    # This ensures "Refresh" is delta-only: recently-fetched data is kept as-is and the
    # background worker only re-downloads entries that are genuinely due for a refresh.
    # (Setting ALL timestamps to 0 would force a 750-ticker full re-download every click.)
    if refresh:
        for t in all_tickers:
            if t in _fund_data:
                age = now - _fund_data[t].get("_ts", 0)
                if age > _FUND_FORCE_REFRESH_TTL:
                    _fund_data[t]["_ts"] = 0.0
        _fund_result_cache_valid = False   # force fresh result on manual refresh

    # Map scan results by ticker (provides tech score, fresh D/E, market_cap)
    scan_map: dict = {}
    for s in list(scan_state.get("data") or []) + list(mc_scan_state.get("data") or []):
        key = (s.get("ticker") or s.get("display_ticker", "")).upper()
        if key:
            scan_map[key] = s

    # Count stale entries and trigger background refresh if not already running.
    # Use per-ticker TTL: known-fail tickers use the longer FAIL TTL.
    stale = [
        t for t in all_tickers
        if now - _fund_data.get(t, {}).get("_ts", 0)
           > (_FUND_FAIL_TTL if _fund_data.get(t, {}).get("_gf") else _FUND_CACHE_TTL)
    ]
    # When all tickers are fresh from disk cache (no BG worker needed), stamp the
    # completed timestamp so the UI shows "all fresh/from cache" correctly.
    if not stale and not _fund_bg_running and _fund_last_completed_ts == 0 and len(all_tickers) > 0:
        _fund_last_completed_ts = time.time()

    if stale and not _fund_bg_running:
        _fund_bg_running = True
        _fund_cancel.clear()   # allow this worker to run (user is on fund tab)
        # Prioritise: passing-gate tickers > scan-active tickers > known-fail tickers
        priority_first = sorted(
            stale,
            key=lambda t: (
                1 if _fund_data.get(t, {}).get("_gf") else 0,  # known-fail last
                0 if t.upper() in scan_map else 1,              # in-scan-map first
            ),
        )
        gen = _fund_generation   # capture generation so worker can detect superseding

        async def _bg_task(tickers=priority_first, g=gen):
            global _fund_bg_running
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, _fund_bg_worker, tickers, g)
            except Exception:
                _fund_bg_running = False
        asyncio.create_task(_bg_task())

    cache_fresh = len(all_tickers) - len(stale)

    # ------------------------------------------------------------------
    # Return stable cached results when available (cache is invalidated by
    # _fund_cache_save() each time a new batch of data is written to disk).
    # This guarantees consistent results on every page-open within the same
    # refresh cycle, eliminating the "different results each visit" issue.
    # Dynamic status fields (bg_running, stale_count, cache_fresh) are
    # always refreshed so the UI accurately reflects background progress.
    # ------------------------------------------------------------------
    if not refresh and _fund_result_cache_valid and _fund_result_cache_body is not None:
        live_body = dict(_fund_result_cache_body)
        live_body["bg_running"]        = _fund_bg_running
        live_body["stale_count"]       = len(stale)
        live_body["cache_fresh"]       = cache_fresh
        live_body["last_completed_ts"] = _fund_last_completed_ts
        return JSONResponse(live_body)

    # Build combined record for every ticker that has at least some cached data
    combined = []
    for ticker in all_tickers:
        key        = ticker.upper()
        cache_rec  = _fund_data.get(key) or _fund_data.get(ticker) or {}
        scan_rec   = scan_map.get(key, {})

        # Skip tickers with zero data in both sources
        if not cache_rec and not scan_rec:
            continue

        rec: dict = {
            "ticker":         ticker,
            "display_ticker": key.replace(".NS", "").replace(".BO", ""),
        }

        # Pull ALL cached fundamentals into the record
        for k in ("sector", "debt_equity", "market_cap_cr", "current_price",
                  "roce", "roe", "roe_5y", "roe_10y",
                  "promoter_holding", "fii_holding", "dii_holding", "inst_holding",
                  "sales_growth_pct", "sales_growth_5y", "sales_growth_10y",
                  "sales_growth_ttm",
                  "profit_growth_3y", "profit_growth_5y", "profit_growth_10y",
                  "profit_growth_ttm",
                  "pe_ratio", "peg_ratio", "earnings_yield",
                  "book_value", "pb_ratio", "dividend_yield",
                  "graham_number", "graham_mos", "sales_growth_avg",
                  "current_ratio", "cash_from_operations", "cfo_yield",
                  "opm", "net_profit_margin"):
            v = cache_rec.get(k)
            if v is not None:
                rec[k] = v

        # Scan results add technical score + live price fields.
        # NOTE: debt_equity and market_cap_cr are intentionally NOT overridden
        # here — the fundamentals hard gate (_FUND_DE_MAX = 1.0) is calibrated
        # against Screener.in balance-sheet D/E.  The swing scan fetches D/E
        # from Yahoo Finance / NSE live API (different formula — TTM vs latest BS),
        # so overriding with scan D/E causes inconsistent gate results between
        # local (scan finished) and Render (scan still running).  Screener.in is
        # the authoritative source for all hard-gate fields on this tab.
        for k in ("sector", "score", "rsi", "return_20d", "rs_outperf_pct", "price"):
            v = scan_rec.get(k)
            if v is not None:
                rec[k] = v
        # market_cap_cr: use scan value ONLY as a fallback when Screener.in has no data
        if "market_cap_cr" not in rec and scan_rec.get("market_cap_cr") is not None:
            rec["market_cap_cr"] = scan_rec["market_cap_cr"]
        if scan_rec.get("display_ticker"):
            rec["display_ticker"] = scan_rec["display_ticker"]

        # Sync current_price from scan result if not in cache
        if "price" in rec and "current_price" not in rec:
            rec["current_price"] = rec["price"]

        # Stop loss — compute from OHLCV disk cache (no network)
        cp_val = rec.get("current_price") or rec.get("price")
        if cp_val:
            sl_val, atr_val = _compute_stop_loss_from_cache(ticker, float(cp_val))
            if sl_val is not None:
                rec["stop_loss"] = sl_val
                rec["atr14"]     = atr_val

        rec["fund_score"] = _fund_quality_score(rec)
        combined.append(rec)

    # Apply hard fundamental quality gates + minimum score threshold (≥ 50)
    qualified = [
        r for r in combined
        if _passes_fund_gates(r) and r.get("fund_score", 0) >= _FUND_MIN_SCORE
    ]
    qualified.sort(key=lambda x: x["fund_score"], reverse=True)
    top30 = qualified[:30]
    for i, s in enumerate(top30, 1):
        s["fund_rank"] = i

    logger.info("Fundamentals: %d total records, %d passed quality gates + score≥%.0f, showing top %d",
                len(combined), len(qualified), _FUND_MIN_SCORE, len(top30))

    response_body = {
        "stocks":             top30,
        "all_stocks":         qualified,
        "total":              len(qualified),
        "total_scored":       len(combined),
        "status":             "complete" if qualified else "no_data",
        "cache_fresh":        cache_fresh,
        "cache_total":        len(all_tickers),
        "bg_running":         _fund_bg_running,
        "stale_count":        len(stale),
        "last_completed_ts":  _fund_last_completed_ts,
        "last_updated_n500":  scan_state.get("last_updated"),
        "last_updated_mc250": mc_scan_state.get("last_updated"),
        "gates": {
            "roce_min":         _FUND_ROCE_MIN,
            "roe_min":          _FUND_ROE_MIN,
            "de_max":           _FUND_DE_MAX,
            "profit_growth_min":_FUND_PROFIT_GROW_MIN,
            "sales_growth_min": _FUND_SALES_GROW_MIN,
            "cfo_positive":     _FUND_CFO_POSITIVE,
            "min_score":        _FUND_MIN_SCORE,
        },
    }
    # Store result in cache so identical results are returned until next data batch
    _fund_result_cache_body  = response_body
    _fund_result_cache_valid = True
    return JSONResponse(response_body)


# -- SME Fundamentals ---------------------------------------------------------
# Tracks NSE Emerge + BSE SME stocks, same quality scoring as main fund tab,
# with an extra "exchange" field ("NSE Emerge" | "BSE SME") per record.

_SME_FUND_CACHE_FILE   = _PL("cache/sme_fundamentals_data.json")
_SME_FUND_CACHE_TTL    = 48 * 3600    # 48 h (same as main fund tab)
_SME_FAIL_TTL          = 72 * 3600    # 72 h for known-fail tickers
_sme_fund_data: dict   = {}           # {ticker: {exchange, fund_score, ...}}
_sme_fund_lock         = threading.Lock()  # protects concurrent writes
_sme_bg_running: bool  = False
_sme_universe: dict    = {}           # {ticker: "NSE Emerge" | "BSE SME"}  -  built at startup

# Result cache for /api/sme/fundamentals (same invalidation pattern as _fund_result_cache)
_sme_result_cache_body:  "dict | None" = None
_sme_result_cache_valid: bool          = False

# SME high-growth gate thresholds (stricter on growth, relaxed on debt vs Nifty500)
_SME_ROCE_MIN         = 15.0   # %  — capital efficiency gate
_SME_ROE_MIN          = 15.0   # %  — equity return gate
_SME_DE_MAX           =  1.5   # ratio — SME firms may carry growth capex debt
_SME_PROFIT_GROW_MIN  = 25.0   # % 3Y CAGR — only genuine high-growth companies
_SME_SALES_GROW_MIN   = 25.0   # % 3Y CAGR
_SME_OPM_MIN          =  8.0   # % operating margin — core business must be profitable
_SME_TTM_GROW_MIN     = 15.0   # % TTM growth (profit or sales) — recent order book momentum

# ── Composite Cash Quality gate (detects fake/junk companies) ──────────────
# Instead of hard "CFO > 0", we use three composite checks that allow
# genuine growth companies while still filtering out accounting fraud.
_SME_CCR_MIN       = -1.0   # Cash Conversion Ratio floor.
                             #   CCR = CFO / Net_Profit_Est (MCap÷PE)
                             #   CCR < -1.0 = company burns MORE cash than profits claim.
                             #   Classic Indian SME fraud: inflated profits, terrible cash flow.
_SME_CF_DEBT_MIN   = -0.5   # CF/Debt ratio floor (only applied when D/E > 0.5).
                             #   CF/Debt < -0.5 = CFO is -50% of total debt → cannot service obligations.
_SME_CFO_DEEP_NEG  = -30.0  # "Deeply negative" CFO threshold (₹Cr).
                             #   Below this, OPM ≥ 8% must be confirmed; else real operating loss.
_SME_MIN_KEY_FIELDS   =  2  # minimum key fields required before applying gates


def _sme_cache_load() -> None:
    global _sme_fund_data
    try:
        if _SME_FUND_CACHE_FILE.exists():
            _sme_fund_data = json.loads(_SME_FUND_CACHE_FILE.read_text(encoding="utf-8"))
            invalidated = 0
            for entry in _sme_fund_data.values():
                if isinstance(entry, dict) and entry.get("_ts", 0) > 0:
                    if "roce" not in entry or not any(k in entry for k in
                                                      ("cash_from_operations", "cfo_yield")) \
                                             or not any(k in entry for k in ("opm", "net_profit_margin")):
                        entry["_ts"] = 0
                        invalidated += 1
            logger.info("SME fund cache loaded: %d entries (%d invalidated)", len(_sme_fund_data), invalidated)
        else:
            _sme_fund_data = {}
    except Exception as exc:
        logger.warning("Could not load SME fund cache: %s", exc)
        _sme_fund_data = {}


def _sme_cache_save() -> None:
    global _sme_result_cache_valid
    _sme_result_cache_valid = False   # new data → force result recompute on next request
    try:
        _SME_FUND_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _SME_FUND_CACHE_FILE.write_text(
            json.dumps(_sme_fund_data, ensure_ascii=False), encoding="utf-8"
        )
    except Exception as exc:
        logger.warning("SME fund cache save failed: %s", exc)


def _passes_sme_gates(rec: dict) -> bool:
    """Hard fundamental gates for SME/Emerge stocks — tuned for HIGH-GROWTH companies.

    Philosophy:
      • GROWTH is the primary criterion (25% 3Y CAGR floor).
      • CASH FLOW uses a COMPOSITE quality check — not a simple "CFO > 0".
        Small SMEs in scale-up phase may have negative CFO due to working-capital
        build or capex, but GENUINE companies still show healthy cash conversion.
        Three-layer cash check (positive CFO always passes; negative triggers):
          1. CCR (Cash Conversion Ratio = CFO / Net_Profit_Est): < -1.0 is a red flag
             — the company burns MORE cash than its profits are worth (suspected fraud).
          2. CF/Debt (CFO / Total_Debt): if D/E > 0.5 AND CF/Debt < -0.5, the company
             cannot cover debt obligations from operations → stress / default risk.
          3. Deep negative CFO without OPM anchor: CFO < -30Cr + OPM < 8% = real loss.
      • OPM ≥ 8% gate ensures core business is operationally profitable.
      • TTM gate checks for recent momentum (order book proxy).

    Active gates:
      ROCE ≥ 15%                   Capital efficiency
      ROE  ≥ 15%                   Return on equity
      D/E  ≤ 1.5                   Moderate leverage
      Profit Growth 3Y ≥ 25%      No stagnant earners
      Sales Growth  3Y ≥ 25%      Fast-growing revenue
      OPM  ≥ 8%   (if available)  Operationally profitable
      TTM  ≥ 15%  (if available)  Recent growth momentum
      Cash Quality (composite)     Fraud / junk filter — see above

    NULL values = data not yet downloaded → gate not applied for that field.
    """
    key_vals = [
        rec.get("roce"),
        rec.get("roe") or rec.get("roe_5y") or rec.get("roe_10y"),
        rec.get("profit_growth_3y") or rec.get("profit_growth_5y"),
        rec.get("sales_growth_pct") or rec.get("sales_growth_5y"),
    ]
    if sum(1 for v in key_vals if v is not None) < _SME_MIN_KEY_FIELDS:
        return False

    # ROCE gate
    roce = rec.get("roce")
    if roce is not None and float(roce) < _SME_ROCE_MIN:
        return False

    # ROE gate (prefer current for SME — history is short for young companies)
    roe = rec.get("roe") or rec.get("roe_5y") or rec.get("roe_10y")
    if roe is not None and float(roe) < _SME_ROE_MIN:
        return False

    # D/E gate
    de = rec.get("debt_equity")
    de_f = float(de) if de is not None else 0.0
    if de is not None and de_f > _SME_DE_MAX:
        return False

    # Profit growth gate (3Y CAGR ≥ 25%)
    pg = rec.get("profit_growth_3y") or rec.get("profit_growth_5y")
    if pg is not None and float(pg) < _SME_PROFIT_GROW_MIN:
        return False

    # Sales growth gate (3Y CAGR ≥ 25%)
    sg = rec.get("sales_growth_pct") or rec.get("sales_growth_5y")
    if sg is not None and float(sg) < _SME_SALES_GROW_MIN:
        return False

    # OPM gate — core business must be operationally profitable
    # Fall back to NPM (net profit margin) when OPM is unavailable (common for SME stocks
    # on Screener.in which reports "NPM last year" instead of OPM for many Emerge listings)
    opm = rec.get("opm") if rec.get("opm") is not None else rec.get("net_profit_margin")
    opm_f = float(opm) if opm is not None else None
    if opm_f is not None and opm_f < _SME_OPM_MIN:
        return False

    # TTM momentum gate (both TTM needed; rejects post-peak decelerators)
    ttm_pg = rec.get("profit_growth_ttm")
    ttm_sg = rec.get("sales_growth_ttm")
    if ttm_pg is not None and ttm_sg is not None:
        best_ttm = max(float(ttm_pg), float(ttm_sg))
        if best_ttm < _SME_TTM_GROW_MIN:
            return False

    # ── Composite Cash Quality Gate ──────────────────────────────────────────
    # Goal: reject fake/junk companies. Allow genuine growth-phase companies.
    # Positive CFO always passes. Only negative CFO triggers the checks below.
    cfo_val = rec.get("cash_from_operations")
    if cfo_val is not None:
        cfo_f = float(cfo_val)
        if cfo_f <= 0:
            pe_f  = float(rec.get("pe_ratio")      or 0)
            mc_f  = float(rec.get("market_cap_cr") or 0)

            # 1. Cash Conversion Ratio (CCR) — primary fraud detector
            #    CCR = CFO / Net_Profit_Est  where Net_Profit_Est = MCap / PE
            #    CCR < -1.0: burns MORE cash per year than its profits claim → red flag.
            #    Use pre-computed value if available; otherwise compute on-the-fly.
            ccr_val = rec.get("ccr")
            if ccr_val is None and pe_f > 0 and mc_f > 0:
                net_p = mc_f / pe_f
                if net_p > 0:
                    ccr_val = cfo_f / net_p
            if ccr_val is not None and float(ccr_val) < _SME_CCR_MIN:
                return False  # inflated-profit / fake-earnings red flag

            # 2. Cash Flow to Debt coverage
            #    Leveraged company (D/E > 0.5) burning cash significantly
            #    cannot service its debt obligations from operations.
            cf_debt_val = rec.get("cf_to_debt")
            if cf_debt_val is None and de_f > 0.5:
                bv_f = float(rec.get("book_value")    or 0)
                cp_f = float(rec.get("current_price") or 0)
                if bv_f > 0 and cp_f > 0 and mc_f > 0:
                    total_debt_cr = de_f * bv_f * mc_f / cp_f
                    if total_debt_cr > 0:
                        cf_debt_val = cfo_f / total_debt_cr
            if (cf_debt_val is not None
                    and float(cf_debt_val) < _SME_CF_DEBT_MIN
                    and de_f > 0.5):
                return False  # leveraged + cannot service debt from operations

            # 3. Deep negative CFO without OPM confirmation
            #    Very negative CFO (< -30Cr) AND no confirmed + OPM ≥ 8%
            #    = real operating loss, not just a timing issue.
            if cfo_f < _SME_CFO_DEEP_NEG and (opm_f is None or opm_f < _SME_OPM_MIN):
                return False

    return True


def _sme_quality_score(rec: dict) -> float:
    """Growth-acceleration score for SME/Emerge stocks.

    SME stocks are early-stage high-growth companies where GROWTH MOMENTUM and
    BUSINESS ACCELERATION are the primary predictors of future returns.
    The scoring places 55% weight on growth (3Y CAGR + TTM acceleration), which
    acts as a proxy for "excellent order book and strong forward projections".

    +- Growth CAGR (35 pts) ---------------------------------------------------+
    |  Profit Growth 3Y   18 pts  Gate ≥ 25%; 50% = 15 pts, 80%+ = 18 pts     |
    |  Revenue Growth 3Y  12 pts  Gate ≥ 25%; 50% = 10 pts, 60% = 12 pts      |
    |  OPM (op. margin)    5 pts  Business profitability quality proxy          |
    +- Growth Acceleration (20 pts) — "Order book / forward projections" -------+
    |  TTM Profit Growth  10 pts  Recent earnings momentum (gate ≥ 15%)         |
    |  TTM Revenue Growth  7 pts  Recent revenue pipeline (gate ≥ 15%)          |
    |  Acceleration bonus  3 pts  TTM growth > 3Y CAGR (+) = accelerating       |
    +- Quality (18 pts) -------------------------------------------------------+
    |  ROCE               10 pts  Capital efficiency (gate ≥ 15%)               |
    |  ROE                 5 pts  Return on equity                               |
    |  Promoter holding    3 pts  Founder conviction (very high bar for SME)     |
    +- Cash Flow (12 pts)  -------------------------------------------------------+
    |  CFO Yield           8 pts  Operating cash / MCap (positive = real profits)|
    |  CFO sign & scale    4 pts  Positive CFO bonus; negative CFO penalty       |
    +- Debt (8 pts) -----------------------------------------------------------+
    |  Debt / Equity       5 pts  Growth debt OK; rewards low-leverage           |
    |  Current Ratio       3 pts  Short-term solvency                            |
    +- Value (4 pts) ----------------------------------------------------------+
    |  PEG ratio           3 pts  Growth at reasonable price                     |
    |  Earnings Yield      1 pt   Inverse of PE                                  |
    +- Size (3 pts) -----------------------------------------------------------+
    |  Market Cap          3 pts  Stability / graduation path proxy              |
    +--------------------------------------------------------------------------+
    Max base = 100 pts
    Bonus: accel ≥ +20% over 3Y (+2), div yield (+1), LT 10Y track record (+1)
    """
    score = 0.0

    # -- Growth CAGR block (35 pts) -------------------------------------------

    # Profit Growth 3Y  -  18 pts  (gate ≥ 25%; sweet spot 50-80%)
    pg3 = rec.get("profit_growth_3y") or rec.get("profit_growth_5y")
    pg3_val = float(pg3) if pg3 is not None else None
    if pg3_val is not None:
        # 25%=7.5, 40%=12, 60%=16, 80%=18 (full)
        score += min(18.0, max(0.0, pg3_val * 0.225))

    # Revenue Growth 3Y  -  12 pts  (gate ≥ 25%; 50%=10, 60%=12 full)
    sg3 = rec.get("sales_growth_pct") or rec.get("sales_growth_5y")
    sg3_val = float(sg3) if sg3 is not None else None
    if sg3_val is not None:
        score += min(12.0, max(0.0, sg3_val * 0.20))

    # Operating Profit Margin  -  5 pts  (replaces hard CFO gate in quality sense)
    # OPM ≥ 8% (gate); scoring: 8%=2, 15%=3.75, 25%=5 (full)
    # Fall back to NPM when OPM is unavailable (common for SME/Emerge stocks on Screener.in)
    opm = rec.get("opm") if rec.get("opm") is not None else rec.get("net_profit_margin")
    if opm is not None:
        opm_f = float(opm)
        score += min(5.0, max(0.0, opm_f * 0.20))

    # -- Growth Acceleration block (20 pts) — "Order book / forward projections" --
    # TTM (trailing twelve months) growth captures the most recent momentum.
    # A stock with 25% 3Y CAGR but 60% TTM growth has a booming order book.

    ttm_pg = rec.get("profit_growth_ttm")
    ttm_sg = rec.get("sales_growth_ttm")

    ttm_pg_val = float(ttm_pg) if ttm_pg is not None else None
    ttm_sg_val = float(ttm_sg) if ttm_sg is not None else None

    # TTM Profit Growth  -  10 pts  (15%=3, 30%=6, 60%=10 full)
    if ttm_pg_val is not None:
        score += min(10.0, max(0.0, ttm_pg_val * 0.167))

    # TTM Revenue Growth  -  7 pts  (15%=2.6, 30%=5.3, 50%=7 full)
    if ttm_sg_val is not None:
        score += min(7.0, max(0.0, ttm_sg_val * 0.14))

    # Acceleration bonus  -  3 pts: TTM > 3Y CAGR = business is accelerating
    # (strong forward pipeline / order book)
    if ttm_pg_val is not None and pg3_val is not None:
        accel_pg = ttm_pg_val - pg3_val  # positive = accelerating profit growth
        if accel_pg >= 20:   score += 1.5   # big acceleration
        elif accel_pg >= 10: score += 1.0
        elif accel_pg >= 0:  score += 0.5
    if ttm_sg_val is not None and sg3_val is not None:
        accel_sg = ttm_sg_val - sg3_val  # positive = accelerating revenue
        if accel_sg >= 15:   score += 1.5   # big revenue acceleration
        elif accel_sg >= 7:  score += 1.0
        elif accel_sg >= 0:  score += 0.5

    # -- Quality block (18 pts) ------------------------------------------------

    # ROCE  -  10 pts  (gate ≥ 15%; 20%=5, 35%=8.75, 40%=10 full)
    roce = rec.get("roce")
    if roce is not None:
        score += min(10.0, max(0.0, float(roce) * 0.25))

    # ROE (current preferred for SME — short history)  -  5 pts
    roe_ref = rec.get("roe") or rec.get("roe_5y") or rec.get("roe_10y")
    if roe_ref is not None:
        score += min(5.0, max(0.0, float(roe_ref) * 0.20))

    # Promoter holding  -  3 pts  (higher bar for SME: 70%+ = full confidence)
    ph = rec.get("promoter_holding")
    if ph is not None:
        ph = float(ph)
        if ph >= 70:    score += 3.0
        elif ph >= 60:  score += 2.5
        elif ph >= 50:  score += 2.0
        elif ph >= 40:  score += 1.0

    # -- Cash Flow block (12 pts) ----------------------------------------------
    # No hard gate; rewarded if positive, penalised if deeply negative.
    # Sub-scores: CFO Yield (5) + Cash Conversion Ratio CCR (4) + CF/Debt (2) + Sign (1)

    cfo_abs = rec.get("cash_from_operations")
    pe_sc   = float(rec.get("pe_ratio")      or 0)
    mc_sc   = float(rec.get("market_cap_cr") or 0)
    de_sc   = float(rec.get("debt_equity")   or 0)

    # CFO Yield (CFO / MCap %)  —  5 pts
    cfo_y = rec.get("cfo_yield")
    if cfo_y is not None:
        cfo_y_f = float(cfo_y)
        if cfo_y_f >= 10.0:   score += 5.0
        elif cfo_y_f >= 6.0:  score += 4.0
        elif cfo_y_f >= 3.0:  score += 3.0
        elif cfo_y_f >= 1.0:  score += 2.0
        elif cfo_y_f >= 0.0:  score += 1.0
        elif cfo_y_f >= -2.0: score += 0.0   # marginally negative — capex phase
        else:                 score -= 1.5   # deeply negative yield — penalty

    # Cash Conversion Ratio (CCR = CFO / Net_Profit_Est)  —  4 pts
    # The higher the CCR, the more REAL the profit claims are.
    # This is the primary fake-profit detector: high ROCE but low CCR = suspect.
    ccr_v = rec.get("ccr")
    if ccr_v is None and pe_sc > 0 and mc_sc > 0 and cfo_abs is not None:
        net_p = mc_sc / pe_sc
        if net_p > 0:
            ccr_v = float(cfo_abs) / net_p
    if ccr_v is not None:
        ccr_f = float(ccr_v)
        if ccr_f >= 1.0:    score += 4.0   # cash > profits (conservative accounting)
        elif ccr_f >= 0.7:  score += 3.5   # excellent quality
        elif ccr_f >= 0.4:  score += 2.5   # good — most cash converts
        elif ccr_f >= 0.15: score += 1.5   # decent
        elif ccr_f >= 0.0:  score += 0.5   # breakeven conversion
        elif ccr_f >= -0.5: score += 0.0   # mildly negative — growth capex territory
        elif ccr_f >= -1.0: score -= 0.5   # concerning — borderline gate failure
        # < -1.0: gate already rejected; won't reach here

    # Cash Flow to Debt Ratio (CF/Debt)  —  2 pts (bonus for debt coverage from cash)
    cf_debt_v = rec.get("cf_to_debt")
    if cf_debt_v is None and de_sc > 0.1 and cfo_abs is not None:
        bv_sc = float(rec.get("book_value")    or 0)
        cp_sc = float(rec.get("current_price") or 0)
        if bv_sc > 0 and cp_sc > 0 and mc_sc > 0:
            total_debt_cr = de_sc * bv_sc * mc_sc / cp_sc
            if total_debt_cr > 0:
                cf_debt_v = float(cfo_abs) / total_debt_cr
    if cf_debt_v is not None:
        cf_d_f = float(cf_debt_v)
        if cf_d_f >= 0.5:    score += 2.0   # covers ≥50% of debt from operations
        elif cf_d_f >= 0.2:  score += 1.5   # covers ≥20%
        elif cf_d_f >= 0.0:  score += 1.0   # breakeven — not burning vs debt
        # negative: 0 pts (handled by penalty in CFO yield)

    # CFO sign/scale bonus  —  1 pt
    if cfo_abs is not None:
        cfo_f_sc = float(cfo_abs)
        if cfo_f_sc >= 30:    score += 1.0
        elif cfo_f_sc >= 10:  score += 0.5
        elif cfo_f_sc >= 0:   score += 0.25

    # -- Debt block (8 pts) ---------------------------------------------------

    # D/E ratio  -  5 pts  (gate ≤ 1.5; rewards low debt)
    de = rec.get("debt_equity")
    if de is not None:
        de = float(de)
        if de == 0.0:       score += 5.0    # debt-free
        elif de <= 0.25:    score += 4.5
        elif de <= 0.50:    score += 3.5
        elif de <= 0.75:    score += 2.5
        elif de <= 1.00:    score += 1.5
        elif de <= 1.50:    score += 0.5    # at gate threshold

    # Current Ratio  -  3 pts
    cr = rec.get("current_ratio")
    if cr is not None:
        cr = float(cr)
        if cr >= 2.5:    score += 3.0
        elif cr >= 2.0:  score += 2.5
        elif cr >= 1.5:  score += 2.0
        elif cr >= 1.0:  score += 1.0

    # -- Value block (4 pts) --------------------------------------------------

    # PEG  -  3 pts  (SME trade at premium; PEG < 1 is rare but rewarded)
    peg = rec.get("peg_ratio")
    if peg is not None:
        peg = float(peg)
        if peg <= 0.5:    score += 3.0
        elif peg <= 1.0:  score += 2.5
        elif peg <= 1.5:  score += 1.5
        elif peg <= 2.0:  score += 0.5

    # Earnings yield  -  1 pt
    ey = rec.get("earnings_yield")
    if ey is not None:
        score += min(1.0, max(0.0, float(ey) * 0.10))

    # -- Size block (3 pts) ---------------------------------------------------

    mc = float(rec.get("market_cap_cr") or 0)
    if mc >= 5_000:     score += 3.0
    elif mc >= 2_000:   score += 2.5
    elif mc >= 1_000:   score += 2.0
    elif mc >= 500:     score += 1.5
    elif mc >= 200:     score += 1.0
    else:               score += 0.5

    # -- Bonus points ---------------------------------------------------------

    # Big acceleration bonus (+2): TTM ≥ 3Y CAGR + 20pp — strong forward pipeline
    # (already partially captured above; this rewards top-tier accelerators)
    if ttm_pg_val is not None and pg3_val is not None and (ttm_pg_val - pg3_val) >= 20:
        score += 1.0
    if ttm_sg_val is not None and sg3_val is not None and (ttm_sg_val - sg3_val) >= 15:
        score += 1.0

    # Dividend yield (+1 pt max — rare for SME; signal of cash confidence)
    dy = rec.get("dividend_yield")
    if dy is not None and float(dy) > 0:
        score += min(1.0, float(dy) * 0.5)

    # Long track record bonus (+1 pt) — 10Y history for an SME = rare & valuable
    pg10 = rec.get("profit_growth_10y")
    if pg10 is not None and float(pg10) >= 20:
        score += 1.0

    return round(min(100.0, max(0.0, score)), 2)


def _sme_fund_refresh_ticker(ticker: str) -> tuple:
    """Fetch & cache fundamentals for one SME ticker.

    Uses SME-aware Screener.in scraping: tries regular URL first, then
    the '-SME' suffix URL used by NSE Emerge stocks
    (e.g. screener.in/company/TICKER-SME/).

    Returns (result, content_changed) same as _fund_refresh_ticker().
    Results are stored in the shared _fund_data cache.
    """
    from data_sources import fundamentals as _fc
    import math

    result: dict = {}
    try:
        sector, de_x100, mc_inr = _fc.get(ticker)
        if sector:
            result["sector"] = sector
        if de_x100 is not None:
            result["debt_equity"]   = round(float(de_x100) / 100, 2)
        if mc_inr is not None:
            result["market_cap_cr"] = round(float(mc_inr) / 1e7, 0)
    except Exception:
        pass

    try:
        # Use SME-aware scraper (also tries TICKER-SME URL variant)
        extra = _fc.get_extra_sme_fundamentals(ticker)
        for k in ("roce", "roe", "roe_5y", "roe_10y",
                  "promoter_holding", "fii_holding", "dii_holding",
                  "sales_growth_pct", "sales_growth_5y", "sales_growth_10y",
                  "profit_growth_3y", "profit_growth_5y", "profit_growth_10y",
                  "sales_growth_ttm", "profit_growth_ttm",
                  "pe_ratio", "book_value", "dividend_yield",
                  "debt_equity", "market_cap_cr", "current_price",
                  "current_ratio", "cash_from_operations",
                  "opm", "net_profit_margin"):
            if k in extra and extra[k] is not None:
                result[k] = extra[k]
    except Exception:
        pass

    # Derived metrics (same as _fund_refresh_ticker)
    pe   = result.get("pe_ratio")
    bv   = result.get("book_value")
    cp   = result.get("current_price")
    pg3  = result.get("profit_growth_3y")
    pg5  = result.get("profit_growth_5y")
    mc   = result.get("market_cap_cr")
    cfo  = result.get("cash_from_operations")

    growth_for_peg = pg3 if (pg3 is not None and pg3 > 0) else (pg5 if pg5 and pg5 > 0 else None)
    if pe and pe > 0 and growth_for_peg and growth_for_peg > 0:
        result["peg_ratio"] = round(pe / growth_for_peg, 2)

    if pe and pe > 0:
        result["earnings_yield"] = round(100.0 / pe, 2)

    if cp and bv and bv > 0:
        result["pb_ratio"] = round(cp / bv, 2)

    eps = (cp / pe) if (cp and pe and pe > 0) else None
    if eps and bv and bv > 0:
        try:
            gn = math.sqrt(22.5 * abs(eps) * abs(bv))
            result["graham_number"] = round(gn, 2)
            if cp and cp > 0:
                result["graham_mos"] = round((gn - cp) / cp * 100, 1)
        except Exception:
            pass

    if mc and cfo is not None:
        mc_inr2 = mc * 1e7
        if mc_inr2 > 0:
            result["cfo_yield"] = round(cfo * 1e7 / mc_inr2 * 100, 2)

    # Cash Conversion Ratio (CCR = CFO / Net_Profit_Estimate)
    # Net_Profit_Est ≈ MCap(Cr) ÷ PE  →  a higher net profit vs MCap means lower CCR if cash is negative.
    # CCR ≥ 1.0 = cash exceeds profit claims (exceptional quality / conservative accounting)
    # CCR < 0   = burning cash vs profits (needs context: capex vs fraud)
    # CCR < -1.0 = burning MORE cash than entire annual profit → RED FLAG (fraud detector)
    de_val = result.get("debt_equity")
    if cfo is not None and pe and float(pe) > 0 and mc and float(mc) > 0:
        net_profit_est = float(mc) / float(pe)
        if net_profit_est > 0:
            result["ccr"] = round(float(cfo) / net_profit_est, 2)

    # Cash Flow to Debt Ratio  (CF/Debt = CFO / Total_Debt_Cr)
    # Total_Debt_Cr ≈ D/E × (BV × MCap_Cr / CurrentPrice)
    #   [BV(Rs/share) × Shares = Total_Equity(Rs); Shares = MCap_Cr×1e7/CP;
    #    Total_Equity(Cr) = BV×MCap_Cr/CP; Total_Debt(Cr) = D/E × Total_Equity(Cr)]
    # Positive CF/Debt = able to pay down debt from operations (healthy)
    # Negative CF/Debt = debt grows faster than cash can cover (risk signal)
    if (cfo is not None and de_val and float(de_val) > 0
            and bv and float(bv) > 0 and cp and float(cp) > 0
            and mc and float(mc) > 0):
        try:
            total_debt_cr = float(de_val) * float(bv) * float(mc) / float(cp)
            if total_debt_cr > 0:
                result["cf_to_debt"] = round(float(cfo) / total_debt_cr, 2)
        except Exception:
            pass

    sg3  = result.get("sales_growth_pct")
    sg10 = result.get("sales_growth_10y")
    if sg3 is not None and sg10 is not None:
        result["sales_growth_avg"] = round((sg3 + sg10) / 2, 1)
    elif sg3 is not None:
        result["sales_growth_avg"] = sg3

    fii = result.get("fii_holding")
    dii = result.get("dii_holding")
    if fii is not None and dii is not None:
        result["inst_holding"] = round(fii + dii, 2)

    # Content-change detection against SME-specific cache (exclude _ts)
    existing = _sme_fund_data.get(ticker, {})
    content_changed = False
    for k, v in result.items():
        if existing.get(k) != v:
            content_changed = True
            break
    if not content_changed:
        for k in existing:
            if k == "_ts":
                continue
            if k not in result:
                content_changed = True
                break

    result["_ts"] = time.time()
    with _sme_fund_lock:
        _sme_fund_data[ticker] = result   # write to SME-specific cache dict
    return result, content_changed


def _sme_bg_worker(tickers: list, generation: int = 0) -> None:
    """Parallel background refresh for SME fundamentals.

    `generation` mirrors the same superseding mechanism as _fund_bg_worker.
    Uses SME-aware fetching (_sme_fund_refresh_ticker) which additionally
    tries the '-SME' URL variant on screener.in for NSE Emerge stocks.
    Data is stored in the SEPARATE _sme_fund_data cache (not _fund_data).

    Performance: 4 parallel workers; known-fail tickers use a 72 h skip TTL.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    global _sme_bg_running, _sme_last_completed_ts
    MAX_WORKERS = 12  # 12 parallel workers (raised from 10)
    BATCH_SIZE  = 50  # raised from 40 to match main fund worker
    total_refreshed = 0
    total_changed   = 0
    total_skipped   = 0
    now = time.time()

    stale: list = []
    for t in tickers:
        entry = _sme_fund_data.get(t, {})
        ttl   = _SME_FAIL_TTL if entry.get("_gf") else _SME_FUND_CACHE_TTL
        if now - entry.get("_ts", 0) >= ttl:
            stale.append(t)
        else:
            total_skipped += 1

    if not stale:
        logger.info("SME BG worker: all %d tickers fresh (%d known-fail skipped)  -  nothing to download",
                    len(tickers), total_skipped)
        _sme_bg_running = False
        return

    known_fail_count = sum(1 for t in stale if _sme_fund_data.get(t, {}).get("_gf"))
    logger.info("SME BG worker: %d stale (incl. %d known-fail) / %d total  -  parallel refresh %d workers",
                len(stale), known_fail_count, len(tickers), MAX_WORKERS)

    def _worker(t: str):
        result, changed = _sme_fund_refresh_ticker(t)
        return t, result, changed

    for batch_start in range(0, len(stale), BATCH_SIZE):
        if _sme_cancel.is_set() or _sme_generation != generation:
            logger.info("SME BG worker (gen %d): %s after %d/%d tickers",
                        generation,
                        "superseded by newer worker" if _sme_generation != generation else "cancelled (tab switched)",
                        total_refreshed, len(stale))
            _sme_cache_save()
            _sme_bg_running = False
            return

        chunk = stale[batch_start: batch_start + BATCH_SIZE]
        fetched_in_batch = 0
        changed_in_batch = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(_worker, t): t for t in chunk}
            for future in as_completed(futures):
                if _sme_cancel.is_set() or _sme_generation != generation:
                    for f in futures:
                        f.cancel()
                    break
                t = futures[future]
                try:
                    _, result, changed = future.result()

                    # Mark gate outcome for TTL management
                    key_coverage = sum(
                        1 for v in [
                            result.get("roce"),
                            result.get("roe") or result.get("roe_5y"),
                            result.get("profit_growth_3y") or result.get("profit_growth_5y"),
                            result.get("sales_growth_pct") or result.get("sales_growth_5y"),
                        ]
                        if v is not None
                    )
                    with _sme_fund_lock:
                        entry = _sme_fund_data.get(t, {})
                        if key_coverage >= 3 and not _passes_sme_gates(result):
                            entry["_gf"] = True
                        else:
                            entry.pop("_gf", None)

                    if any(k in result for k in ("roce", "roe", "promoter_holding")):
                        fetched_in_batch += 1
                    if changed:
                        changed_in_batch += 1

                except Exception as exc:
                    logger.debug("SME BG refresh error %s: %s", t, exc)

        total_refreshed += fetched_in_batch
        total_changed   += changed_in_batch
        end = min(batch_start + BATCH_SIZE, len(stale))

        if changed_in_batch:
            _sme_cache_save()
            logger.info("SME BG: %d/%d  -  %d fetched, %d changed -> saved",
                        end, len(stale), fetched_in_batch, changed_in_batch)
        else:
            logger.info("SME BG: %d/%d  -  %d fetched, no content changes",
                        end, len(stale), fetched_in_batch)

    _sme_cache_save()
    _sme_last_completed_ts = time.time()
    logger.info("SME BG worker done (gen %d): %d fetched, %d content-changed, %d known-fail skipped",
                generation, total_refreshed, total_changed, total_skipped)
    _sme_bg_running = False


# -- Stock Momentum ------------------------------------------------------------
# Momentum filter thresholds are now defined in scanner.py (MOM_*) and imported
# at the top of this file.  Aliases kept here for the criteria payload / logging.
_MOM_RSI_MIN   = MOM_RSI_MIN
_MOM_WRSI_MIN  = MOM_WRSI_MIN
_MOM_ADX_MIN   = MOM_ADX_MIN
_MOM_VOLZ_MIN  = MOM_VOLZ_MIN
_MOM_RS_MIN    = MOM_RS_MIN
_MOM_RET20_MIN = MOM_RET20_MIN
_MOM_RET5_MIN  = MOM_RET5_MIN
_MOM_TVMIN_CR  = MOM_TV_MIN_CR


def _mom_score(s: dict) -> float:
    """Pure Momentum Score  -  technicals only.  Max = 100 pts.

    Weights are tuned for better winning-rate predictability:

    RSI zone         (0 - 30)  -  primary momentum; RSI 50->80 linear
                                   (reduced from 35: extreme RSI>80 = overbought risk)
    RS outperformance(0 - 25)  -  #1 win-rate predictor — market leaders continue leading
    ADX strength     (0 - 20)  -  directional trend conviction; ADX 20->55 linear
    MACD histogram   (0 - 10)  -  momentum acceleration / confirmation
    Weekly RSI       (0 - 10)  -  higher-timeframe trend alignment; wRSI 50->70 linear
    Volume surge     (0 -  5)  -  institutional activity / confirmation only
    ─────────────────────────
    TOTAL MAX                100 pts
    """
    rsi   = float(s.get("rsi")           or 0)
    wrsi  = float(s.get("weekly_rsi")    or 0)
    adx   = float(s.get("adx")           or 0)
    vz    = float(s.get("vol_zscore")    or 0)
    rs    = float(s.get("rs_outperf_pct")or 0)
    mhist = float(s.get("macd_hist")     or 0)

    rsi_pts   = min(30.0, max(0.0, (rsi  - 50.0) * 1.0))      # RSI  50->80 -> 0->30
    wrsi_pts  = min(10.0, max(0.0, (wrsi - 50.0) * 0.5))      # wRSI 50->70 -> 0->10
    adx_pts   = min(20.0, max(0.0, (adx  - 20.0) * 0.571))    # ADX  20->55 -> 0->20
    rs_pts    = min(25.0, max(0.0, rs   * 2.5))                # RS   0->10% -> 0->25
    vol_pts   = min( 5.0, max(0.0, (vz  - 0.5)  * 2.5))       # volZ 0.5->2.5 -> 0->5
    macd_pts  = min(10.0, max(0.0, mhist * 200.0))             # hist 0->0.05 -> 0->10

    return round(rsi_pts + wrsi_pts + adx_pts + rs_pts + vol_pts + macd_pts, 2)


@app.get("/api/stock-momentum")
async def get_stock_momentum(
    date: str = Query(None, description="Historical date YYYY-MM-DD; omit for live data"),
) -> JSONResponse:
    """
    High-momentum subset of the N500 + MC250 universe.

    **Completely independent of Swing Trade.**  Uses a dedicated
    momentum-only scan (scanner.scan(momentum_only=True)) that applies
    only momentum criteria — no fundamentals, no EMA cross, no
    HH20 breakout, no closing range, no sector outperformance, no
    composite scoring.

    Pass ?date=YYYY-MM-DD for historical results.

    Filters (all must pass):
      RSI-14  >= 58    (early momentum zone — catches sooner than 62)
      RSI SMA-3 rising (momentum accelerating, not stalling)
      Weekly RSI >= 55 (weekly trend turning constructive)
      Weekly RSI rising (higher-timeframe trend pointing UP)
      ADX-14  >= 23    (trend establishing — catches earlier setups)
      ADX rising       (trend strengthening, not exhausting)
      Vol Z-score >= 0.8 (above-average accumulation volume)
      RS outperf >= 2.5% vs index (emerging market leadership)
      20D return >= 3% absolute (stock is moving)
      MACD > Signal AND MACD > 0 (bullish zone confirmation)
      MACD histogram positive AND not contracting > 30% (acceleration)
    """
    target_date = None
    if date:
        target_date, err = _validate_date_param(date)
        if err:
            return err

    if target_date:
        # Historical mode: run momentum-only scans for the requested date
        try:
            loop = asyncio.get_event_loop()
            await asyncio.gather(
                loop.run_in_executor(
                    None, lambda: scanner.scan(target_date=target_date, momentum_only=True)
                ),
                loop.run_in_executor(
                    None, lambda: scanner_mc.scan(target_date=target_date, momentum_only=True)
                ),
            )
        except Exception as exc:
            logger.error("Historical stock-momentum scan failed: %s", exc, exc_info=True)
            return JSONResponse({"error": str(exc), "stocks": [], "total": 0}, status_code=500)
        n500_stocks  = [dict(s, from_index="Nifty 500")    for s in (scanner.last_momentum_results    or [])]
        mc_stocks    = [dict(s, from_index="Microcap 250") for s in (scanner_mc.last_momentum_results or [])]
        n500_status  = "complete"
        mc250_status = "complete"
        overall_status = "complete"
        last_n500  = f"Historical data as of {date}"
        last_mc250 = f"Historical data as of {date}"
    else:
        # Live mode: read from the dedicated independent momentum scan state
        n500_stocks  = [dict(s, from_index="Nifty 500")    for s in (mom_scan_state.get("data")    or [])]
        mc_stocks    = [dict(s, from_index="Microcap 250") for s in (mc_mom_scan_state.get("data") or [])]
        n500_status  = mom_scan_state["status"]
        mc250_status = mc_mom_scan_state["status"]
        overall_status = (
            "scanning"     if n500_status in ("scanning", "initializing")
                              or mc250_status in ("scanning", "initializing") else
            "complete"     if (n500_status == "complete" or mc250_status == "complete") else
            "error"
        )
        last_n500  = mom_scan_state.get("last_updated")
        last_mc250 = mc_mom_scan_state.get("last_updated")

    all_stocks = n500_stocks + mc_stocks

    if all_stocks:
        logger.info(
            "Stock Momentum: %d stocks qualified "
            "(N500=%d, MC250=%d) — applying mom_score ranking",
            len(all_stocks),
            sum(1 for s in all_stocks if s.get("from_index") == "Nifty 500"),
            sum(1 for s in all_stocks if s.get("from_index") == "Microcap 250"),
        )
    else:
        logger.info(
            "Stock Momentum: no qualifying stocks yet "
            "(n500_status=%s, mc250_status=%s)",
            n500_status, mc250_status,
        )
        # Kick dedicated momentum scans on very first start (status == "initializing")
        # Guard uses "not in scanning" so a queued-but-not-yet-started task
        # (status already set to "scanning") is never duplicated.
        if not target_date:
            if n500_status not in ("scanning", "complete"):
                logger.info("Stock Momentum: kicking N500 momentum-only scan")
                asyncio.create_task(run_n500_momentum_scan())
            if mc250_status not in ("scanning", "complete"):
                logger.info("Stock Momentum: kicking MC250 momentum-only scan")
                asyncio.create_task(run_mc250_momentum_scan())

    # Compute momentum score and sort
    for s in all_stocks:
        s["mom_score"] = _mom_score(s)
        # Enrich with sector from fundamentals cache (best-effort — no blocking call)
        sym = s.get("display_ticker") or (s.get("ticker") or "").replace(".NS", "")
        fund = _fund_data.get(sym) or _fund_data.get(sym + ".NS") or {}
        if fund.get("sector"):
            s["sector"] = fund["sector"]
    all_stocks.sort(key=lambda x: x["mom_score"], reverse=True)
    filtered = all_stocks

    n500_count = sum(1 for s in filtered if s.get("from_index") == "Nifty 500")
    mc_count   = sum(1 for s in filtered if s.get("from_index") == "Microcap 250")

    return JSONResponse({
        "stocks":               filtered[:50],
        "total":                len(filtered),
        "n500_count":           n500_count,
        "mc_count":             mc_count,
        "n500_status":          n500_status,
        "mc250_status":         mc250_status,
        "last_updated_n500":    last_n500,
        "last_updated_mc250":   last_mc250,
        "status":               overall_status,
        "scan_stage":           mom_scan_state.get("scan_stage", "") if not target_date else "",
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
            "macd":       "MACD(12,26,9) > Signal AND MACD > 0",
        },
    })


@app.get("/api/morning-momentum")
async def get_morning_momentum(
    date: str = Query(None, description="Historical date YYYY-MM-DD; omit for live data"),
) -> JSONResponse:
    """
    Morning Momentum — checks ALL 750 tickers (N500 + MC250) for the 3-candle
    Morning Star bullish-reversal candlestick pattern.

    No momentum/swing criteria required — every ticker is tested.
    Historical date support uses cache-first with incremental (delta) downloads
    so date changes in the UI are fast after the initial cache warm-up.
    """
    target_date = None
    if date:
        target_date, err = _validate_date_param(date)
        if err:
            return err

    if target_date:
        # Historical mode: cache-first morning-star scan for the requested date
        try:
            loop = asyncio.get_event_loop()
            n500_results, mc_results = await asyncio.gather(
                loop.run_in_executor(
                    None, lambda: scanner.scan_morning_star(target_date=target_date)
                ),
                loop.run_in_executor(
                    None, lambda: scanner_mc.scan_morning_star(target_date=target_date)
                ),
            )
        except Exception as exc:
            logger.error("Historical morning-momentum scan failed: %s", exc, exc_info=True)
            return JSONResponse({"error": str(exc), "stocks": [], "total": 0}, status_code=500)
        n500_all     = [dict(s, from_index="Nifty 500")    for s in (n500_results or [])]
        mc_all       = [dict(s, from_index="Microcap 250") for s in (mc_results   or [])]
        n500_status  = "complete"
        mc250_status = "complete"
        overall_status = "complete"
        last_n500    = f"Historical data as of {date}"
        last_mc250   = f"Historical data as of {date}"
        scan_stage   = ""
    else:
        # Live mode: read from dedicated Morning Star scan state
        n500_all     = [dict(s, from_index="Nifty 500")    for s in (ms_scan_state.get("data")    or [])]
        mc_all       = [dict(s, from_index="Microcap 250") for s in (mc_ms_scan_state.get("data") or [])]
        n500_status  = ms_scan_state["status"]
        mc250_status = mc_ms_scan_state["status"]
        overall_status = (
            "scanning"  if n500_status in ("scanning", "initializing")
                           or mc250_status in ("scanning", "initializing") else
            "complete"  if (n500_status == "complete" or mc250_status == "complete") else
            "error"
        )
        last_n500  = ms_scan_state.get("last_updated")
        last_mc250 = mc_ms_scan_state.get("last_updated")
        scan_stage = (
            ms_scan_state.get("scan_stage", "") or mc_ms_scan_state.get("scan_stage", "")
        )

        # Kick Morning Star scans on very first visit (status still "initializing")
        if not n500_all and not mc_all:
            if n500_status not in ("scanning", "complete"):
                asyncio.create_task(run_n500_ms_scan())
            if mc250_status not in ("scanning", "complete"):
                asyncio.create_task(run_mc250_ms_scan())

    all_stocks = n500_all + mc_all

    # Sort by Morning Star score desc (best candidates first), then 20D return as tiebreaker
    all_stocks.sort(key=lambda x: (x.get("mom_score") or 0, x.get("return_20d") or 0), reverse=True)

    n500_count = sum(1 for s in all_stocks if s.get("from_index") == "Nifty 500")
    mc_count   = sum(1 for s in all_stocks if s.get("from_index") == "Microcap 250")

    logger.info(
        "Morning Momentum: %d Morning Star stocks (N500=%d, MC250=%d)",
        len(all_stocks), n500_count, mc_count,
    )

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


@app.get("/api/sme/fundamentals")
async def get_sme_fundamentals(refresh: int = 0) -> JSONResponse:
    """
    Top SME stocks ranked by high-growth fundamental quality.
    Universe: NSE Emerge + BSE SME IPO tickers.
    Uses a SEPARATE cache file (cache/sme_fundamentals_data.json).
    Gates: ROCE≥15%, ROE≥15%, D/E≤1.5, ProfitGrowth3Y≥25%, SalesGrowth≥25%, CFO>0.
    Each result includes an 'exchange' field: 'NSE Emerge' or 'BSE SME'.
    """
    global _sme_bg_running, _sme_universe, _sme_result_cache_body, _sme_result_cache_valid, _sme_last_completed_ts

    # Build universe on first call (cached in module-level dict after that)
    if not _sme_universe:
        from sme_tickers import build_sme_universe
        _sme_universe = build_sme_universe()

    all_tickers = list(_sme_universe.keys())
    now = time.time()

    # If ?refresh=1 — reset _ts so BG worker will re-fetch from Screener.in
    if refresh:
        for t in all_tickers:
            if t in _sme_fund_data:
                _sme_fund_data[t]["_ts"] = 0.0
        _sme_result_cache_valid = False   # force fresh result on manual refresh

    # Count stale entries and trigger background refresh if not already running.
    stale = [
        t for t in all_tickers
        if now - _sme_fund_data.get(t, {}).get("_ts", 0)
           > (_SME_FAIL_TTL if _sme_fund_data.get(t, {}).get("_gf") else _SME_FUND_CACHE_TTL)
    ]
    # When all tickers are fresh from disk cache (no BG worker needed), stamp the
    # completed timestamp so the UI shows "all fresh/from cache" correctly.
    if not stale and not _sme_bg_running and _sme_last_completed_ts == 0 and len(all_tickers) > 0:
        _sme_last_completed_ts = time.time()

    if stale and not _sme_bg_running:
        _sme_bg_running = True
        _sme_cancel.clear()   # allow this worker to run (user is on sme tab)
        gen = _sme_generation   # capture generation so worker can detect superseding

        async def _sme_bg_task(tickers=stale, g=gen):
            global _sme_bg_running
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, _sme_bg_worker, tickers, g)
            except Exception:
                _sme_bg_running = False
        asyncio.create_task(_sme_bg_task())

    cache_fresh = len(all_tickers) - len(stale)
    nse_count = sum(1 for v in _sme_universe.values() if v == "NSE Emerge")
    bse_count = sum(1 for v in _sme_universe.values() if v == "BSE SME")

    # ------------------------------------------------------------------
    # Return stable cached result (invalidated by _sme_cache_save() each
    # time a new batch of data is written to disk).
    # ------------------------------------------------------------------
    if not refresh and _sme_result_cache_valid and _sme_result_cache_body is not None:
        live_body = dict(_sme_result_cache_body)
        live_body["bg_running"]        = _sme_bg_running
        live_body["stale_count"]       = len(stale)
        live_body["cache_fresh"]       = cache_fresh
        live_body["last_completed_ts"] = _sme_last_completed_ts
        return JSONResponse(live_body)

    combined = []
    for ticker in all_tickers:
        key       = ticker.upper()
        # Read from SME-specific cache
        cache_rec = _sme_fund_data.get(key) or _sme_fund_data.get(ticker) or {}

        if not cache_rec:
            continue

        rec: dict = {
            "ticker":         ticker,
            "display_ticker": key.replace(".NS", "").replace(".BO", ""),
            "exchange":       _sme_universe.get(ticker, "NSE Emerge"),
        }

        for k in ("sector", "debt_equity", "market_cap_cr", "current_price",
                  "roce", "roe", "roe_5y", "roe_10y",
                  "promoter_holding", "fii_holding", "dii_holding", "inst_holding",
                  "sales_growth_pct", "sales_growth_5y", "sales_growth_10y",
                  "profit_growth_3y", "profit_growth_5y", "profit_growth_10y",
                  "sales_growth_ttm", "profit_growth_ttm",
                  "pe_ratio", "peg_ratio", "earnings_yield",
                  "book_value", "pb_ratio", "dividend_yield",
                  "graham_number", "graham_mos", "sales_growth_avg",
                  "current_ratio", "cash_from_operations", "cfo_yield",
                  "opm", "net_profit_margin"):
            v = cache_rec.get(k)
            if v is not None:
                rec[k] = v

        if "current_price" not in rec and rec.get("price"):
            rec["current_price"] = rec["price"]

        # Apply SME high-growth gates before scoring
        if not _passes_sme_gates(rec):
            continue

        # Stop loss — compute from OHLCV disk cache (no network)
        cp_sme = rec.get("current_price") or rec.get("price")
        if cp_sme:
            sl_sme, atr_sme = _compute_stop_loss_from_cache(ticker, float(cp_sme))
            if sl_sme is not None:
                rec["stop_loss"] = sl_sme
                rec["atr14"]     = atr_sme

        rec["fund_score"] = _sme_quality_score(rec)
        combined.append(rec)

    combined.sort(key=lambda x: x["fund_score"], reverse=True)
    top30 = combined[:30]
    for i, s in enumerate(top30, 1):
        s["sme_rank"] = i

    response_body = {
        "stocks":       top30,
        "all_stocks":         combined,
        "total":              len(combined),
        "status":             "complete" if combined else "no_data",
        "cache_fresh":        cache_fresh,
        "cache_total":        len(all_tickers),
        "bg_running":         _sme_bg_running,
        "stale_count":        len(stale),
        "last_completed_ts":  _sme_last_completed_ts,
        "nse_count":          nse_count,
        "bse_count":          bse_count,
        "gates": {
            "roce_min":         _SME_ROCE_MIN,
            "roe_min":          _SME_ROE_MIN,
            "de_max":           _SME_DE_MAX,
            "profit_grow_min":  _SME_PROFIT_GROW_MIN,
            "sales_grow_min":   _SME_SALES_GROW_MIN,
            "opm_min":          _SME_OPM_MIN,
            "ttm_grow_min":     _SME_TTM_GROW_MIN,
            "ccr_min":          _SME_CCR_MIN,
            "cf_debt_min":      _SME_CF_DEBT_MIN,
            "cfo_deep_neg_cr":  _SME_CFO_DEEP_NEG,
        },
    }
    # Store in cache so identical results are returned until next data batch
    _sme_result_cache_body  = response_body
    _sme_result_cache_valid = True
    return JSONResponse(response_body)



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


# -- Entry point ---------------------------------------------------------------
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

