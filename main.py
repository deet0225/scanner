"""
main.py — FastAPI application entry point for the Nifty 500 Stock Scanner
=========================================================================
Architecture (tab-wise separation)
------------------------------------
  shared_state.py               All shared mutable state (scanner instances,
                                scan dicts, locks, cancel events, fund data)
  routes/utils.py               Shared helper functions (SSE, date validation,
                                industry enrichment, stop-loss computation)
  routes/swing_trades.py        Tab: Swing Trades (Nifty500 + Microcap250)
  routes/stock_momentum.py      Tab: Stock Momentum
  routes/morning_momentum.py    Tab: Morning Momentum (Morning Star pattern)
  routes/fundamentals.py        Tab: Fundamentals
  routes/sme.py                 Tab: SME (NSE Emerge + BSE SME)
  routes/sector_momentum.py     Tab: Sector Momentum
  routes/misc.py                Config, cache, tab-active, dashboard
This file only handles:
  - SSL / proxy bypass
  - Logging setup
  - FastAPI app creation + lifespan (startup / shutdown)
  - Including all per-tab routers
  - Uvicorn entry point
"""
# ---------------------------------------------------------------------------
# SSL bypass for corporate proxy
# ---------------------------------------------------------------------------
import os, ssl, warnings
os.environ.setdefault("PYTHONHTTPSVERIFY", "0")
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
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
# ---------------------------------------------------------------------------
# Logging — UTF-8 safe, all uvicorn loggers routed through root handler
# ---------------------------------------------------------------------------
import sys
import logging
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
for _name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
    _ul = logging.getLogger(_name)
    _ul.handlers.clear()
    _ul.propagate = True
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------
# Core imports
# ---------------------------------------------------------------------------
import asyncio
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
import shared_state as ss
from config import SCAN_INTERVAL_MINUTES, AUTO_RESCAN
# Per-tab routers
from routes.swing_trades     import router as swing_router
from routes.swing_trades     import run_scan, run_mc_scan
from routes.swing_trades     import _maybe_run_n500_scan, _maybe_run_mc_scan
from routes.stock_momentum   import router as mom_router
from routes.stock_momentum   import run_n500_momentum_scan, run_mc250_momentum_scan
from routes.stock_momentum   import _maybe_run_n500_momentum_scan, _maybe_run_mc250_momentum_scan
from routes.morning_momentum import router as ms_router
from routes.morning_momentum import run_n500_ms_scan, run_mc250_ms_scan
from routes.morning_momentum import _maybe_run_n500_ms_scan, _maybe_run_mc250_ms_scan
from routes.fundamentals     import router as fund_router, _fund_cache_load
from routes.sme              import router as sme_router, _sme_cache_load
from routes.sector_momentum  import router as sector_router
from routes.misc             import router as misc_router
scheduler = AsyncIOScheduler()
# ---------------------------------------------------------------------------
# Application lifespan (startup + shutdown)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load fundamentals caches from disk (no network calls at startup)
    _fund_cache_load()
    _sme_cache_load()
    if AUTO_RESCAN:
        # Schedule periodic rescans — offset by 1 min to avoid simultaneous starts
        scheduler.add_job(_maybe_run_n500_scan,           "interval", minutes=SCAN_INTERVAL_MINUTES,     id="stock_scan")
        scheduler.add_job(_maybe_run_mc_scan,             "interval", minutes=SCAN_INTERVAL_MINUTES + 1, id="mc_scan")
        scheduler.add_job(_maybe_run_n500_momentum_scan,  "interval", minutes=SCAN_INTERVAL_MINUTES,     id="n500_mom_scan")
        scheduler.add_job(_maybe_run_mc250_momentum_scan, "interval", minutes=SCAN_INTERVAL_MINUTES + 1, id="mc250_mom_scan")
        scheduler.add_job(_maybe_run_n500_ms_scan,        "interval", minutes=SCAN_INTERVAL_MINUTES + 2, id="n500_ms_scan")
        scheduler.add_job(_maybe_run_mc250_ms_scan,       "interval", minutes=SCAN_INTERVAL_MINUTES + 3, id="mc250_ms_scan")
        scheduler.start()
        logger.info("Auto-rescan ENABLED: Nifty500 every %d min, Microcap250 every %d min",
                    SCAN_INTERVAL_MINUTES, SCAN_INTERVAL_MINUTES + 1)
        # Kick all scans immediately at startup
        asyncio.create_task(run_scan())
        asyncio.create_task(run_mc_scan())
        asyncio.create_task(run_n500_momentum_scan())
        asyncio.create_task(run_mc250_momentum_scan())
        asyncio.create_task(run_n500_ms_scan())
        asyncio.create_task(run_mc250_ms_scan())
    else:
        logger.info(
            "On-demand mode: caches loaded from disk "
            "(N500=%d, MC250=%d tickers). All network downloads start on tab visit.",
            len(ss.scanner.tickers), len(ss.scanner_mc.tickers),
        )
    yield
    if AUTO_RESCAN:
        scheduler.shutdown(wait=False)
# ---------------------------------------------------------------------------
# FastAPI app + router registration
# ---------------------------------------------------------------------------
app = FastAPI(title="Nifty Stock Scanner", lifespan=lifespan)
app.include_router(swing_router)    # Swing Trades: /api/trigger /api/results /api/stream /api/stock/…
app.include_router(mom_router)      # Stock Momentum: /api/stock-momentum
app.include_router(ms_router)       # Morning Momentum: /api/morning-momentum
app.include_router(fund_router)     # Fundamentals: /api/fundamentals
app.include_router(sme_router)      # SME: /api/sme/fundamentals
app.include_router(sector_router)   # Sector Momentum: /api/sector-momentum /api/sector-stocks
app.include_router(misc_router)     # Misc: /api/config /api/cache/* /api/tab-active /
# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
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
