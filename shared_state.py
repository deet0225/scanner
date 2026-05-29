"""
shared_state.py — All shared mutable state for the Nifty Stock Scanner.

Every module that needs to read or modify global state imports this module
and accesses attributes as `ss.var_name` (e.g. `import shared_state as ss`).
This avoids cross-module `global` declarations while keeping state visible.
"""

import asyncio
import threading

import pytz

from scanner import StockScanner
from tickers import NIFTY500_TICKERS, NIFTY_MICROCAP250_TICKERS
from config import MICROCAP_BENCHMARK_TICKER, MICROCAP_BENCHMARK_ETF_FALLBACKS

try:
    from sector_map import SECTOR_MAP as _SECTOR_MAP
except ImportError:
    _SECTOR_MAP: dict = {}

IST = pytz.timezone("Asia/Kolkata")

# ---------------------------------------------------------------------------
# Scanner instances
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Swing Trade scan states (Nifty500 + Microcap250)
# ---------------------------------------------------------------------------
scan_state: dict = {
    "data": [], "momentum_data": [], "last_updated": None,
    "status": "initializing", "scan_count": 0, "filters_passed": 0,
    "next_scan_ts": None, "error": None,
    "total_tickers": len(scanner.tickers), "scan_stage": "",
    "regime_ok": True, "regime_summary": "",
}
mc_scan_state: dict = {
    "data": [], "momentum_data": [], "last_updated": None,
    "status": "initializing", "scan_count": 0, "filters_passed": 0,
    "next_scan_ts": None, "error": None,
    "total_tickers": len(scanner_mc.tickers), "scan_stage": "",
    "regime_ok": True, "regime_summary": "",
}

# ---------------------------------------------------------------------------
# Stock Momentum scan states
# ---------------------------------------------------------------------------
mom_scan_state: dict = {
    "data": [], "last_updated": None,
    "status": "initializing", "scan_count": 0, "filters_passed": 0,
    "next_scan_ts": None, "error": None,
    "total_tickers": len(scanner.tickers), "scan_stage": "",
    "regime_ok": True, "regime_summary": "",
}
mc_mom_scan_state: dict = {
    "data": [], "last_updated": None,
    "status": "initializing", "scan_count": 0, "filters_passed": 0,
    "next_scan_ts": None, "error": None,
    "total_tickers": len(scanner_mc.tickers), "scan_stage": "",
    "regime_ok": True, "regime_summary": "",
}

# ---------------------------------------------------------------------------
# Morning Star scan states
# ---------------------------------------------------------------------------
ms_scan_state: dict = {
    "data": [], "last_updated": None,
    "status": "initializing", "scan_count": 0, "filters_passed": 0,
    "next_scan_ts": None, "error": None,
    "total_tickers": len(scanner.tickers), "scan_stage": "",
}
mc_ms_scan_state: dict = {
    "data": [], "last_updated": None,
    "status": "initializing", "scan_count": 0, "filters_passed": 0,
    "next_scan_ts": None, "error": None,
    "total_tickers": len(scanner_mc.tickers), "scan_stage": "",
}

# ---------------------------------------------------------------------------
# Async locks (one per scan type so they run independently)
# ---------------------------------------------------------------------------
_scan_lock: asyncio.Lock = asyncio.Lock()
_mom_scan_lock: asyncio.Lock = asyncio.Lock()
_ms_scan_lock: asyncio.Lock = asyncio.Lock()

# ---------------------------------------------------------------------------
# Per-tab cancellation events — set when user switches away from that tab
# ---------------------------------------------------------------------------
_fund_cancel: threading.Event = threading.Event()
_sme_cancel: threading.Event = threading.Event()
_active_tab: str = "swing"

# ---------------------------------------------------------------------------
# Generation counters — incremented on Force-Live-Data / cache-clear
# ---------------------------------------------------------------------------
_fund_generation: int = 0
_fund_last_completed_ts: float = 0.0
_sme_generation: int = 0
_sme_last_completed_ts: float = 0.0

# ---------------------------------------------------------------------------
# Fundamentals in-memory cache (Nifty500 + Microcap250)
# ---------------------------------------------------------------------------
_fund_data: dict = {}
_fund_data_lock: threading.Lock = threading.Lock()
_fund_bg_running: bool = False
_fund_result_cache_body: "dict | None" = None
_fund_result_cache_valid: bool = False

# ---------------------------------------------------------------------------
# SME fundamentals in-memory cache
# ---------------------------------------------------------------------------
_sme_fund_data: dict = {}
_sme_fund_lock: threading.Lock = threading.Lock()
_sme_bg_running: bool = False
_sme_result_cache_body: "dict | None" = None
_sme_result_cache_valid: bool = False
_sme_universe: dict = {}

# ---------------------------------------------------------------------------
# Misc flags
# ---------------------------------------------------------------------------
mc_scan_ever_triggered: bool = False
n500_tab_active: bool = True

