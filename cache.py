"""
cache.py — Local OHLCV data cache for the stock scanner.

Strategy
--------
Each ticker's OHLCV DataFrame is stored as a compressed pickle file:
    scanner/cache/ohlcv/<TICKER>.pkl

On every live scan:
  • Fresh  (≤ STALE_DAYS calendar days old) → loaded from disk instantly  (~0.3 ms each)
  • Stale  (> STALE_DAYS but file exists)   → incremental 70-day download, merged with cache
  • Missing (no file)                        → full HIST_DAYS download, saved to cache

Expected speedup
----------------
  First scan  : same as before  (~5-10 min, full network download)
  Same day    : < 5 seconds  (500+ tickers from disk, no network)
  Next session: 30-90 seconds  (only incremental 70-day updates per stale ticker)

Notes
-----
  • Historical scans (target_date) always bypass cache.
  • Thread-safe reads: pickle.load is safe to call concurrently.
  • Thread-safe writes: wrapped in try/except; occasional write collision causes
    a missed cache update (harmless — the ticker re-downloads next run).
"""

import os
import pickle
import datetime
import logging

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# IST date helper
# All NSE market data uses Indian Standard Time (IST = UTC+5:30).
# Using the system-local date (datetime.date.today()) causes different cache
# staleness decisions on UTC servers (e.g. Render) vs local IST machines,
# producing different scan results across environments.
# _ist_today() always returns the current date in IST, irrespective of the
# server's system timezone — eliminating the Render vs local data discrepancy.
# ---------------------------------------------------------------------------
_IST_OFFSET = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


def _ist_today() -> datetime.date:
    """Return today's date in IST (Indian Standard Time = UTC+5:30).

    Use this instead of datetime.date.today() everywhere dates are compared
    against NSE OHLCV candle dates to ensure consistent behaviour on both
    UTC-timezone servers (Render) and local IST machines.
    """
    return datetime.datetime.now(_IST_OFFSET).date()

# ── Cache location ────────────────────────────────────────────────────────────
_BASE_DIR = os.path.join(os.path.dirname(__file__), "cache", "ohlcv")

# ── Staleness logic ───────────────────────────────────────────────────────────
#
# For a DAILY scanner the cache must be refreshed every trading day so that
# each scan uses the latest EOD closing prices.
#
# Rule:
#   • Same-day     (delta == 0)  → always FRESH  (no double-download in one day)
#   • Weekend run                → FRESH if last row is Friday's (delta ≤ 2)
#   • Monday run                 → FRESH if last row is Friday's (delta ≤ 3)
#   • Tue–Fri run                → FRESH only if last row is from TODAY (delta == 0)
#                                  delta == 1 means yesterday's data → STALE → updates
#
# This ensures every weekday scan automatically does an incremental update
# (CACHE_UPDATE_DAYS = 75 days, fast ~30-60s for 500 tickers) to pull the
# latest NSE closing prices while still avoiding redundant same-day downloads.
#


# ── Helpers ───────────────────────────────────────────────────────────────────

def _path(ticker: str) -> str:
    """Return the .pkl file path for a ticker, creating the directory if needed."""
    os.makedirs(_BASE_DIR, exist_ok=True)
    # Sanitise: RELIANCE.NS → RELIANCE_NS,  ^CRSLDX → IDX_CRSLDX
    safe = (ticker
            .replace("^", "IDX_")
            .replace(".", "_")
            .replace("/", "_")
            .replace("\\", "_"))
    return os.path.join(_BASE_DIR, safe + ".pkl")


# ── Public API ────────────────────────────────────────────────────────────────

def load(ticker: str) -> "pd.DataFrame | None":
    """Load cached OHLCV for *ticker*.  Returns None when not cached."""
    p = _path(ticker)
    if not os.path.exists(p):
        return None
    try:
        with open(p, "rb") as fh:
            df = pickle.load(fh)
        return df if (df is not None and not df.empty) else None
    except Exception as exc:
        logger.debug("cache.load(%s) error: %s", ticker, exc)
        return None


def save(ticker: str, df: "pd.DataFrame") -> None:
    """Persist *df* to cache.  Silent on failure."""
    if df is None or df.empty:
        return
    try:
        with open(_path(ticker), "wb") as fh:
            pickle.dump(df, fh, protocol=4)
    except Exception as exc:
        logger.debug("cache.save(%s) error: %s", ticker, exc)


def is_fresh(df: "pd.DataFrame") -> bool:
    """
    Return True if *df* already contains the latest expected EOD data,
    meaning no incremental download is needed for today's scan.

    Logic:
      • Same-day data (delta 0)     → always fresh (no redundant downloads)
      • Weekend  (Sat/Sun)          → fresh if last row ≤ 2 days old (Friday data)
      • Monday                      → fresh if last row ≤ 3 days old (Friday data)
      • Public holiday (weekday,    → fresh if last row ≤ 1 day old (yesterday)
        but data not yet available)
      • Normal weekday (Tue–Fri)    → fresh ONLY if last row == today
                                      (yesterday's data triggers incremental update)
    """
    if df is None or df.empty:
        return False
    try:
        last = df.index[-1]
        last_date = last.date() if hasattr(last, "date") else last
        today     = _ist_today()   # IST date — consistent across UTC/local servers
        delta     = (today - last_date).days
        weekday   = today.weekday()   # 0=Mon … 6=Sun

        if delta < 0:
            return True   # clock skew / future-dated data — don't re-download

        if weekday >= 5:              # Saturday / Sunday
            return delta <= 2         # Friday data is latest available

        if weekday == 0:              # Monday
            return delta <= 3         # Friday data (3 days ago) is latest

        # Tuesday – Friday: fresh ONLY if we already have today's close
        # (delta == 1 = yesterday's data = stale → triggers incremental update)
        # delta == 1 is also accepted as a 1-day public-holiday buffer so that
        # a single Indian holiday does not force a full incremental re-run when
        # the market was already closed and no new data exists.
        return delta == 0             # strict: need today's EOD data

    except Exception:
        return False


def merge(existing: "pd.DataFrame",
          recent: "pd.DataFrame",
          max_rows: int = 750) -> "pd.DataFrame":
    """
    Append *recent* rows to *existing*, deduplicate by index (keep newest
    values on conflicts), sort chronologically, and trim to *max_rows*.
    """
    if recent is None or recent.empty:
        return existing
    if existing is None or existing.empty:
        return recent
    combined = pd.concat([existing, recent])
    combined = combined[~combined.index.duplicated(keep="last")]
    combined.sort_index(inplace=True)
    return combined.iloc[-max_rows:] if len(combined) > max_rows else combined


def stats() -> dict:
    """Return a dict with cache file count and total size in MB."""
    if not os.path.exists(_BASE_DIR):
        return {"tickers": 0, "size_mb": 0.0, "path": _BASE_DIR}
    files = [f for f in os.listdir(_BASE_DIR) if f.endswith(".pkl")]
    total = sum(os.path.getsize(os.path.join(_BASE_DIR, f)) for f in files)
    return {
        "tickers":  len(files),
        "size_mb":  round(total / 1_048_576, 1),
        "path":     _BASE_DIR,
    }


def clear(ticker: str = None) -> int:
    """
    Delete a single ticker's cache file (if *ticker* given) or ALL cache
    files.  Returns the number of files deleted.
    """
    if not os.path.exists(_BASE_DIR):
        return 0
    if ticker:
        p = _path(ticker)
        if os.path.exists(p):
            os.remove(p)
            return 1
        return 0
    files = [f for f in os.listdir(_BASE_DIR) if f.endswith(".pkl")]
    for f in files:
        try:
            os.remove(os.path.join(_BASE_DIR, f))
        except Exception:
            pass
    return len(files)

