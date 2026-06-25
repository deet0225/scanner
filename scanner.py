"""
scanner.py -- Nifty 500 Stock Scanner

Data source : yfinance NSE (.NS)             (primary – fast batch download)
              TradingView via tvDatafeed      (2nd fallback – excellent quality)
              yfinance BSE (.BO)              (3rd fallback)
              BSE / NSE Bhavcopy CSV          (latest-day volume/price patch)
ADX         : ta library (pure-Python, replaces pandas_ta — compatible with Python 3.14+)
RS ratio    : (1 + stock_ret20d) / (1 + index_ret20d)

Filter pipeline
---------------
Regime (aborts scan if failing):
  0a. Nifty500 20 EMA > 50 EMA
  0b. Nifty500 RSI(14) > 50 for last 3 consecutive days

Per-stock technical:
   1. Avg traded value 20D  > Rs.5 Cr
   2. Median traded value 20D > Rs.1 Cr               (optional: REQUIRE_MEDIAN_TV_20D)
   3. Relative Volume Percentile Rank (60 sessions) > 60
   4. ATR5 < 0.88 x ATR20                             (optional: REQUIRE_ATR_CONTRACTION)
   5. 20 EMA > 50 EMA  (stock uptrend regime)
   6. Weekly close > weekly 20 EMA
   7. Close > Highest High of last 20 bars             (optional: REQUIRE_HH20_BREAKOUT)
      AND Close >= High - 30% x (High - Low)           (optional: REQUIRE_PRICE_PROXIMITY)
   8. Close <= 20 EMA + 1.5 x ATR14
   9. (Close - Low) / (High - Low) >= 0.65             (optional: REQUIRE_CLOSING_RANGE)
  10. Weekly RSI(14) > 57
  11. RSI(14) > 60  AND  RSI 3-period SMA rising
  12. Volume Z-Score > 1.0  AND  Median TV(5D) > Median TV(20D)  (optional: REQUIRE_MEDIAN_TV_TREND)
      Volume Z-Score uses 3-day rolling avg (VOLUME_LOOKBACK_DAYS=3)
  13. RS Ratio SMA(10) > RS Ratio SMA(20)
  14. Gap-up from prev close <= 3%
  15. ADX(14) > 25  AND  +DI > -DI  [ta library]

Per-stock fundamental:
  16. Market Cap > Rs.500 Cr
  17. D/E < 3.0  (Yahoo Finance: D/E x100, threshold = 300)
      Note: uses multi-source chain  NSE live → Screener.in → Apify → Yahoo.

Data quality / fallback strategy
---------------------------------
  OHLCV   1st: yfinance .NS  (NSE, auto_adjust=True)  — batch
  OHLCV   2nd: TradingView NSE / BSE                  — good quality, no auth
  OHLCV   3rd: yfinance .BO  (BSE, auto_adjust=True)  — batch fallback
  Latest-day:  BSE / NSE Bhavcopy CSV                 — patches last candle
Momentum / RS:
  18. stock_ret20d - index_ret20d > 0.05   (RS outperformance > 5% over 20D)
  19. Stock 20D return > sector average + 4%

Composite score | Stop loss: max(structural_low_3d, Entry - 1.0×ATR14), bounded [Entry−1.5×ATR, Entry−0.5×ATR]
"""
import os, ssl, warnings
os.environ.setdefault("PYTHONHTTPSVERIFY", "0")
os.environ["CURL_CA_BUNDLE"]     = ""   # curl-based HTTP layers
os.environ["REQUESTS_CA_BUNDLE"] = ""   # requests library layer
try:
    ssl._create_default_https_context = ssl._create_unverified_context  # type: ignore
except Exception:
    pass
warnings.filterwarnings("ignore")
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass

import io, re, time, logging, datetime as dt_mod, threading, contextlib
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests
import yfinance as yf
import ta as _ta_lib  # pure-Python TA library (pandas_ta replacement, supports Python 3.14)

# ---------------------------------------------------------------------------
# IST date helper  (see cache.py for detailed rationale)
# ---------------------------------------------------------------------------
_IST_OFFSET = dt_mod.timezone(dt_mod.timedelta(hours=5, minutes=30))


def _ist_today() -> dt_mod.date:
    """Return today's date in IST (UTC+5:30), not the system-local timezone.

    Using dt_mod.date.today() on a UTC server (Render/cloud) returns the UTC
    date which can differ from IST by one calendar day between midnight UTC
    and 05:30 UTC (= IST midnight to 11:00 IST).  NSE data is always dated
    in IST, so all comparisons against candle dates must use the IST date to
    produce identical results on both local and remote environments.
    """
    return dt_mod.datetime.now(_IST_OFFSET).date()

# No-op context manager — kept so call sites need no changes
@contextlib.contextmanager
def _suppress_ta_stdout():
    yield


def _ta_atr(df, length=14):
    """Drop-in replacement for df.ta.atr(length=N, append=False)."""
    h = df["High"]  if "High"  in df.columns else df["high"]
    l = df["Low"]   if "Low"   in df.columns else df["low"]
    c = df["Close"] if "Close" in df.columns else df["close"]
    return _ta_lib.volatility.AverageTrueRange(
        high=h, low=l, close=c, window=length, fillna=False
    ).average_true_range()


def _ta_adx(df, length=14):
    """Drop-in replacement for df.ta.adx(length=N, append=False).
    Returns DataFrame with columns ADX_{N}, DMP_{N}, DMN_{N}."""
    h = df["High"]  if "High"  in df.columns else df["high"]
    l = df["Low"]   if "Low"   in df.columns else df["low"]
    c = df["Close"] if "Close" in df.columns else df["close"]
    ind = _ta_lib.trend.ADXIndicator(high=h, low=l, close=c, window=length, fillna=False)
    return pd.DataFrame({
        f"ADX_{length}": ind.adx(),
        f"DMP_{length}": ind.adx_pos(),
        f"DMN_{length}": ind.adx_neg(),
    })


def _ta_macd(df, fast=12, slow=26, signal=9):
    """Drop-in replacement for df.ta.macd(fast=.., slow=.., signal=.., append=False).
    Returns DataFrame with columns MACD_F_S_SIG, MACDs_F_S_SIG, MACDh_F_S_SIG."""
    c = df["Close"] if "Close" in df.columns else df["close"]
    ind = _ta_lib.trend.MACD(
        close=c, window_slow=slow, window_fast=fast, window_sign=signal, fillna=False
    )
    tag = f"{fast}_{slow}_{signal}"
    return pd.DataFrame({
        f"MACD_{tag}":  ind.macd(),
        f"MACDs_{tag}": ind.macd_signal(),
        f"MACDh_{tag}": ind.macd_diff(),
    })

from tickers import NIFTY500_TICKERS
from config import (
    RSI_MIN, RSI_PERIOD,
    EMA_PERIOD, EMA_SHORT_PERIOD,
    VOLUME_AVG_DAYS, ADX_PERIOD, ADX_MIN,
    WEIGHT_VOLUME, WEIGHT_RSI, WEIGHT_EMA, WEIGHT_ADX, WEIGHT_MOMENTUM, WEIGHT_RS,
    VOLUME_SCORE_CAP, EMA_PCT_SCORE_CAP, ADX_SCORE_CAP, RS_SCORE_CAP, MOMENTUM_SCORE_CAP,
    HIST_DAYS, TOP_N, MIN_DATA_ROWS, CACHE_UPDATE_DAYS,
    RETURN_3M_DAYS,
    DEBT_EQUITY_MAX, MARKET_CAP_MIN,
    SECTOR_OUTPERFORM_MIN, MOMENTUM_OUTPERFORM_MIN,
    DOWNLOAD_THREADS, DOWNLOAD_BATCH_SIZE, FUNDAMENTALS_THREADS,
    FUNDAMENTALS_DELAY, DOWNLOAD_THROTTLE, CRUMB_TTL,
    MARKET_BENCHMARK_TICKER, MARKET_BENCHMARK_ETF_FALLBACKS,
    SECTOR_INDEX_ETF_FALLBACKS,
    SECTOR_INDEX_MAP, SECTOR_LOOKBACK_DAYS,
    SECTOR_FALLBACK_TO_MARKET,
    AVG_TRADED_VALUE_20D_MIN, MEDIAN_TRADED_VALUE_20D_MIN,
    REL_VOL_PERCENTILE_MIN, ATR_RATIO_MAX, VOLUME_ZSCORE_MIN,
    GAP_UP_MAX, CLOSING_RANGE_MIN, PRICE_PROXIMITY_MAX, EMA_ATR_MULTIPLIER,
    REQUIRE_EMA_ATR_CEILING,
    WEEKLY_RSI_MIN,
    REGIME_ABORT_ON_FAIL, REGIME_RSI_REQUIRE_DAYS,
    REQUIRE_HH20_BREAKOUT, REQUIRE_ATR_CONTRACTION, REQUIRE_RSI_SMA3_RISING,
    REQUIRE_MEDIAN_TV_20D, REQUIRE_CLOSING_RANGE, REQUIRE_MEDIAN_TV_TREND,
    REQUIRE_PRICE_PROXIMITY, VOLUME_LOOKBACK_DAYS,
    REQUIRE_WEEKLY_EMA, REQUIRE_RS_UPTREND, REQUIRE_ADX_THRESHOLD,
    REQUIRE_FUNDAMENTALS,
)
# Multi-source data clients (TradingView, nsepython, Screener.in)
from data_sources import (
    fundamentals  as _fund_client,   # enhanced FundamentalsClient singleton
    tv_client     as _tv_client,     # TradingView OHLCV
)
import cache as _cache               # local OHLCV disk cache

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Momentum-only scan thresholds
# Defined here (not in config) so _analyze_momentum() can use them without
# circular imports.  main.py imports these directly to stay in sync.
#
# Thresholds are deliberately a step BELOW the swing-trade gates so that
# nascent momentum is captured BEFORE the move becomes obvious.  Quality is
# maintained by mandatory rising-trend confirmation filters (RSI SMA-3
# rising, MACD histogram expanding, ADX and weekly RSI trending up) which
# together ensure only genuinely strengthening setups pass.
# ---------------------------------------------------------------------------
MOM_RSI_MIN   = 60.0   # RSI-14 — early momentum zone (was 62; catches sooner)
MOM_WRSI_MIN  = 55.0   # Weekly RSI-14 — weekly trend turning bullish (was 60)
MOM_ADX_MIN   = 22.0   # ADX — trend just establishing (was 25; <20 = directionless)
MOM_VOLZ_MIN  = 0.8    # Volume Z-score — above-average accumulation (was 0.8)
MOM_RS_MIN    = 2.5    # RS outperformance vs benchmark (%) — emerging leader (was 3.0)
MOM_RET20_MIN = 3.0    # 20-day absolute return floor (%) — stock is moving (was 5.0)
MOM_RET5_MIN  = 0.0    # 5-day return must be non-negative (not rolling over)
MOM_TV_MIN_CR = 2.0    # Min avg daily traded value (Crores) — decent liquidity

# ---------------------------------------------------------------------------
# Morning Star quality filter thresholds (scan_morning_star only)
# ---------------------------------------------------------------------------
MS_TV_MIN_CR      = 3.0   # Min avg daily traded value in Crores (liquidity gate)
MS_PRIOR_DROP_PCT = 3.0   # Pre-pattern decline: trough in last 6 bars must be at
                           #   least 3% below the close ~10 bars before the pattern
MS_VOL_CONFIRM_X  = 1.0   # Day-3 reversal area volume >= 100% of 20D avg volume

# ---------------------------------------------------------------------------
# Shared yfinance session -- verify=False so corporate-proxy self-signed certs
# don't break downloads.  Also silence yfinance's own noisy error logger so
# that "YFTzMissingError / possibly delisted" warnings for index tickers
# don't pollute the console (we handle failures gracefully via fallbacks).
# ---------------------------------------------------------------------------
_YF_SESSION = requests.Session()
_YF_SESSION.verify = False
_YF_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
})
# Size the connection pool generously: yfinance hits multiple Yahoo subdomains
# (query1, query2, fc.yahoo.com, finance.yahoo.com) and makes several requests
# per ticker -- so pool_connections must cover all those hosts and pool_maxsize
# must handle concurrent connections from all download threads simultaneously.
_yf_adapter = requests.adapters.HTTPAdapter(
    pool_connections=32,              # distinct host pools (covers all YF subdomains)
    pool_maxsize=DOWNLOAD_THREADS * 4,  # connections per pool (4x threads = safe headroom)
)
_YF_SESSION.mount("https://", _yf_adapter)
_YF_SESSION.mount("http://",  _yf_adapter)
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
logging.getLogger("peewee").setLevel(logging.CRITICAL)


# ---------------------------------------------------------------------------
# Yahoo Finance crumb client  (fundamentals only -- market_cap + D/E + sector)
# ---------------------------------------------------------------------------

class _YahooClient:
    SUMMARY_URL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{t}"
    HOME_URL    = "https://finance.yahoo.com/"
    UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

    def __init__(self):
        self.s = requests.Session()
        self.s.verify = False
        self.s.headers.update({
            "User-Agent": self.UA,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        self._crumb    = None
        self._crumb_ts = 0.0
        self._lock     = threading.Lock()   # protect crumb refresh across parallel threads

    def init_crumb(self):
        with self._lock:
            if self._crumb and time.time() - self._crumb_ts < CRUMB_TTL:
                return
            logger.info("Fetching Yahoo Finance crumb...")
            r = self.s.get(self.HOME_URL, timeout=20)
            r.raise_for_status()
            m = re.compile('"crumb":"([^"]+)"').findall(r.text)
            if not m:
                raise RuntimeError("Crumb not found in Yahoo Finance HTML")
            self._crumb    = m[0]
            self._crumb_ts = time.time()
            logger.info("Crumb OK | cookies: %s", list(self.s.cookies.keys()))

    def fundamentals(self, ticker):
        """Returns (sector, debt_equity_x100, market_cap_inr).
        debt_equity_x100: Yahoo's format where 250 = D/E ratio 2.5.
        market_cap_inr  : absolute INR (e.g. 12e9 = Rs.1200 Cr).
        Any value may be None if unavailable.
        """
        try:
            self.init_crumb()
            r = self.s.get(
                self.SUMMARY_URL.format(t=ticker),
                params={"modules": "summaryDetail,assetProfile,financialData",
                        "crumb": self._crumb},
                timeout=15,
            )
            if r.status_code != 200:
                return None, None, None
            res = r.json().get("quoteSummary", {}).get("result")
            if not res:
                return None, None, None
            sd = res[0].get("summaryDetail", {})
            ap = res[0].get("assetProfile", {})
            fd = res[0].get("financialData", {})

            def _raw(d, key):
                v = d.get(key, {})
                return v.get("raw") if isinstance(v, dict) else v

            debt_equity = _raw(fd, "debtToEquity")  # x100 format
            market_cap  = _raw(sd, "marketCap")      # absolute INR
            return ap.get("sector"), debt_equity, market_cap
        except Exception as exc:
            logger.debug("fundamentals(%s): %s", ticker, exc)
            return None, None, None


# ---------------------------------------------------------------------------
# BSE Bhavcopy client  (latest-day volume / price validation / patch)
# ---------------------------------------------------------------------------

class _BSEBhavcopy:
    """
    Downloads BSE Bhavcopy CSV ZIP for end-of-day price/volume validation.

    Primary URL  : https://www.bseindia.com/download/BhavCopy/Equity/EQ{DDMMYYYY}_CSV.ZIP
    Fallback URL : https://archives.nseindia.com/products/content/sec_bhavdata_full_{DD-Mon-YYYY}.csv

    BSE CSV columns (EQ series):
        SC_CODE, SC_NAME, SC_GROUP, SC_TYPE, OPEN, HIGH, LOW, CLOSE,
        TRAD_QTY, NO_OF_TRADES, NET_TRNOVER (Rs. lakhs), TDCLOINDI

    NSE CSV columns (equity bhav):
        SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, LAST, PREVCLOSE,
        TOTTRDQTY, TOTTRDVAL, TIMESTAMP, TOTALTRADES, ISIN

    SC_NAME / SYMBOL both match NSE ticker symbols directly.
    NET_TRNOVER * 100 000 = traded value in INR; TOTTRDVAL is already in INR.
    """
    BSE_URL = "https://www.bseindia.com/download/BhavCopy/Equity/EQ{date}_CSV.ZIP"
    NSE_URL = "https://archives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv"
    UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

    def __init__(self):
        self.s = requests.Session()
        self.s.verify = False
        self.s.headers.update({
            "User-Agent": self.UA,
            "Referer":    "https://www.bseindia.com/",
            "Accept-Language": "en-US,en;q=0.9",
        })
        # Increase pool size for BSE/NSE connections too
        _bse_adapter = requests.adapters.HTTPAdapter(pool_connections=4,
                                                     pool_maxsize=8)
        self.s.mount("https://", _bse_adapter)
        self.s.mount("http://",  _bse_adapter)
        # date_str -> {upper_symbol: row_dict}
        self._cache: dict = {}

    # ---- BSE download ----------------------------------------------------------

    def _load_bse(self, trade_date: dt_mod.date) -> dict:
        """Attempt to download BSE Bhavcopy ZIP for trade_date."""
        import zipfile, io as _io
        date_str = trade_date.strftime("%d%m%Y")
        result: dict = {}
        try:
            r = self.s.get(self.BSE_URL.format(date=date_str), timeout=10)
            if r.status_code != 200:
                logger.debug("BSE bhavcopy %s: HTTP %d", date_str, r.status_code)
                return result
            with zipfile.ZipFile(_io.BytesIO(r.content)) as z:
                csv_name = next(
                    (n for n in z.namelist()
                     if n.upper().startswith("EQ") and n.upper().endswith(".CSV")),
                    None,
                )
                if csv_name is None:
                    return result
                raw = pd.read_csv(z.open(csv_name))
            raw.columns = raw.columns.str.strip()
            if "SC_TYPE" in raw.columns:
                raw = raw[raw["SC_TYPE"].str.strip() == "EQ"]
            # Vectorized parsing -- much faster than iterrows()
            raw["SC_NAME"] = raw["SC_NAME"].astype(str).str.strip().str.upper()
            raw = raw[raw["SC_NAME"].str.len() > 0]
            for col in ("OPEN", "HIGH", "LOW", "CLOSE", "TRAD_QTY", "NET_TRNOVER"):
                if col in raw.columns:
                    raw[col] = pd.to_numeric(raw[col], errors="coerce").fillna(0)
            result = {
                row["SC_NAME"]: {
                    "date":     trade_date,
                    "open":     float(row.get("OPEN",  0)),
                    "high":     float(row.get("HIGH",  0)),
                    "low":      float(row.get("LOW",   0)),
                    "close":    float(row.get("CLOSE", 0)),
                    "volume":   float(row.get("TRAD_QTY",    0)),
                    "turnover": float(row.get("NET_TRNOVER", 0)) * 100_000,
                }
                for row in raw.to_dict("records")
            }
            logger.info("BSE bhavcopy %s: loaded %d EQ records", date_str, len(result))
        except Exception as exc:
            logger.debug("BSE bhavcopy %s error: %s", date_str, exc)
        return result

    # ---- NSE fallback ----------------------------------------------------------

    def _load_nse(self, trade_date: dt_mod.date) -> dict:
        """Attempt to download NSE EOD bhav CSV for trade_date (fallback)."""
        # NSE date format: DD-Mon-YYYY  e.g. 09-May-2026
        nse_date = trade_date.strftime("%d-%b-%Y")
        result: dict = {}
        try:
            r = self.s.get(self.NSE_URL.format(date=nse_date), timeout=10)
            if r.status_code != 200:
                logger.debug("NSE bhav %s: HTTP %d", nse_date, r.status_code)
                return result
            raw = pd.read_csv(pd.io.common.StringIO(r.text))
            raw.columns = raw.columns.str.strip()
            # Keep only EQ series
            if "SERIES" in raw.columns:
                raw = raw[raw["SERIES"].str.strip() == "EQ"]
            # Vectorized parsing -- much faster than iterrows()
            raw["SYMBOL"] = raw["SYMBOL"].astype(str).str.strip().str.upper()
            raw = raw[raw["SYMBOL"].str.len() > 0]
            for col in ("OPEN", "HIGH", "LOW", "CLOSE", "TOTTRDQTY", "TOTTRDVAL"):
                if col in raw.columns:
                    raw[col] = pd.to_numeric(raw[col], errors="coerce").fillna(0)
            result = {
                row["SYMBOL"]: {
                    "date":     trade_date,
                    "open":     float(row.get("OPEN",      0)),
                    "high":     float(row.get("HIGH",      0)),
                    "low":      float(row.get("LOW",       0)),
                    "close":    float(row.get("CLOSE",     0)),
                    "volume":   float(row.get("TOTTRDQTY", 0)),
                    "turnover": float(row.get("TOTTRDVAL", 0)),
                }
                for row in raw.to_dict("records")
            }
            logger.info("NSE bhav %s: loaded %d EQ records", nse_date, len(result))
        except Exception as exc:
            logger.debug("NSE bhav %s error: %s", nse_date, exc)
        return result

    # ---- Unified loader --------------------------------------------------------

    def _load(self, trade_date: dt_mod.date) -> dict:
        """Download and cache one day's EOD data (BSE -> NSE fallback)."""
        ds = trade_date.strftime("%d%m%Y")
        if ds in self._cache:
            return self._cache[ds]
        # Try BSE first, fall back to NSE
        result = self._load_bse(trade_date)
        if not result:
            result = self._load_nse(trade_date)
        self._cache[ds] = result
        return result

    def get(self, nse_symbol: str, trade_date: dt_mod.date) -> "dict | None":
        """Return bhavcopy row for nse_symbol on trade_date, or None."""
        data = self._load(trade_date)
        sym = nse_symbol.upper()
        if sym in data:
            return data[sym]
        # Try without any suffix that might differ (e.g. trailing numbers)
        for key in data:
            if key.startswith(sym) or sym.startswith(key):
                return data[key]
        return None

    def load_recent(self, target_date=None, lookback: int = 3) -> "dt_mod.date | None":
        """
        Load bhavcopy for the most recent available trading day.
        Tries up to `lookback` non-weekend days going backwards from anchor.
        Stops immediately on first success to avoid unnecessary network trips.
        """
        anchor = target_date if target_date else _ist_today()
        offset, tries = 0, 0
        while tries < lookback and offset < lookback + 4:
            check = anchor - dt_mod.timedelta(days=offset)
            offset += 1
            if check.weekday() >= 5:   # skip weekends
                continue
            tries += 1
            data = self._load(check)
            if data:
                return check     # stop on first success
        return None


# ---------------------------------------------------------------------------
# Zerodha Kite Connect client  (primary OHLCV source)
# Requires env vars: ZERODHA_API_KEY, ZERODHA_ACCESS_TOKEN
# ---------------------------------------------------------------------------

class _ZerodhaClient:
    """
    Fetches daily OHLCV from Zerodha Kite Connect historical API.

    Instrument map : GET https://api.kite.trade/instruments  (public CSV, no auth)
    Historical data: GET https://api.kite.trade/instruments/historical/{token}/day
    Auth header    : Authorization: token {api_key}:{access_token}
    """
    _INSTRUMENTS_URL = "https://api.kite.trade/instruments"
    _HIST_URL        = "https://api.kite.trade/instruments/historical/{token}/day"
    _QUOTE_URL       = "https://api.kite.trade/quote"
    _TIMEOUT         = 18
    _QUOTE_BATCH_MAX = 500   # Zerodha allows up to 500 instruments per request

    def __init__(self) -> None:
        self._api_key      = (os.getenv("ZERODHA_API_KEY")      or "").strip()
        self._access_token = (os.getenv("ZERODHA_ACCESS_TOKEN") or "").strip()
        self._session      = requests.Session()
        self._session.verify = False
        self._session.headers.update({
            "Accept":          "application/json",
            "X-Kite-Version":  "3",
            "User-Agent":      "scanner-zerodha/1.0",
        })
        if self._api_key and self._access_token:
            self._session.headers.update({
                "Authorization": f"token {self._api_key}:{self._access_token}"
            })
        self._map_lock   = threading.Lock()
        self._sym_to_tok: dict = {}   # {SYMBOL: instrument_token (int)}
        self._map_ts: float   = 0.0

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
            if self._sym_to_tok and (time.time() - self._map_ts) < 6 * 3600:
                return
            try:
                r = self._session.get(self._INSTRUMENTS_URL, timeout=self._TIMEOUT)
                r.raise_for_status()
                import io as _io
                df = pd.read_csv(_io.StringIO(r.text))
                nse_eq = df[
                    (df["exchange"] == "NSE") & (df["instrument_type"] == "EQ")
                ]
                mp: dict = {}
                for _, row in nse_eq.iterrows():
                    sym = str(row.get("tradingsymbol") or "").strip().upper()
                    tok = row.get("instrument_token")
                    if sym and not pd.isna(tok):
                        mp[sym] = int(tok)
                self._sym_to_tok = mp
                self._map_ts     = time.time()
                logger.info("Zerodha instrument map loaded: %d NSE EQ symbols", len(mp))
            except Exception as exc:
                logger.warning("Zerodha instrument map load failed: %s", exc)

    def _token_for(self, ticker: str) -> "int | None":
        self._load_instrument_map()
        sym = ticker.upper().replace(".NS", "").replace(".BO", "")
        return self._sym_to_tok.get(sym)

    def get_daily_ohlcv(self, ticker: str, days: int,
                        end_date=None) -> "pd.DataFrame | None":
        """Fetch up to `days` daily candles ending on `end_date`."""
        if not self.enabled:
            return None
        token = self._token_for(ticker)
        if not token:
            return None
        end_dt   = end_date if isinstance(end_date, dt_mod.date) else _ist_today()
        start_dt = end_dt - dt_mod.timedelta(days=days + 60)
        try:
            r = self._session.get(
                self._HIST_URL.format(token=token),
                params={
                    "from": start_dt.strftime("%Y-%m-%d"),
                    "to":   end_dt.strftime("%Y-%m-%d"),
                },
                timeout=self._TIMEOUT,
            )
            r.raise_for_status()
            body    = r.json() or {}
            candles = (body.get("data") or {}).get("candles") or []
            if not candles:
                return None
            # Zerodha candle: [timestamp, open, high, low, close, volume, oi]
            df = pd.DataFrame(
                candles,
                columns=["Date", "Open", "High", "Low", "Close", "Volume", "OI"],
            )
            for col in ("Open", "High", "Low", "Close", "Volume"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date", "Close"])
            df = df[df["Close"] > 0]
            if df.empty:
                return None
            df = df.sort_values("Date")
            idx = df["Date"].dt.tz_localize(None).dt.normalize()
            df = df.set_index(idx)
            return df[["Open", "High", "Low", "Close", "Volume"]]
        except Exception as exc:
            logger.debug("ZerodhaClient.get_daily_ohlcv(%s): %s", ticker, exc)
            return None

    def get_live_quotes_batch(self, tickers: list) -> dict:
        """
        Fetch live OHLCV for multiple NSE tickers via Zerodha /quote API.

        Returns {SYMBOL: {"open", "high", "low", "close", "volume"}}
        where "close" = last_price (current traded price, not previous-day close).
        Tickers may include the .NS / .BO suffix — it is stripped automatically.
        Batches up to _QUOTE_BATCH_MAX instruments per API call.
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


def _apply_zerodha_live_patch(all_data: dict, lbl: str) -> None:
    """
    Patch today's OHLCV candle in every DataFrame in *all_data* using live
    quotes from the Zerodha /quote API.  Called only in live mode (target_date
    is None).  No-op when Zerodha credentials are not configured.

    For each ticker:
      • If today's row already exists (historical API returned a partial candle),
        the Close is replaced with last_price and High/Low are widened if needed.
      • If today's row is absent (pre-market or first scan of the day),
        a new row is appended so all filters see the latest price.
    """
    if not _zerodha.enabled:
        return
    quotes = _zerodha.get_live_quotes_batch(list(all_data.keys()))
    if not quotes:
        logger.debug("%s Zerodha live patch: no quotes returned", lbl)
        return
    today_ts = pd.Timestamp(_ist_today())
    patched  = 0
    for ticker in list(all_data.keys()):
        sym = ticker.upper().replace(".NS", "").replace(".BO", "")
        q   = quotes.get(sym)
        if not q or not q["close"]:
            continue
        df = all_data[ticker]
        if today_ts in df.index:
            # Widen High/Low so intraday extremes are preserved, update Close
            df.loc[today_ts, "Open"]   = q["open"]
            df.loc[today_ts, "High"]   = max(float(df.at[today_ts, "High"]), q["high"])
            df.loc[today_ts, "Low"]    = min(float(df.at[today_ts, "Low"]),  q["low"])
            df.loc[today_ts, "Close"]  = q["close"]
            df.loc[today_ts, "Volume"] = q["volume"]
        else:
            new_row = pd.DataFrame(
                [[q["open"], q["high"], q["low"], q["close"], q["volume"]]],
                index=[today_ts],
                columns=["Open", "High", "Low", "Close", "Volume"],
            )
            all_data[ticker] = pd.concat([df, new_row]).sort_index()
        patched += 1
    logger.info("%s Zerodha live patch: %d/%d tickers updated with live candle",
                lbl, patched, len(all_data))


# ---------------------------------------------------------------------------
# Core scanner
# ---------------------------------------------------------------------------

class StockScanner:

    def __init__(self, tickers=None, benchmark_ticker=None,
                 benchmark_etf_fallbacks=None, label="Nifty500"):
        self.tickers                  = tickers if tickers is not None else NIFTY500_TICKERS
        self.benchmark_ticker         = benchmark_ticker or MARKET_BENCHMARK_TICKER
        self.benchmark_etf_fallbacks  = benchmark_etf_fallbacks or MARKET_BENCHMARK_ETF_FALLBACKS
        self.label                    = label
        self._yf                      = _YahooClient()
        self._bse                     = _BSEBhavcopy()
        # Regime state exposed to main.py after each scan()
        self.last_regime_ok           = True
        self.last_regime_summary      = ""
        # Momentum-only results exposed to main.py after each scan()
        # Contains stocks that passed only the 6 momentum criteria, independent
        # of the strict Swing Trade filters (EMA cross, breakout, fundamentals, etc.)
        self.last_momentum_results: list = []

    # -- Data quality checker ------------------------------------------------

    @staticmethod
    def _is_quality_ok(df: "pd.DataFrame | None",
                       min_rows: int = MIN_DATA_ROWS) -> bool:
        """Return True if df has enough rows and acceptable data quality."""
        if df is None or len(df) < min_rows:
            return False
        # Too many NaN closes
        if df["Close"].isna().sum() > len(df) * 0.05:
            return False
        # Too many zero/NaN volume rows (> 30%) -> likely bad Yahoo data
        zero_vol = (df["Volume"].fillna(0) == 0).sum()
        if zero_vol > len(df) * 0.30:
            return False
        return True

    # -- yfinance OHLCV (NSE primary -> BSE fallback) -------------------------

    @staticmethod
    def _fetch_yf_direct(ticker: str, days: int, end_date=None) -> "pd.DataFrame | None":
        """
        Fetch daily OHLCV for a single ticker by calling Yahoo Finance's v8 chart
        API directly (bypasses yf.download() which fails in corporate proxy
        environments for single-ticker calls due to the consent redirect).
        Returns an auto-adjusted DataFrame with columns Open/High/Low/Close/Volume.
        """
        try:
            if end_date is not None:
                end_dt = end_date if isinstance(end_date, dt_mod.date) \
                         else dt_mod.date.fromisoformat(str(end_date))
            else:
                end_dt = _ist_today()   # IST date — consistent across UTC/local servers
            start_dt = end_dt - dt_mod.timedelta(days=days + 60)

            # Use IST-aware datetimes so period1/period2 produce identical Unix timestamps
            # on both UTC servers (Render) and local IST machines.  Without the tzinfo
            # argument, datetime.timestamp() uses the system-local timezone, giving
            # different boundaries on UTC vs IST hosts and causing subtle data differences
            # that manifest most visibly in the Morning Star tab (last-3-candle pattern).
            _IST_TZ = dt_mod.timezone(dt_mod.timedelta(hours=5, minutes=30))
            period1 = int(dt_mod.datetime.combine(start_dt, dt_mod.time.min,
                                                   _IST_TZ).timestamp())
            period2 = int(dt_mod.datetime.combine(end_dt, dt_mod.time(23, 59, 59),
                                                   _IST_TZ).timestamp())

            url = (
                f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                f"?interval=1d&period1={period1}&period2={period2}&events=div,splits"
            )
            r = _YF_SESSION.get(url, timeout=20)
            if r.status_code != 200:
                logger.debug("_fetch_yf_direct(%s): HTTP %d", ticker, r.status_code)
                return None

            data = r.json()
            result = (data.get("chart") or {}).get("result")
            if not result:
                return None

            res      = result[0]
            ts       = res.get("timestamp", [])
            quote    = (res.get("indicators") or {}).get("quote", [{}])[0]
            adjclose_list = ((res.get("indicators") or {}).get("adjclose") or [{}])[0].get("adjclose") or []

            if not ts or not quote:
                return None

            opens   = quote.get("open",   [])
            highs   = quote.get("high",   [])
            lows    = quote.get("low",    [])
            closes  = quote.get("close",  [])
            volumes = quote.get("volume", [])

            # Build index
            idx = pd.to_datetime(ts, unit="s", utc=True).tz_convert("Asia/Kolkata").tz_localize(None)
            idx = idx.normalize()  # strip time, keep date component

            n = len(ts)

            def _to_float(lst):
                """Convert a list (may contain None) to a numpy float64 array."""
                return np.array(
                    [float(x) if x is not None else np.nan for x in lst[:n]],
                    dtype=np.float64
                )

            open_arr   = _to_float(opens)
            high_arr   = _to_float(highs)
            low_arr    = _to_float(lows)
            close_arr  = _to_float(closes)
            vol_arr    = np.array(
                [float(x) if x is not None else 0.0 for x in volumes[:n]],
                dtype=np.float64
            )

            # Apply split/dividend adjustment (mirror auto_adjust=True)
            if adjclose_list and len(adjclose_list) >= n:
                adj_arr = np.array(
                    [float(x) if x is not None else np.nan for x in adjclose_list[:n]],
                    dtype=np.float64
                )
                mask  = (close_arr != 0) & ~np.isnan(close_arr) & ~np.isnan(adj_arr)
                ratio = np.where(mask, adj_arr / close_arr, 1.0)
                open_arr  = open_arr  * ratio
                high_arr  = high_arr  * ratio
                low_arr   = low_arr   * ratio
                close_arr = adj_arr

            df = pd.DataFrame({
                "Open":   open_arr,
                "High":   high_arr,
                "Low":    low_arr,
                "Close":  close_arr,
                "Volume": vol_arr,
            }, index=idx)


            df = df.dropna(subset=["Close"])
            df = df[df["Close"] > 0]
            return df if not df.empty else None

        except Exception as exc:
            logger.debug("_fetch_yf_direct(%s): %s", ticker, exc)
            return None

    @staticmethod
    def _fetch_yf(ticker: str, days: int, end_date) -> "pd.DataFrame | None":
        """yfinance single-ticker download -- uses direct chart API (yf.download
        requires curl_cffi in yfinance 1.x which conflicts with our requests.Session)."""
        return StockScanner._fetch_yf_direct(ticker, days, end_date)

    @staticmethod
    def _download_ohlcv(ticker: str, days: int = HIST_DAYS,
                        end_date=None) -> "pd.DataFrame | None":
        """
        Fetch daily OHLCV with auto_adjust=True.

        Priority:
          0. Zerodha Kite Connect           [primary — requires API credentials]
          1. NSE ticker (.NS) via yfinance
          2. TradingView (NSE → BSE)       — excellent quality, no auth
          3. BSE ticker (.BO) via yfinance  — existing fallback
        """
        # 0. Primary: Zerodha Kite Connect
        if _zerodha.enabled:
            df_zd = _zerodha.get_daily_ohlcv(ticker, days, end_date)
            if StockScanner._is_quality_ok(df_zd):
                return df_zd

        # 1. Fallback: NSE via yfinance
        df_ns = StockScanner._fetch_yf(ticker, days, end_date)
        if StockScanner._is_quality_ok(df_ns):
            return df_ns

        # 2. TradingView fallback (NSE / BSE)
        df_tv = _tv_client.get_history(ticker, days, end_date)
        if StockScanner._is_quality_ok(df_tv):
            logger.debug("Using TradingView data for %s", ticker)
            return df_tv

        # 3. Fallback: BSE via yfinance (.BO)
        bse_ticker = ticker.replace(".NS", ".BO")
        logger.debug("NSE data quality poor for %s -- trying BSE (%s)",
                     ticker, bse_ticker)
        df_bo = StockScanner._fetch_yf(bse_ticker, days, end_date)
        if StockScanner._is_quality_ok(df_bo):
            logger.debug("Using BSE (.BO) yfinance data for %s", ticker)
            return df_bo

        # Return whichever has the most rows (might still be usable)
        candidates = [df for df in (df_ns, df_tv, df_bo)
                      if df is not None]
        if not candidates:
            return None
        return max(candidates, key=len)

    @staticmethod
    def _download_batch_ns(tickers: list, days: int, end_date=None) -> dict:
        """
        Download daily OHLCV for *multiple* tickers in parallel using the
        Yahoo Finance v8 chart API directly (_fetch_yf_direct).

        yf.download() is no longer used here: yfinance 1.x requires curl_cffi
        sessions which are incompatible with the corporate-proxy requests.Session
        we use elsewhere.  The direct chart API bypasses consent-page redirects
        and works reliably in constrained network environments.

        Returns {ticker: DataFrame} for all tickers that returned quality data.
        """
        if not tickers:
            return {}

        import random as _rnd
        _WORKERS = min(4, len(tickers))   # ≤4 concurrent to avoid Yahoo rate-limits
        result: dict = {}

        def _fetch_one(t):
            # 0. Try Zerodha first (when credentials are configured)
            if _zerodha.enabled:
                try:
                    df = _zerodha.get_daily_ohlcv(t, days, end_date)
                    if df is not None and not df.empty:
                        return t, df
                except Exception:
                    pass

            # 1. Fallback: yfinance direct chart API
            # Small random jitter so all workers don't fire at t=0
            time.sleep(_rnd.uniform(0.0, 0.15))
            for attempt in range(3):
                try:
                    df = StockScanner._fetch_yf_direct(t, days, end_date)
                    if df is not None and not df.empty:
                        return t, df
                except Exception:
                    pass
                if attempt < 2:
                    time.sleep(0.4 * (attempt + 1))   # 0.4s, then 0.8s back-off
            return t, None

        with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
            futures = {pool.submit(_fetch_one, t): t for t in tickers}
            for fut in as_completed(futures):
                try:
                    t, df = fut.result()
                    if df is not None and not df.empty:
                        result[t] = df
                except Exception as exc:
                    logger.debug("_download_batch_ns worker(%s): %s", futures[fut], exc)

        return result

    # -- Index / benchmark download (no volume quality check) ----------------

    @staticmethod
    def _fetch_index(ticker: str, days: int, end_date=None,
                     etf_fallbacks: "list | None" = None) -> "pd.DataFrame | None":
        """
        Download OHLCV for a market index or ETF proxy.

        Source priority per candidate
        ─────────────────────────────
        1. yfinance yf.download()       (batch path)
        2. yfinance direct chart API    (corporate-proxy-safe)
        3. TradingView NSE              (for ^ index tickers only, after all YF attempts fail)

        ETF fallback candidates (from config) are tried after the primary
        index ticker exhausts all sources.
        """
        candidates = [ticker] + (etf_fallbacks or [])

        for candidate in candidates:
            # ── Source 1 & 2: Yahoo Finance ──────────────────────────────────
            for fetch_fn in (StockScanner._fetch_yf, StockScanner._fetch_yf_direct):
                df = fetch_fn(candidate, days, end_date)
                if df is not None and not df.empty:
                    close = df["Close"].dropna()
                    if len(close) >= 2 and float(close.iloc[-1]) > 0:
                        if candidate != ticker:
                            logger.info("Index %s unavailable -- using ETF proxy %s (%d rows) [Yahoo Finance]",
                                        ticker, candidate, len(close))
                        else:
                            logger.debug("Index %s loaded via Yahoo Finance (%d rows)", ticker, len(close))
                        return df

            # ── Source 3: TradingView (primary ^ index only, strip ^ prefix) ──
            # Only attempt TV for the primary index ticker that starts with ^.
            # ETF proxies (NIFTYBEES.NS etc.) are plain stocks and _download_ohlcv
            # already handles them; trying TV for ETFs here would be redundant.
            if candidate == ticker and candidate.startswith("^") and _tv_client.available:
                tv_sym = candidate.lstrip("^")   # e.g. "^CNXMC250" → "CNXMC250"
                df_tv = _tv_client.get_history(tv_sym, days, end_date)
                if df_tv is not None and not df_tv.empty:
                    close = df_tv["Close"].dropna()
                    if len(close) >= 2 and float(close.iloc[-1]) > 0:
                        logger.info("Index %s loaded via TradingView NSE (%d rows)", ticker, len(close))
                        return df_tv
                else:
                    logger.debug("Index %s: TradingView also unavailable (sym=%s)", ticker, tv_sym)

        return None

    # -- RSI (Wilder / EWM) --------------------------------------------------

    @staticmethod
    def _rsi(price: pd.Series, n: int = RSI_PERIOD) -> pd.Series:
        d  = price.diff()
        g  = d.where(d > 0, 0.0)
        l  = -d.where(d < 0, 0.0)
        ag = g.ewm(com=n - 1, min_periods=n, adjust=False).mean()
        al = l.ewm(com=n - 1, min_periods=n, adjust=False).mean()
        rs = ag / al.replace(0, np.nan)
        return (100 - 100 / (1 + rs)).fillna(50)

    # -- Market / sector data ------------------------------------------------

    def _fetch_market_data(self, target_date=None):
        """
        Download Nifty500 + all sector indices and run regime checks.

        Falls back to NSE ETF proxies when ^ index tickers fail (common through
        corporate proxies where Yahoo Finance blocks ^ ticker queries).

        Returns
        -------
        regime_ok      : bool
        bench_df       : pd.DataFrame | None   Nifty500 daily OHLCV
        market_ret20d  : float                 Nifty500 20-day return (%)
        sector_returns : dict                  {sector_label: 20D_return_%}
        """
        # Collect the unique index tickers needed (de-duplicated)
        index_tickers: dict = {}                   # ticker -> etf_fallbacks list
        index_tickers[self.benchmark_ticker] = self.benchmark_etf_fallbacks
        for t in set(SECTOR_INDEX_MAP.values()):
            index_tickers.setdefault(t, SECTOR_INDEX_ETF_FALLBACKS.get(t, []))

        idx_data: dict = {}
        for t, fallbacks in index_tickers.items():
            df = self._fetch_index(t, days=HIST_DAYS, end_date=target_date,
                                   etf_fallbacks=fallbacks)
            if df is not None and len(df) >= SECTOR_LOOKBACK_DAYS + 1:
                idx_data[t] = df
            else:
                logger.debug("Index %s: no usable data (tried %d fallbacks)",
                             t, len(fallbacks))
            time.sleep(0.05)

        bench_df  = idx_data.get(self.benchmark_ticker)
        regime_ok = True

        if bench_df is not None:
            bc = bench_df["Close"].dropna()

            # 0a. 20 EMA > 50 EMA
            if len(bc) >= EMA_PERIOD + 1:
                ema20 = float(bc.ewm(span=EMA_SHORT_PERIOD, adjust=False,
                                     min_periods=EMA_SHORT_PERIOD).mean().iloc[-1])
                ema50 = float(bc.ewm(span=EMA_PERIOD, adjust=False,
                                     min_periods=EMA_PERIOD).mean().iloc[-1])
                if ema20 <= ema50:
                    regime_ok = False
                    logger.info("%s regime FAIL (EMA): 20EMA=%.2f <= 50EMA=%.2f",
                                self.label, ema20, ema50)
                else:
                    logger.info("%s EMA regime OK: 20EMA=%.2f > 50EMA=%.2f",
                                self.label, ema20, ema50)

            # 0b. RSI > 50 for at least REGIME_RSI_REQUIRE_DAYS of the last 3 days
            if regime_ok and len(bc) >= RSI_PERIOD + 3:
                rsi_s  = self._rsi(bc, RSI_PERIOD)
                last3  = rsi_s.iloc[-3:]
                days_above = int((last3 > 50).sum())
                if days_above < REGIME_RSI_REQUIRE_DAYS:
                    regime_ok = False
                    logger.info("%s regime FAIL (RSI): last-3 = %s  (%d/%d days above 50)",
                                self.label,
                                [round(x, 1) for x in last3.tolist()],
                                days_above, REGIME_RSI_REQUIRE_DAYS)
                else:
                    logger.info("%s RSI regime OK: last-3 RSI = %.1f, %.1f, %.1f  (%d/%d days >50)",
                                self.label, *last3.tolist(),
                                days_above, REGIME_RSI_REQUIRE_DAYS)
        else:
            logger.warning("Benchmark ticker %s unavailable (and all ETF fallbacks failed). "
                           "Regime check skipped; market_ret20d will be estimated from "
                           "downloaded stock data.", self.benchmark_ticker)

        def _ret20(df: pd.DataFrame) -> float:
            c = df["Close"].dropna()
            if len(c) < SECTOR_LOOKBACK_DAYS + 1:
                return 0.0
            return float((c.iloc[-1] / c.iloc[-(SECTOR_LOOKBACK_DAYS + 1)] - 1) * 100)

        market_ret20d = _ret20(bench_df) if bench_df is not None else 0.0
        if bench_df is not None:
            logger.info("Nifty500 20D return: %.2f%%", market_ret20d)
        else:
            logger.info("Nifty500 20D return: N/A (benchmark unavailable; "
                        "will be computed from stock data after OHLCV download)")

        # Build sector returns (first mapping per label wins)
        sector_returns: dict = {}
        for name, ticker in SECTOR_INDEX_MAP.items():
            if name not in sector_returns:
                sector_returns[name] = _ret20(idx_data[ticker]) if ticker in idx_data else None

        return regime_ok, bench_df, market_ret20d, sector_returns

    # -- Per-stock analysis (all technical filters) --------------------------

    def _analyze(self, ticker: str, df: pd.DataFrame,
                 bench_df: "pd.DataFrame | None",
                 bse_date: "dt_mod.date | None" = None) -> "dict | None":
        """
        Apply all technical filters. Returns result dict on pass, None on fail.
        bse_date: if provided, BSE Bhavcopy is checked and the latest candle's
                  volume/price may be patched when Yahoo Finance shows zero volume.
        """
        sym = ticker.replace(".NS", "")

        c  = df["Close"].dropna()
        v  = df["Volume"].reindex(c.index).fillna(0)
        h  = df["High"].reindex(c.index)
        lo = df["Low"].reindex(c.index)
        o  = df["Open"].reindex(c.index)

        if len(c) < MIN_DATA_ROWS:
            return None

        cp = float(c.iloc[-1])   # entry price
        ch = float(h.iloc[-1])
        cl = float(lo.iloc[-1])
        co = float(o.iloc[-1])

        # ---- BSE Bhavcopy patch (latest candle) ----------------------------
        # When Yahoo Finance shows zero or suspiciously low volume for the most
        # recent session, patch Open/High/Low/Close/Volume from BSE Bhavcopy.
        bse_row = None
        if bse_date is not None:
            last_yf_date = c.index[-1].date()
            # Only patch if the last YF row is on or near the bse_date
            if abs((last_yf_date - bse_date).days) <= 3:
                bse_row = self._bse.get(sym, bse_date)
                if bse_row and bse_row["close"] > 0:
                    yf_last_vol = float(v.iloc[-1])
                    bse_vol     = bse_row["volume"]
                    # Patch when YF volume is zero OR deviates >70% from BSE
                    needs_patch = (
                        yf_last_vol == 0
                        or (bse_vol > 0 and (
                            yf_last_vol < bse_vol * 0.30
                            or yf_last_vol > bse_vol * 3.0
                        ))
                    )
                    if needs_patch:
                        logger.debug(
                            "%-12s  BSE patch: O=%.2f H=%.2f L=%.2f C=%.2f "
                            "Vol=%.0f (YF vol was %.0f)",
                            sym, bse_row["open"], bse_row["high"],
                            bse_row["low"],  bse_row["close"],
                            bse_vol, yf_last_vol,
                        )
                        last_idx = df.index[-1]
                        # Patch df in-place so ATR (df.ta) and weekly resample
                        # also see the corrected values for the latest candle.
                        df.loc[last_idx, "Open"]   = bse_row["open"]
                        df.loc[last_idx, "High"]   = bse_row["high"]
                        df.loc[last_idx, "Low"]    = bse_row["low"]
                        df.loc[last_idx, "Close"]  = bse_row["close"]
                        df.loc[last_idx, "Volume"] = bse_vol
                        # Re-derive working series from the now-patched df
                        c  = df["Close"].dropna()
                        v  = df["Volume"].reindex(c.index).fillna(0)
                        h  = df["High"].reindex(c.index)
                        lo = df["Low"].reindex(c.index)
                        o  = df["Open"].reindex(c.index)
                        # Refresh scalar snapshots
                        cp = bse_row["close"]
                        ch = bse_row["high"]
                        cl = bse_row["low"]
                        co = bse_row["open"]

        # ---- ATR ----------------------------------------------------------------
        try:
            with _suppress_ta_stdout():
                atr5_s  = _ta_atr(df, length=5)
                atr14_s = _ta_atr(df, length=14)
                atr20_s = _ta_atr(df, length=20)
        except Exception as exc:
            logger.debug("%-12s SKIP  ATR error: %s", sym, exc)
            return None

        if atr14_s is None or atr14_s.dropna().empty:
            return None
        atr5  = float(atr5_s.iloc[-1])  if atr5_s  is not None else float("nan")
        atr14 = float(atr14_s.iloc[-1])
        atr20 = float(atr20_s.iloc[-1]) if atr20_s is not None else float("nan")

        if any(pd.isna(x) for x in (atr5, atr14, atr20)) or atr20 <= 0:
            return None

        # Filter 4: ATR5 < 0.88 x ATR20  (skip when REQUIRE_ATR_CONTRACTION=False)
        if REQUIRE_ATR_CONTRACTION and atr5 >= ATR_RATIO_MAX * atr20:
            logger.debug("%-12s SKIP  ATR5/ATR20 = %.3f >= %.2f",
                         sym, atr5 / atr20, ATR_RATIO_MAX)
            return None

        # ---- EMA -----------------------------------------------------------
        ema20_s = c.ewm(span=EMA_SHORT_PERIOD, adjust=False,
                        min_periods=EMA_SHORT_PERIOD).mean()
        ema50_s = c.ewm(span=EMA_PERIOD, adjust=False,
                        min_periods=EMA_PERIOD).mean()
        e20     = float(ema20_s.iloc[-1])
        e50     = float(ema50_s.iloc[-1])

        # Filter 5: 20 EMA > 50 EMA  (daily trend aligned)
        if pd.isna(e20) or pd.isna(e50) or e20 <= e50:
            logger.debug("%-12s SKIP  20EMA %.2f <= 50EMA %.2f", sym, e20, e50)
            return None

        # Filter 5b: Close > 20 EMA  (price must be ABOVE its short-term average)
        # Without this, a stock with EMA20 > EMA50 but price below EMA20 passes —
        # that is a stock in a pullback, not in confirmed momentum. We want price
        # already trading above the average, showing buyer conviction.
        if cp <= e20:
            logger.debug("%-12s SKIP  close %.2f <= EMA20 %.2f (below average)", sym, cp, e20)
            return None

        # ---- Traded value --------------------------------------------------
        tv = c * v   # INR

        # Filter 1: Avg traded value 20D (always active — primary liquidity gate)
        avg_tv_20d = float(tv.iloc[-VOLUME_AVG_DAYS:].mean())
        if avg_tv_20d < AVG_TRADED_VALUE_20D_MIN:
            logger.debug("%-12s SKIP  avg TV 20D Rs.%.2fCr < %.0fCr",
                         sym, avg_tv_20d / 1e7, AVG_TRADED_VALUE_20D_MIN / 1e7)
            return None

        # Filter 2: Median traded value 20D (skip when REQUIRE_MEDIAN_TV_20D=False)
        # Redundant with Filter 1 for swing trading — avg already ensures liquidity.
        med_tv_20d = float(tv.iloc[-VOLUME_AVG_DAYS:].median())
        if REQUIRE_MEDIAN_TV_20D and med_tv_20d < MEDIAN_TRADED_VALUE_20D_MIN:
            logger.debug("%-12s SKIP  median TV 20D Rs.%.2fCr < %.0fCr",
                         sym, med_tv_20d / 1e7, MEDIAN_TRADED_VALUE_20D_MIN / 1e7)
            return None

        # Filter 3: Relative Volume Percentile Rank > threshold
        # Use the rolling average of the last VOLUME_LOOKBACK_DAYS candles so that
        # intraday scans (incomplete today candle) and single-day noise don't cause
        # every stock to fail.  The lookback avg is compared to the prior 60 sessions.
        if len(v) < 61 + VOLUME_LOOKBACK_DAYS:
            return None
        cur_vol_avg = float(v.iloc[-VOLUME_LOOKBACK_DAYS:].mean())   # rolling avg
        past60_vol  = v.iloc[-(61 + VOLUME_LOOKBACK_DAYS):-VOLUME_LOOKBACK_DAYS].values
        rel_vol_pct = float(np.sum(past60_vol < cur_vol_avg)) / len(past60_vol) * 100
        if rel_vol_pct <= REL_VOL_PERCENTILE_MIN:
            logger.debug("%-12s SKIP  rel-vol pctile %.1f <= %d",
                         sym, rel_vol_pct, REL_VOL_PERCENTILE_MIN)
            return None

        # ---- Weekly data ---------------------------------------------------
        weekly = (
            df.resample("W-FRI")
              .agg({"Open": "first", "High": "max", "Low": "min",
                    "Close": "last", "Volume": "sum"})
              .dropna(subset=["Close"])
        )
        if len(weekly) < 25:
            logger.debug("%-12s SKIP  insufficient weekly data (%d weeks)",
                         sym, len(weekly))
            return None

        # Filter 6: Weekly close > weekly 20 EMA
        # Skip when REQUIRE_WEEKLY_EMA=False (default for swing trades).
        # The daily 20EMA>50EMA filter (F5) already confirms the stock is in an
        # uptrend.  Weekly EMA adds a lagging confirmation that rejects stocks
        # pulling back toward their weekly EMA — exactly the early-entry zone.
        w_ema20 = weekly["Close"].ewm(span=EMA_SHORT_PERIOD, adjust=False,
                                      min_periods=EMA_SHORT_PERIOD).mean()
        w_close = float(weekly["Close"].iloc[-1])
        w_e20   = float(w_ema20.iloc[-1])
        if REQUIRE_WEEKLY_EMA:
            if pd.isna(w_e20) or w_close <= w_e20:
                logger.debug("%-12s SKIP  weekly close %.2f <= w-EMA20 %.2f",
                             sym, w_close, w_e20)
                return None

        # Filter 7a: Close > Highest High of last 20 bars (breakout)
        # Skip entirely when REQUIRE_HH20_BREAKOUT=False (weak market mode)
        if len(h) < 22:
            return None
        hh20 = float(h.iloc[-21:-1].max())
        if REQUIRE_HH20_BREAKOUT and cp <= hh20:
            logger.debug("%-12s SKIP  Close %.2f <= HH20 %.2f", sym, cp, hh20)
            return None

        # Filter 7b: Close >= High - PRICE_PROXIMITY_MAX% x (High - Low)
        # Skip when REQUIRE_PRICE_PROXIMITY=False (default for swing trades).
        # This is a strict single-candle test; it rejects stocks whose candle
        # closes mid-range, but for next-day swing entries that is irrelevant.
        candle_range = ch - cl
        if REQUIRE_PRICE_PROXIMITY:
            if candle_range > 0 and cp < ch - PRICE_PROXIMITY_MAX * candle_range:
                logger.debug("%-12s SKIP  close not near candle high", sym)
                return None

        # Filter 8: Close <= 20EMA + N×ATR14  (not overextended)
        # Skip entirely when REQUIRE_EMA_ATR_CEILING=False.
        ema_atr_ceil = e20 + EMA_ATR_MULTIPLIER * atr14
        if REQUIRE_EMA_ATR_CEILING and cp > ema_atr_ceil:
            logger.debug("%-12s SKIP  overextended: close %.2f > ceil %.2f (EMA+%.1f×ATR)",
                         sym, cp, ema_atr_ceil, EMA_ATR_MULTIPLIER)
            return None

        # Filter 9: Closing range >= 50% (skip when REQUIRE_CLOSING_RANGE=False)
        # Redundant with Filter 7b (candle proximity): if close is within the top
        # 30% of the H-L range (7b), it almost always passes the 50% test too.
        if REQUIRE_CLOSING_RANGE and candle_range > 0 and (cp - cl) / candle_range < CLOSING_RANGE_MIN:
            logger.debug("%-12s SKIP  closing range %.2f < %.2f",
                         sym, (cp - cl) / candle_range, CLOSING_RANGE_MIN)
            return None

        # Filter 10: Weekly RSI > 58
        w_rsi_s = self._rsi(weekly["Close"], 14)
        w_rsi   = float(w_rsi_s.iloc[-1])
        if w_rsi < WEEKLY_RSI_MIN:
            logger.debug("%-12s SKIP  weekly RSI %.1f < %d", sym, w_rsi, WEEKLY_RSI_MIN)
            return None

        # Filter 11: RSI(14) > RSI_MIN  AND  (optionally) RSI SMA(3) rising
        rsi_s   = self._rsi(c, RSI_PERIOD)
        rsi_val = float(rsi_s.iloc[-1])
        if rsi_val < RSI_MIN:
            logger.debug("%-12s SKIP  RSI %.1f < %d", sym, rsi_val, RSI_MIN)
            return None
        rsi_sma3 = rsi_s.rolling(3).mean()
        if REQUIRE_RSI_SMA3_RISING:
            if rsi_sma3.dropna().empty or \
                    float(rsi_sma3.iloc[-1]) <= float(rsi_sma3.iloc[-2]):
                logger.debug("%-12s SKIP  RSI SMA(3) not rising", sym)
                return None

        # Filter 12a: Volume Z-Score > threshold  (window = VOLUME_AVG_DAYS + lookback bars)
        # Use the same VOLUME_LOOKBACK_DAYS rolling average as Filter 3 so both
        # filters are consistent and immune to single-session volume noise.
        if len(v) < VOLUME_AVG_DAYS + VOLUME_LOOKBACK_DAYS + 1:
            return None
        vol_window = v.iloc[-(VOLUME_AVG_DAYS + VOLUME_LOOKBACK_DAYS):-VOLUME_LOOKBACK_DAYS]
        vol_mean   = float(vol_window.mean())
        vol_std    = float(vol_window.std())
        vol_zscore = (cur_vol_avg - vol_mean) / vol_std if vol_std > 0 else 0.0
        if vol_zscore <= VOLUME_ZSCORE_MIN:
            logger.debug("%-12s SKIP  vol Z-score %.2f <= %.1f",
                         sym, vol_zscore, VOLUME_ZSCORE_MIN)
            return None

        # Filter 12b: Median TV(5D) > Median TV(20D) (skip when REQUIRE_MEDIAN_TV_TREND=False)
        # Redundant for swing: Z-Score (12a) and Rel-Vol-Pct (3) already capture
        # volume expansion.  Three overlapping volume filters is overkill.
        med_tv_5d = float(tv.iloc[-5:].median())
        if REQUIRE_MEDIAN_TV_TREND and med_tv_5d <= med_tv_20d:
            logger.debug("%-12s SKIP  median TV5D <= median TV20D", sym)
            return None

        # Filter 13: RS Ratio trending up
        # When REQUIRE_RS_UPTREND=True  : RS SMA(10) > RS SMA(20) — confirmed multi-week uptrend
        # When REQUIRE_RS_UPTREND=False : Filter 13 is SKIPPED entirely.
        #   Relative-strength is still enforced by Filter 18 (MOMENTUM_OUTPERFORM_MIN = 0%
        #   means the stock must not underperform the index over 20D), which uses
        #   properly normalised 20D return comparison — not raw price ratio.
        if bench_df is None:
            return None
        bench_c  = bench_df["Close"].reindex(c.index, method="ffill").dropna()
        if len(bench_c) < 25:
            return None
        rs_line  = c.reindex(bench_c.index) / bench_c
        rs_sma10 = rs_line.rolling(10).mean()
        rs_sma20 = rs_line.rolling(20).mean()
        rs10_v   = float(rs_sma10.dropna().iloc[-1]) if not rs_sma10.dropna().empty else float("nan")
        rs20_v   = float(rs_sma20.dropna().iloc[-1]) if not rs_sma20.dropna().empty else float("nan")
        if REQUIRE_RS_UPTREND:
            if pd.isna(rs10_v) or pd.isna(rs20_v) or rs10_v <= rs20_v:
                logger.debug("%-12s SKIP  RS SMA10 %.5f <= RS SMA20 %.5f",
                             sym, rs10_v, rs20_v)
                return None
        # else: skip Filter 13; relative strength captured by Filter 18 (momentum outperform)

        # Filter 14: Gap-up from prev close <= 4%
        if len(c) < 2:
            return None
        prev_close = float(c.iloc[-2])
        gap = (co - prev_close) / prev_close if prev_close > 0 else 0.0
        if gap > GAP_UP_MAX:
            logger.debug("%-12s SKIP  gap %.2f%% > %.0f%%",
                         sym, gap * 100, GAP_UP_MAX * 100)
            return None

        # Filter 15: ADX(14) > ADX_MIN  AND  +DI > -DI
        # When REQUIRE_ADX_THRESHOLD=False (swing default): only +DI > -DI is enforced.
        # This allows early breakout entries where ADX is still rising from low levels
        # (trend is starting) rather than requiring an "established" trend (ADX > 20).
        try:
            with _suppress_ta_stdout():
                adx_res = _ta_adx(df, length=ADX_PERIOD)
        except Exception as exc:
            logger.debug("%-12s SKIP  ADX error: %s", sym, exc)
            return None
        if adx_res is None or adx_res.empty:
            return None
        adx_col = "ADX_%d" % ADX_PERIOD
        pdi_col = "DMP_%d" % ADX_PERIOD
        ndi_col = "DMN_%d" % ADX_PERIOD
        if not all(c_ in adx_res.columns for c_ in (adx_col, pdi_col, ndi_col)):
            return None
        adx_val = float(adx_res[adx_col].iloc[-1])
        pdi_val = float(adx_res[pdi_col].iloc[-1])
        ndi_val = float(adx_res[ndi_col].iloc[-1])
        if REQUIRE_ADX_THRESHOLD:
            if pd.isna(adx_val) or adx_val <= ADX_MIN:
                logger.debug("%-12s SKIP  ADX %.1f <= %d", sym, adx_val, ADX_MIN)
                return None
        if pd.isna(pdi_val) or pd.isna(ndi_val) or pdi_val <= ndi_val:
            logger.debug("%-12s SKIP  +DI %.1f <= -DI %.1f", sym, pdi_val, ndi_val)
            return None

        # ---- RS ratio (momentum filter + scoring) --------------------------
        if len(c) <= SECTOR_LOOKBACK_DAYS:
            return None
        stock_ret = float(c.iloc[-1] / c.iloc[-(SECTOR_LOOKBACK_DAYS + 1)] - 1)
        # bench_c already has .dropna() applied (line above)
        if len(bench_c) <= SECTOR_LOOKBACK_DAYS:
            return None
        index_ret  = float(bench_c.iloc[-1] / bench_c.iloc[-(SECTOR_LOOKBACK_DAYS + 1)] - 1)
        rs_ratio  = (1 + stock_ret) / (1 + index_ret)
        rs_outperf = stock_ret - index_ret   # decimal

        # Filter 18: stock_ret - index_ret > MOMENTUM_OUTPERFORM_MIN
        if rs_outperf <= MOMENTUM_OUTPERFORM_MIN:
            logger.debug("%-12s SKIP  RS outperf %.4f <= %.2f",
                         sym, rs_outperf, MOMENTUM_OUTPERFORM_MIN)
            return None

        # Filter 19: 5-day return must be non-negative (not losing last week)
        # A stock with strong 20D return but a negative 5D return is already
        # rolling over — the momentum is stalling or reversing near-term.
        if len(c) >= 6:
            r5d = float(c.iloc[-1] / c.iloc[-6] - 1) * 100
            if r5d < 0.0:
                logger.debug("%-12s SKIP  5D return %.2f%% < 0 (rolling over)", sym, r5d)
                return None

        # ---- Returns -------------------------------------------------------
        r1m = round(stock_ret * 100, 2)
        r3m = round(float(c.iloc[-1] / c.iloc[-(RETURN_3M_DAYS + 1)] - 1) * 100, 2) \
              if len(c) > RETURN_3M_DAYS else 0.0

        # ---- Stop loss: Optimised tight structure-aware stop ----------------
        # Base: 1.0×ATR14 below close (tighter than 1.5× used previously).
        # Structural lift: raise to the highest of (candle low, recent 3-bar
        # swing low) when those levels sit above the ATR floor — placing the
        # stop just below the nearest technical support.
        # Bounds: [cp − 1.5×ATR14 … cp − 0.5×ATR14]
        #   • Lower bound (1.5×) keeps the stop from becoming unrealistically wide.
        #   • Upper bound (0.5×) prevents stops that are too close to price noise.
        _sl_recent_low = float(lo.iloc[-4:-1].min()) if len(lo) >= 4 else cl
        _sl_structural = max(cl, _sl_recent_low)        # nearest technical floor
        _sl_candidate  = max(_sl_structural, cp - 1.0 * atr14)
        _sl_lo         = cp - 1.5 * atr14              # never wider than 1.5×ATR
        _sl_hi         = cp - 0.5 * atr14              # never tighter than 0.5×ATR
        stop_loss      = round(min(max(_sl_candidate, _sl_lo), _sl_hi), 2)

        logger.debug(
            "%-12s PASS  rsi=%.1f wRSI=%.1f adx=%.1f +DI=%.1f "
            "volZ=%.2f rvPct=%.0f rs=%.4f(+%.2f%%) sl=%.2f",
            sym, rsi_val, w_rsi, adx_val, pdi_val,
            vol_zscore, rel_vol_pct, rs_ratio, rs_outperf * 100, stop_loss,
        )

        return {
            "ticker":          ticker,
            "display_ticker":  sym,
            "name":            sym,
            "price":           round(cp, 2),
            "stop_loss":       stop_loss,
            "atr14":           round(atr14, 2),
            "vol_zscore":      round(vol_zscore, 2),
            "rel_vol_pct":     round(rel_vol_pct, 1),
            "rsi":             round(rsi_val, 2),
            "weekly_rsi":      round(w_rsi, 2),
            "adx":             round(adx_val, 2),
            "pdi":             round(pdi_val, 2),
            "ndi":             round(ndi_val, 2),
            "price_vs_ema20":  round((cp / e20 - 1) * 100, 2),
            "ema20_vs_50":     round((e20 / e50 - 1) * 100, 2),
            "return_1m":       r1m,
            "return_3m":       r3m,
            "return_20d":      r1m,           # alias used in momentum step
            "rs_ratio":        round(rs_ratio, 4),
            "rs_outperf_pct":  round(rs_outperf * 100, 2),
            "avg_tv_20d_cr":   round(avg_tv_20d / 1e7, 2),
        }

    # -- Morning Star candlestick pattern detector --------------------------------

    @staticmethod
    def _is_morning_star(df: "pd.DataFrame", atr14: "float | None" = None) -> "dict | None":
        """
        Strict Morning Star / Morning Doji Star detection — last 3 bars only.

        Returns a detail dict on success, None on failure.  Callers can use
        `if result:` as a boolean check since None is falsy.

        Strict criteria (all must be met):
        ────────────────────────────────────
        Day 1  Large BEARISH candle
               • body >= 1.0% of price  (significant selling, not a small dip)
               • body >= 0.3 × ATR14 when ATR is available

        Day 2  Tiny indecision STAR
               • |close − open| < 35% of Day-1 body  (very small relative to Day-1)

        Day 3  Strong BULLISH recovery
               • close > open  (bullish)
               • close >= Day-1 close + 50% × Day-1 body  (≥ midpoint — classic rule)
               • body >= 50% of Day-1 body  (strong buyers, not a small green candle)

        Only the most recent 3 bars are checked.  No sliding window — the pattern
        must be forming NOW, not in a completed state 1–2 days ago.

        Return dict keys used for Star Score:
          penetration  (c3 − c1) / body1  — 0.5 = midpoint, 1.0 = full engulf
          body1        size of bearish Day-1 candle
          body3        size of bullish Day-3 candle
          star_ratio   body2 / body1  — lower = cleaner star
          full_engulf  True when c3 >= o1  (Day-3 closes above Day-1 open)
          bars_ago     always 0 (last 3 bars)
          o1, c1, c3   raw prices for further checks
        """
        if len(df) < 3:
            return None
        try:
            raw_o = df["Open"].values
            raw_c = df["Close"].values

            o1, c1 = float(raw_o[-3]), float(raw_c[-3])   # Day 1 — bearish
            o2, c2 = float(raw_o[-2]), float(raw_c[-2])   # Day 2 — star
            o3, c3 = float(raw_o[-1]), float(raw_c[-1])   # Day 3 — bullish

            # All prices must be positive
            if any(x <= 0 for x in (o1, c1, o2, c2, o3, c3)):
                return None

            # Day 1: must be bearish
            if c1 >= o1:
                return None

            # Day 3: must be bullish
            if c3 <= o3:
                return None

            body1 = o1 - c1           # positive (bearish)
            body2 = abs(c2 - o2)      # star body (agnostic)
            body3 = c3 - o3           # positive (bullish)

            if body1 <= 0:
                return None

            # Day-1 must be a genuinely significant candle
            # Floor: max(1.0% of price, 0.3×ATR14)
            min_body = (atr14 * 0.3) if (atr14 and atr14 > 0) else 0.0
            price_pct_floor = o1 * 0.010   # 1.0% of Day-1 open price
            significant_body = max(min_body, price_pct_floor)
            if body1 < significant_body:
                return None

            # Day-2 star must be tiny relative to Day-1 (< 35%)
            if body2 >= body1 * 0.35:
                return None

            # Day-3 must close at or above the midpoint of Day-1's body
            # midpoint = c1 + 50% × body1
            if c3 < c1 + body1 * 0.50:
                return None

            # Day-3 body must be strong — at least 50% of Day-1 body
            # This ensures genuine buying conviction, not a tiny green candle
            if body3 < body1 * 0.50:
                return None

            # Pattern passes all strict criteria
            penetration = (c3 - c1) / body1
            full_engulf = c3 >= o1
            return {
                "penetration": penetration,
                "body1":       body1,
                "body3":       body3,
                "star_ratio":  body2 / body1,
                "full_engulf": full_engulf,
                "bars_ago":    0,
                "o1": o1, "c1": c1, "c3": c3,
            }

        except Exception:
            return None

    # -- Momentum-only analysis (early-detection, quality-confirmed) -------------

    def _analyze_momentum(self, ticker: str, df: "pd.DataFrame",
                          bench_df: "pd.DataFrame | None",
                          scan_date: "datetime.date | None" = None) -> "dict | None":
        """
        Early-detection momentum scan — catches nascent up-moves BEFORE they
        become obvious, while mandatory rising-trend confirmations keep quality high.

        Hard filters (all must pass):
          1.  Avg TV 20D     >= MOM_TV_MIN_CR          liquidity floor
          2.  Volume Z-score >= MOM_VOLZ_MIN            above-average accumulation
          3.  Weekly RSI-14  >= MOM_WRSI_MIN            weekly trend constructive
          3b. Weekly RSI rising (≤ 2 pt pullback OK)    weekly trend pointing UP
          4.  RSI-14         >= MOM_RSI_MIN             daily momentum zone
          4b. RSI SMA-3 rising                          momentum accelerating
          5.  ADX-14         >= MOM_ADX_MIN             trend establishing
          5b. +DI > −DI                                 direction is UP
          5c. ADX rising (≤ 3 pt dip OK)               trend strengthening
          6.  RS outperf     >= MOM_RS_MIN%             beats benchmark over 20D
          6b. 20D return     >= MOM_RET20_MIN%          stock is moving
          7.  5-day return   >= MOM_RET5_MIN%           not rolling over last week
          8.  Price > EMA-20 > EMA-50                   clean uptrend structure
          9.  MACD line > Signal AND MACD > 0           bullish zone confirmed
          9b. MACD histogram not contracting > 30%      momentum still accelerating

        scan_date: when provided, df is sliced to <= scan_date before any
          computation, the last candle's date is validated for freshness
          (≤ 5 calendar days stale), and BSE Bhavcopy is used to patch
          the latest candle's OHLCV/volume — matching the behaviour of
          _analyze() so that momentum-only scans are equally accurate.

        Intentionally skips ALL Swing Trade entry conditions (HH20 breakout,
        closing range, price proximity, ATR ceiling, weekly EMA threshold,
        RS uptrend line, fundamentals, etc.) — only trend and momentum quality
        filters apply.
        """
        sym = ticker.replace(".NS", "")

        # ── Date-anchored slice ─────────────────────────────────────────────────
        # Defensive guard: trim any rows whose index is beyond the scan_date.
        # This matters for live-mode cached data that may already have today's
        # bar appended while we are running a historical / intra-day rescan.
        if scan_date is not None:
            df = df[df.index.normalize() <= pd.Timestamp(scan_date)]
            if df.empty:
                return None

        c   = df["Close"].dropna()
        v   = df["Volume"].reindex(c.index).fillna(0)
        lo  = df["Low"].reindex(c.index)

        if len(c) < MIN_DATA_ROWS:
            return None

        # ── Last-candle freshness check ─────────────────────────────────────────
        # Reject the ticker if its most recent candle is more than 5 calendar
        # days before the scan reference date — stale data produces wrong signals.
        candle_date: "datetime.date | None" = None
        try:
            candle_date = c.index[-1].date()
        except Exception:
            pass
        if scan_date is not None and candle_date is not None:
            staleness = (scan_date - candle_date).days
            if staleness > 5:
                logger.debug(
                    "%-12s SKIP  last candle %s is %d days before scan_date %s (stale)",
                    sym, candle_date, staleness, scan_date,
                )
                return None

        cp = float(c.iloc[-1])
        if cp <= 0:
            return None

        # ── BSE Bhavcopy patch for the latest candle ────────────────────────────
        # When running in momentum_only=True mode _analyze() is never called, so
        # the YF volume / price patch that _analyze() normally applies is missing.
        # We replicate the same logic here so volume-based filters see accurate data.
        if scan_date is not None and candle_date is not None:
            if abs((candle_date - scan_date).days) <= 3:
                bse_row = self._bse.get(sym, scan_date)
                if bse_row and bse_row.get("close", 0) > 0:
                    yf_last_vol = float(v.iloc[-1])
                    bse_vol     = bse_row["volume"]
                    needs_patch = (
                        yf_last_vol == 0
                        or (bse_vol > 0 and (
                            yf_last_vol < bse_vol * 0.30
                            or yf_last_vol > bse_vol * 3.0
                        ))
                    )
                    if needs_patch:
                        logger.debug(
                            "%-12s  [mom] BSE patch: O=%.2f H=%.2f L=%.2f C=%.2f "
                            "Vol=%.0f (YF vol was %.0f)",
                            sym, bse_row["open"], bse_row["high"],
                            bse_row["low"],  bse_row["close"],
                            bse_vol, yf_last_vol,
                        )
                        last_idx = df.index[-1]
                        df = df.copy()   # avoid mutating shared cached df
                        df.loc[last_idx, "Open"]   = bse_row["open"]
                        df.loc[last_idx, "High"]   = bse_row["high"]
                        df.loc[last_idx, "Low"]    = bse_row["low"]
                        df.loc[last_idx, "Close"]  = bse_row["close"]
                        df.loc[last_idx, "Volume"] = bse_vol
                        # Re-derive series from the patched df
                        c  = df["Close"].dropna()
                        v  = df["Volume"].reindex(c.index).fillna(0)
                        lo = df["Low"].reindex(c.index)
                        cp = bse_row["close"]

        # 1. Liquidity — very relaxed floor (0.6 Cr vs 3 Cr for swing)
        tv         = c * v
        avg_tv_20d = float(tv.iloc[-VOLUME_AVG_DAYS:].mean())
        if avg_tv_20d < MOM_TV_MIN_CR * 1e7:
            return None

        # 2. Volume Z-score
        if len(v) < VOLUME_AVG_DAYS + VOLUME_LOOKBACK_DAYS + 1:
            return None
        cur_vol_avg = float(v.iloc[-VOLUME_LOOKBACK_DAYS:].mean())
        vol_window  = v.iloc[-(VOLUME_AVG_DAYS + VOLUME_LOOKBACK_DAYS):-VOLUME_LOOKBACK_DAYS]
        vol_mean    = float(vol_window.mean())
        vol_std     = float(vol_window.std())
        vol_zscore  = (cur_vol_avg - vol_mean) / vol_std if vol_std > 0 else 0.0
        if vol_zscore < MOM_VOLZ_MIN:
            return None

        # 3 & 4. Weekly RSI and daily RSI — share the weekly resample
        weekly = (
            df.resample("W-FRI")
              .agg({"Open": "first", "High": "max", "Low": "min",
                    "Close": "last",  "Volume": "sum"})
              .dropna(subset=["Close"])
        )
        if len(weekly) < 25:
            return None
        w_rsi_s = self._rsi(weekly["Close"], 14)
        w_rsi   = float(w_rsi_s.iloc[-1])
        if w_rsi < MOM_WRSI_MIN:
            return None

        rsi_s   = self._rsi(c, RSI_PERIOD)
        rsi_val = float(rsi_s.iloc[-1])
        if rsi_val < MOM_RSI_MIN:
            return None

        # 3b. RSI SMA-3 must be RISING — confirms momentum is accelerating, not stalling.
        #     This is the key quality gate that separates early genuine movers from
        #     stocks that briefly touch the RSI threshold and immediately reverse.
        rsi_sma3 = rsi_s.rolling(3).mean()
        if len(rsi_sma3.dropna()) >= 2:
            rsi_sma3_rising = float(rsi_sma3.iloc[-1]) > float(rsi_sma3.iloc[-2])
            if not rsi_sma3_rising:
                return None

        # 3c. Weekly RSI must be RISING (not falling) — higher-timeframe trend is
        #     turning constructive.  Catches early entries by accepting lower absolute
        #     weekly RSI levels, but only when the weekly trend is pointing UP.
        if len(w_rsi_s.dropna()) >= 2:
            w_rsi_prev = float(w_rsi_s.iloc[-2])
            if w_rsi < w_rsi_prev - 2.0:   # allow tiny noise (< 2 pts pullback is OK)
                return None

        # 5. ADX >= MOM_ADX_MIN  (expensive — checked after cheap filters)
        try:
            with _suppress_ta_stdout():
                adx_res = _ta_adx(df, length=ADX_PERIOD)
        except Exception:
            return None
        if adx_res is None or adx_res.empty:
            return None
        adx_col = "ADX_%d" % ADX_PERIOD
        pdi_col = "DMP_%d" % ADX_PERIOD
        ndi_col = "DMN_%d" % ADX_PERIOD
        if not all(col in adx_res.columns for col in (adx_col, pdi_col, ndi_col)):
            return None
        adx_val = float(adx_res[adx_col].iloc[-1])
        pdi_val = float(adx_res[pdi_col].iloc[-1])
        ndi_val = float(adx_res[ndi_col].iloc[-1])
        if adx_val < MOM_ADX_MIN:
            return None

        # 5b. +DI must be greater than -DI — the directional trend is UP, not down.
        #     High ADX in a downtrend is a trap; this filter eliminates all falling-
        #     knife stocks that happen to have strong ADX readings.
        if pd.isna(pdi_val) or pd.isna(ndi_val) or pdi_val <= ndi_val:
            return None

        # 5c. ADX must be RISING (trend is strengthening, not exhausting).
        #     Early detection requires lower absolute ADX (20+) but the ADX must be
        #     trending upward — a falling ADX below 25 signals a fading trend.
        if len(adx_res) >= 4:
            adx_prev3 = float(adx_res[adx_col].iloc[-4])
            # Require ADX to be at least as high as it was 3 bars ago (or rising)
            if adx_val < adx_prev3 - 3.0:   # allow small dips (< 3 pts is noise)
                return None

        # 6. RS outperformance and 20D return vs benchmark
        if bench_df is None:
            return None
        bench_c = bench_df["Close"].reindex(c.index, method="ffill").dropna()
        if len(bench_c) < SECTOR_LOOKBACK_DAYS + 1 or len(c) < SECTOR_LOOKBACK_DAYS + 1:
            return None
        stock_ret  = float(c.iloc[-1] / c.iloc[-(SECTOR_LOOKBACK_DAYS + 1)] - 1)
        index_ret  = float(bench_c.iloc[-1] / bench_c.iloc[-(SECTOR_LOOKBACK_DAYS + 1)] - 1)
        rs_outperf = stock_ret - index_ret   # decimal
        r20        = stock_ret * 100         # percent
        if rs_outperf * 100 < MOM_RS_MIN:
            return None
        if r20 < MOM_RET20_MIN:
            return None

        rs_ratio = (1 + stock_ret) / (1 + index_ret)

        # 7. 5-day return must be non-negative (stock must not be losing last week)
        if len(c) >= 6:
            r5 = float(c.iloc[-1] / c.iloc[-6] - 1) * 100
            if r5 < MOM_RET5_MIN:
                return None

        # 8. EMA alignment: price > EMA-20 > EMA-50  (clean uptrend structure)
        #    Without this, a stock with strong 20D return but now in pullback/
        #    breakdown passes all other filters yet is heading the wrong direction.
        ema20_s = c.ewm(span=EMA_SHORT_PERIOD, adjust=False,
                        min_periods=EMA_SHORT_PERIOD).mean()
        ema50_s = c.ewm(span=EMA_PERIOD, adjust=False, min_periods=EMA_PERIOD).mean()
        e20 = float(ema20_s.iloc[-1])
        e50 = float(ema50_s.iloc[-1])
        if e20 <= 0 or e50 <= 0:
            return None
        if cp < e20:          # price below EMA-20 → not in uptrend
            return None
        if e20 < e50:         # EMA-20 below EMA-50 → trend not aligned
            return None

        # 9. MACD(12,26,9): line > signal AND line > 0 AND histogram not contracting.
        #    • line > signal:  bullish crossover / still above (trend live)
        #    • line > 0:       MACD in bull zone — not below zero line
        #    • histogram not contracting > 30%: momentum still accelerating
        #      (histogram = line − signal, so if line > signal it is already > 0;
        #       the contraction check is the meaningful additional quality gate)
        macd_line = macd_signal = macd_hist = None
        try:
            with _suppress_ta_stdout():
                macd_df = _ta_macd(df, fast=12, slow=26, signal=9)
            if macd_df is not None and not macd_df.empty:
                macd_col = "MACD_12_26_9"
                sig_col  = "MACDs_12_26_9"
                hist_col = "MACDh_12_26_9"
                if all(col in macd_df.columns for col in (macd_col, sig_col, hist_col)):
                    macd_line   = float(macd_df[macd_col].iloc[-1])
                    macd_signal = float(macd_df[sig_col].iloc[-1])
                    macd_hist   = float(macd_df[hist_col].iloc[-1])
        except Exception:
            pass

        if macd_line is not None and macd_signal is not None:
            if macd_line <= macd_signal:   # bearish — below signal line
                return None
            if macd_line <= 0:             # below zero line — not in bull zone
                return None
            # Histogram contraction check: histogram > 0 is guaranteed by line > signal,
            # but if it is shrinking by more than 30% the momentum pulse is fading.
            if macd_hist is not None:
                hist_series = macd_df["MACDh_12_26_9"].dropna()
                if len(hist_series) >= 2:
                    h_prev = float(hist_series.iloc[-2])
                    if h_prev > 0 and macd_hist < h_prev * 0.70:
                        return None   # histogram contracting > 30% — momentum fading

        # --- All filters passed; compute display-only metrics ---

        # EMA20 / EMA50 already computed above (for filter 8) — reuse
        price_vs_ema20 = round((cp / e20 - 1) * 100, 2) if e20 > 0 else None
        ema20_vs_50    = round((e20 / e50 - 1) * 100, 2) if e50 > 0 else None

        # Relative-volume percentile (display only)
        if len(v) >= 61 + VOLUME_LOOKBACK_DAYS:
            past60  = v.iloc[-(61 + VOLUME_LOOKBACK_DAYS):-VOLUME_LOOKBACK_DAYS].values
            rel_vol_pct = float(np.sum(past60 < cur_vol_avg)) / len(past60) * 100
        else:
            rel_vol_pct = 0.0

        # 3-month return (display only)
        r3m = round(float(c.iloc[-1] / c.iloc[-(RETURN_3M_DAYS + 1)] - 1) * 100, 2) \
              if len(c) > RETURN_3M_DAYS else 0.0

        # ATR-14 stop loss (computed last — only for stocks passing all 6 filters)
        stop_loss = round(cp * 0.95, 2)   # fallback: 5% below price
        atr14_val = None
        try:
            with _suppress_ta_stdout():
                atr14_s = _ta_atr(df, length=14)
            if atr14_s is not None and not atr14_s.dropna().empty:
                atr14_val  = float(atr14_s.iloc[-1])
                cl         = float(lo.iloc[-1])
                _sl_recent = float(lo.iloc[-4:-1].min()) if len(lo) >= 4 else cl
                _sl_struct = max(cl, _sl_recent)
                _cand      = max(_sl_struct, cp - 1.0 * atr14_val)
                stop_loss  = round(
                    min(max(_cand, cp - 1.5 * atr14_val), cp - 0.5 * atr14_val), 2
                )
        except Exception:
            pass

        # Morning Star pattern check — cheap, run after all 6 filters pass
        morning_star = self._is_morning_star(df, atr14=atr14_val) is not None

        return {
            "ticker":         ticker,
            "display_ticker": sym,
            "name":           sym,
            "price":          round(cp, 2),
            "stop_loss":      stop_loss,
            "atr14":          round(atr14_val, 2) if atr14_val is not None else None,
            "vol_zscore":     round(vol_zscore, 2),
            "rel_vol_pct":    round(rel_vol_pct, 1),
            "rsi":            round(rsi_val, 2),
            "weekly_rsi":     round(w_rsi, 2),
            "adx":            round(adx_val, 2),
            "pdi":            round(pdi_val, 2),
            "ndi":            round(ndi_val, 2),
            "price_vs_ema20": price_vs_ema20,
            "ema20_vs_50":    ema20_vs_50,
            "return_20d":     round(r20, 2),
            "return_1m":      round(r20, 2),
            "return_3m":      r3m,
            "rs_ratio":       round(rs_ratio, 4),
            "rs_outperf_pct": round(rs_outperf * 100, 2),
            "avg_tv_20d_cr":  round(avg_tv_20d / 1e7, 2),
            "morning_star":   morning_star,   # True if 3-candle Morning Star pattern detected
            "macd":           round(macd_line,   4) if macd_line   is not None else None,
            "macd_signal":    round(macd_signal, 4) if macd_signal is not None else None,
            "macd_hist":      round(macd_hist,   4) if macd_hist   is not None else None,
            # Candle-level metadata — date of the last bar actually used
            "candle_date":    candle_date.isoformat() if candle_date else None,
        }

    # -- Individual stock detailed analysis (pass/fail per criterion) ---------

    def analyze_single(self, ticker: str, target_date=None) -> dict:
        """
        Analyze one stock against every criterion and return full pass/fail detail.
        Does NOT abort early -- evaluates all criteria regardless of prior failures.
        Uses _fetch_yf_direct (chart API) for reliable single-ticker OHLCV data.
        """
        if "." not in ticker:
            ticker = ticker.upper() + ".NS"
        sym = ticker.replace(".NS", "").replace(".BO", "")

        criteria: list[dict] = []

        def _c(id_, name, category, passed, value, threshold, detail=""):
            criteria.append({
                "id": id_, "name": name, "category": category,
                "passed": bool(passed),
                "value": str(value), "threshold": threshold, "detail": detail,
            })

        # ── Step 1: Download OHLCV ─────────────────────────────────────────
        # Priority: yfinance direct chart API → TradingView → BSE
        df = self._fetch_yf_direct(ticker, days=HIST_DAYS, end_date=target_date)
        if not self._is_quality_ok(df):
            # Try TradingView (excellent quality, no auth)
            df_tv = _tv_client.get_history(ticker, HIST_DAYS, end_date=target_date)
            if self._is_quality_ok(df_tv):
                df = df_tv
                logger.debug("analyze_single: using TradingView for %s", ticker)
            else:
                # Try BSE (.BO)
                bse_ticker = ticker.replace(".NS", ".BO")
                logger.debug("analyze_single: trying BSE fallback %s", bse_ticker)
                df_bo = self._fetch_yf_direct(bse_ticker, days=HIST_DAYS, end_date=target_date)
                if self._is_quality_ok(df_bo):
                    df = df_bo
                elif df_bo is not None and (df is None or len(df_bo) > len(df or [])):
                    df = df_bo

        if df is None or len(df) < MIN_DATA_ROWS:
            # Last resort: try yf.download() batch path with single ticker
            logger.debug("analyze_single: direct API failed for %s -- trying batch path", ticker)
            batch_result = self._download_batch_ns([ticker], HIST_DAYS, end_date=target_date)
            df = batch_result.get(ticker)
            if df is None or len(df) < MIN_DATA_ROWS:
                rows = len(df) if df is not None else 0
                return {
                    "ticker": ticker, "display_ticker": sym,
                    "error": f"Insufficient OHLCV data ({rows} rows, need {MIN_DATA_ROWS}). "
                             f"Ticker may be delisted or unavailable.",
                    "criteria": [], "summary": {"total": 0, "passed": 0, "failed": 0},
                    "price": None, "stop_loss": None,
                }

        c  = df["Close"].dropna()
        v  = df["Volume"].reindex(c.index).fillna(0)
        h  = df["High"].reindex(c.index)
        lo = df["Low"].reindex(c.index)
        o  = df["Open"].reindex(c.index)
        cp = float(c.iloc[-1])
        ch = float(h.iloc[-1])
        cl = float(lo.iloc[-1])
        co = float(o.iloc[-1])

        # ── BSE patch ───────────────────────────────────────────────────────
        bse_date = self._bse.load_recent(target_date=target_date)
        if bse_date:
            bse_row = self._bse.get(sym, bse_date)
            if bse_row and bse_row["close"] > 0:
                yf_last_vol = float(v.iloc[-1])
                bse_vol     = bse_row["volume"]
                needs_patch = (yf_last_vol == 0 or (bse_vol > 0 and (
                    yf_last_vol < bse_vol * 0.30 or yf_last_vol > bse_vol * 3.0)))
                if needs_patch:
                    last_idx = df.index[-1]
                    df.loc[last_idx, "Open"]   = bse_row["open"]
                    df.loc[last_idx, "High"]   = bse_row["high"]
                    df.loc[last_idx, "Low"]    = bse_row["low"]
                    df.loc[last_idx, "Close"]  = bse_row["close"]
                    df.loc[last_idx, "Volume"] = bse_vol
                    c  = df["Close"].dropna()
                    v  = df["Volume"].reindex(c.index).fillna(0)
                    h  = df["High"].reindex(c.index)
                    lo = df["Low"].reindex(c.index)
                    o  = df["Open"].reindex(c.index)
                    cp = bse_row["close"]; ch = bse_row["high"]
                    cl = bse_row["low"];   co = bse_row["open"]

        # ── Step 2: Market / regime data ────────────────────────────────────
        try:
            _, bench_df, market_ret20d, sector_returns = \
                self._fetch_market_data(target_date=target_date)
        except Exception:
            bench_df = None; market_ret20d = 0.0; sector_returns = {}

        # ── Regime checks ───────────────────────────────────────────────────
        bench_label = self.label
        if bench_df is not None:
            bc = bench_df["Close"].dropna()
            if len(bc) >= EMA_PERIOD + 1:
                b_ema20 = float(bc.ewm(span=EMA_SHORT_PERIOD, adjust=False,
                                       min_periods=EMA_SHORT_PERIOD).mean().iloc[-1])
                b_ema50 = float(bc.ewm(span=EMA_PERIOD, adjust=False,
                                       min_periods=EMA_PERIOD).mean().iloc[-1])
                _c("regime_ema", f"{bench_label} 20EMA > 50EMA", "Regime",
                   b_ema20 > b_ema50,
                   f"20EMA={b_ema20:.2f} / 50EMA={b_ema50:.2f}",
                   "20EMA must be above 50EMA",
                   f"{(b_ema20/b_ema50-1)*100:+.2f}%")
            if len(bc) >= RSI_PERIOD + 3:
                b_rsi  = self._rsi(bc, RSI_PERIOD)
                last3  = b_rsi.iloc[-3:]
                all_ok = bool((last3 > 50).all())
                _c("regime_rsi", f"{bench_label} RSI>50 (3 days)", "Regime",
                   all_ok,
                   f"RSI last 3d: {last3.iloc[-3]:.1f} / {last3.iloc[-2]:.1f} / {last3.iloc[-1]:.1f}",
                   "RSI(14) > 50 for 3 consecutive days")
        else:
            _c("regime_ema", f"{bench_label} 20EMA > 50EMA", "Regime", False,
               "N/A", "Benchmark data unavailable")
            _c("regime_rsi", f"{bench_label} RSI>50 (3 days)", "Regime", False,
               "N/A", "Benchmark data unavailable")

        # ── ATR ─────────────────────────────────────────────────────────────
        try:
            with _suppress_ta_stdout():
                atr5_s  = _ta_atr(df, length=5)
                atr14_s = _ta_atr(df, length=14)
                atr20_s = _ta_atr(df, length=20)
            atr5  = float(atr5_s.iloc[-1])  if atr5_s  is not None else float("nan")
            atr14 = float(atr14_s.iloc[-1]) if atr14_s is not None else float("nan")
            atr20 = float(atr20_s.iloc[-1]) if atr20_s is not None else float("nan")
        except Exception:
            atr5 = atr14 = atr20 = float("nan")

        if not any(pd.isna(x) for x in (atr5, atr14, atr20)) and atr20 > 0:
            _atr_pass = (not REQUIRE_ATR_CONTRACTION) or (atr5 < ATR_RATIO_MAX * atr20)
            _atr_note = "" if REQUIRE_ATR_CONTRACTION else " (filter disabled)"
            _c("atr_contraction", "ATR5 < 0.88 × ATR20", "Technical",
               _atr_pass,
               f"ATR5={atr5:.2f} / ATR20={atr20:.2f} -> ratio={atr5/atr20:.3f}",
               f"Ratio must be < {ATR_RATIO_MAX}{_atr_note}")
        else:
            _c("atr_contraction", "ATR5 < 0.88 × ATR20", "Technical", False,
               "N/A", f"< {ATR_RATIO_MAX}", "ATR calculation failed")

        # ── EMA ─────────────────────────────────────────────────────────────
        ema20_s = c.ewm(span=EMA_SHORT_PERIOD, adjust=False,
                        min_periods=EMA_SHORT_PERIOD).mean()
        ema50_s = c.ewm(span=EMA_PERIOD, adjust=False,
                        min_periods=EMA_PERIOD).mean()
        e20 = float(ema20_s.iloc[-1])
        e50 = float(ema50_s.iloc[-1])
        _c("ema_trend", "20EMA > 50EMA (stock)", "Technical",
           not (pd.isna(e20) or pd.isna(e50)) and e20 > e50,
           f"EMA20={e20:.2f} / EMA50={e50:.2f}",
           "20EMA must be above 50EMA",
           f"{(e20/e50-1)*100:+.2f}%" if e50 > 0 else "")

        # ── Traded Value ────────────────────────────────────────────────────
        tv = c * v
        avg_tv_20d = float(tv.iloc[-VOLUME_AVG_DAYS:].mean())
        med_tv_20d = float(tv.iloc[-VOLUME_AVG_DAYS:].median())
        _c("avg_tv", f"Avg Traded Value 20D > ₹{AVG_TRADED_VALUE_20D_MIN/1e7:.0f}Cr", "Liquidity",
           avg_tv_20d >= AVG_TRADED_VALUE_20D_MIN,
           f"₹{avg_tv_20d/1e7:.2f}Cr",
           f"≥ ₹{AVG_TRADED_VALUE_20D_MIN/1e7:.0f}Cr")
        _med_tv_note = "" if REQUIRE_MEDIAN_TV_20D else " (filter disabled)"
        _c("med_tv", f"Median Traded Value 20D > ₹{MEDIAN_TRADED_VALUE_20D_MIN/1e7:.0f}Cr", "Liquidity",
           (med_tv_20d >= MEDIAN_TRADED_VALUE_20D_MIN) if REQUIRE_MEDIAN_TV_20D else True,
           f"₹{med_tv_20d/1e7:.2f}Cr",
           f"≥ ₹{MEDIAN_TRADED_VALUE_20D_MIN/1e7:.0f}Cr{_med_tv_note}")

        # ── Relative Volume Percentile ───────────────────────────────────────
        if len(v) >= 61 + VOLUME_LOOKBACK_DAYS:
            cur_vol_avg = float(v.iloc[-VOLUME_LOOKBACK_DAYS:].mean())
            past60      = v.iloc[-(61 + VOLUME_LOOKBACK_DAYS):-VOLUME_LOOKBACK_DAYS].values
            rvp         = float(np.sum(past60 < cur_vol_avg)) / len(past60) * 100
            _c("rel_vol_pct", f"Rel Vol Percentile > {REL_VOL_PERCENTILE_MIN} ({VOLUME_LOOKBACK_DAYS}d avg)", "Volume",
               rvp > REL_VOL_PERCENTILE_MIN,
               f"{rvp:.1f}th percentile",
               f"> {REL_VOL_PERCENTILE_MIN}")
        else:
            cur_vol_avg = float(v.iloc[-VOLUME_LOOKBACK_DAYS:].mean()) if len(v) >= VOLUME_LOOKBACK_DAYS else float(v.iloc[-1])
            _c("rel_vol_pct", f"Rel Vol Percentile > {REL_VOL_PERCENTILE_MIN} ({VOLUME_LOOKBACK_DAYS}d avg)", "Volume", False,
               "N/A", f"> {REL_VOL_PERCENTILE_MIN}", "Insufficient data")
            rvp = 0.0

        # ── Weekly data ─────────────────────────────────────────────────────
        weekly = (df.resample("W-FRI")
                    .agg({"Open": "first", "High": "max", "Low": "min",
                          "Close": "last", "Volume": "sum"})
                    .dropna(subset=["Close"]))
        if len(weekly) >= 25:
            w_ema20 = weekly["Close"].ewm(span=EMA_SHORT_PERIOD, adjust=False,
                                          min_periods=EMA_SHORT_PERIOD).mean()
            w_close = float(weekly["Close"].iloc[-1])
            w_e20   = float(w_ema20.iloc[-1])
            _wema_note = "" if REQUIRE_WEEKLY_EMA else " (filter disabled for swing — daily EMA sufficient)"
            _c("weekly_ema", "Weekly Close > Weekly 20EMA", "Technical",
               (not pd.isna(w_e20) and w_close > w_e20) if REQUIRE_WEEKLY_EMA else True,
               f"W-Close={w_close:.2f} / W-EMA20={w_e20:.2f}",
               f"Weekly close above 20-week EMA{_wema_note}")

            w_rsi_s = self._rsi(weekly["Close"], 14)
            w_rsi   = float(w_rsi_s.iloc[-1])
            _c("weekly_rsi", f"Weekly RSI > {WEEKLY_RSI_MIN}", "Momentum",
               w_rsi >= WEEKLY_RSI_MIN,
               f"W-RSI={w_rsi:.1f}",
               f"≥ {WEEKLY_RSI_MIN}")
        else:
            _c("weekly_ema", "Weekly Close > Weekly 20EMA", "Technical", False,
               "N/A", "Weekly EMA", "Insufficient weekly data")
            _c("weekly_rsi", f"Weekly RSI > {WEEKLY_RSI_MIN}", "Momentum", False,
               "N/A", f"≥ {WEEKLY_RSI_MIN}", "Insufficient weekly data")
            w_rsi = 0.0

        # ── Breakout: HH20 ──────────────────────────────────────────────────
        if len(h) >= 22:
            hh20 = float(h.iloc[-21:-1].max())
            _hh20_pass = (not REQUIRE_HH20_BREAKOUT) or (cp > hh20)
            _hh20_note = "" if REQUIRE_HH20_BREAKOUT else " (filter disabled)"
            _c("hh20_breakout", "Close > Highest High (20 days)", "Breakout",
               _hh20_pass,
               f"Close={cp:.2f} / HH20={hh20:.2f}",
               f"Close must exceed prior 20-day high{_hh20_note}")
        else:
            _c("hh20_breakout", "Close > Highest High (20 days)", "Breakout", False,
               "N/A", "Close > HH20", "Insufficient data")
            hh20 = cp

        # ── Breakout: Candle upper proximity ───────────────────────────────
        candle_range = ch - cl
        in_upper = candle_range <= 0 or cp >= ch - PRICE_PROXIMITY_MAX * candle_range
        _prox_note = "" if REQUIRE_PRICE_PROXIMITY else " (filter disabled for swing trades)"
        _c("candle_proximity", f"Close ≥ High − {int(PRICE_PROXIMITY_MAX*100)}%×(H-L)", "Breakout",
           in_upper if REQUIRE_PRICE_PROXIMITY else True,
           f"Close={cp:.2f} / Min={ch - PRICE_PROXIMITY_MAX*candle_range:.2f}",
           f"Close in upper {int((1-PRICE_PROXIMITY_MAX)*100)}% of candle range{_prox_note}")

        # ── Not over-extended ───────────────────────────────────────────────
        atr14_v = atr14 if not pd.isna(atr14) else 0.0
        ema_ceil = e20 + EMA_ATR_MULTIPLIER * atr14_v
        _ceil_note = "" if REQUIRE_EMA_ATR_CEILING else " (filter disabled)"
        _c("not_overextended", f"Close ≤ 20EMA + {EMA_ATR_MULTIPLIER}×ATR14", "Breakout",
           (cp <= ema_ceil) if REQUIRE_EMA_ATR_CEILING else True,
           f"Close={cp:.2f} / Ceil={ema_ceil:.2f}",
           f"Close must be ≤ 20EMA + {EMA_ATR_MULTIPLIER}×ATR14{_ceil_note}")

        # ── Closing range ───────────────────────────────────────────────────
        cr = (cp - cl) / candle_range if candle_range > 0 else 1.0
        _cr_note = "" if REQUIRE_CLOSING_RANGE else " (filter disabled — covered by candle proximity)"
        _c("closing_range", "Closing Range ≥ 50%", "Breakout",
           (cr >= CLOSING_RANGE_MIN) if REQUIRE_CLOSING_RANGE else True,
           f"{cr*100:.1f}%",
           f"≥ {CLOSING_RANGE_MIN*100:.0f}%{_cr_note}")

        # ── RSI(14) ──────────────────────────────────────────────────────────
        rsi_s   = self._rsi(c, RSI_PERIOD)
        rsi_val = float(rsi_s.iloc[-1])
        _c("rsi14", f"RSI14 > {RSI_MIN}", "Momentum",
           rsi_val >= RSI_MIN,
           f"{rsi_val:.1f}",
           f"≥ {RSI_MIN}")

        rsi_sma3 = rsi_s.rolling(3).mean()
        rsi_rising = (not rsi_sma3.dropna().empty and
                      float(rsi_sma3.iloc[-1]) > float(rsi_sma3.iloc[-2]))
        _rsi_sma3_note = "" if REQUIRE_RSI_SMA3_RISING else " (filter disabled)"
        _c("rsi_sma3_rising", "RSI SMA(3) Rising", "Momentum",
           rsi_rising if REQUIRE_RSI_SMA3_RISING else True,
           f"SMA3={rsi_sma3.iloc[-1]:.2f} (prev={rsi_sma3.iloc[-2]:.2f})" if not rsi_sma3.dropna().empty else "N/A",
           f"RSI 3-period SMA must be rising{_rsi_sma3_note}")

        # ── Volume Z-Score ──────────────────────────────────────────────────
        vol_zscore = 0.0
        if len(v) >= VOLUME_AVG_DAYS + VOLUME_LOOKBACK_DAYS + 1:
            vol_win  = v.iloc[-(VOLUME_AVG_DAYS + VOLUME_LOOKBACK_DAYS):-VOLUME_LOOKBACK_DAYS]
            vol_mean = float(vol_win.mean())
            vol_std  = float(vol_win.std())
            vol_zscore = (cur_vol_avg - vol_mean) / vol_std if vol_std > 0 else 0.0
            _c("vol_zscore", f"Volume Z-Score > {VOLUME_ZSCORE_MIN} ({VOLUME_LOOKBACK_DAYS}d avg)", "Volume",
               vol_zscore > VOLUME_ZSCORE_MIN,
               f"Z={vol_zscore:.2f}",
               f"> {VOLUME_ZSCORE_MIN}")
        else:
            _c("vol_zscore", f"Volume Z-Score > {VOLUME_ZSCORE_MIN} ({VOLUME_LOOKBACK_DAYS}d avg)", "Volume", False,
               "N/A", f"> {VOLUME_ZSCORE_MIN}", "Insufficient data")

        # ── Median TV(5D) > Median TV(20D) ──────────────────────────────────
        med_tv_5d = float(tv.iloc[-5:].median())
        _tv_trend_note = "" if REQUIRE_MEDIAN_TV_TREND else " (filter disabled — volume expansion already checked by Z-Score + Rel-Vol-Pct)"
        _c("med_tv5_vs_20", "Median TV(5D) > Median TV(20D)", "Volume",
           (med_tv_5d > med_tv_20d) if REQUIRE_MEDIAN_TV_TREND else True,
           f"5D=₹{med_tv_5d/1e7:.2f}Cr / 20D=₹{med_tv_20d/1e7:.2f}Cr",
           f"5-day median must exceed 20-day median{_tv_trend_note}")

        # ── RS Ratio SMA10 > SMA20 ──────────────────────────────────────────
        if bench_df is not None:
            bench_c  = bench_df["Close"].reindex(c.index, method="ffill").dropna()
            if len(bench_c) >= 25:
                rs_line  = c.reindex(bench_c.index) / bench_c
                rs_sma10 = rs_line.rolling(10).mean()
                rs_sma20 = rs_line.rolling(20).mean()
                rs10_v   = float(rs_sma10.dropna().iloc[-1]) if not rs_sma10.dropna().empty else float("nan")
                rs20_v   = float(rs_sma20.dropna().iloc[-1]) if not rs_sma20.dropna().empty else float("nan")
                rs_ratio_now = float(rs_line.dropna().iloc[-1]) if not rs_line.dropna().empty else float("nan")
                if REQUIRE_RS_UPTREND:
                    _rs_pass = not (pd.isna(rs10_v) or pd.isna(rs20_v)) and rs10_v > rs20_v
                    _rs_note = ""
                else:
                    # Filter disabled — relative strength captured by momentum outperform check
                    _rs_pass = True
                    _rs_note = " (disabled — momentum filter checks relative performance)"
                _c("rs_sma_trend", "RS Ratio SMA(10) > SMA(20)", "RS",
                   _rs_pass,
                   f"SMA10={rs10_v:.5f} / SMA20={rs20_v:.5f}" if not (pd.isna(rs10_v) or pd.isna(rs20_v)) else "N/A",
                   f"RS uptrend{_rs_note}")
            else:
                _c("rs_sma_trend", "RS Ratio SMA(10) > SMA(20)", "RS", False,
                   "N/A", "RS SMA10 > SMA20", "Insufficient benchmark data")
        else:
            _c("rs_sma_trend", "RS Ratio SMA(10) > SMA(20)", "RS", False,
               "N/A", "RS SMA10 > SMA20", "Benchmark unavailable")

        # ── Gap-up ──────────────────────────────────────────────────────────
        if len(c) >= 2:
            prev_close = float(c.iloc[-2])
            gap = (co - prev_close) / prev_close if prev_close > 0 else 0.0
            _c("gap_up", f"Gap Up ≤ {int(GAP_UP_MAX*100)}%", "Entry",
               gap <= GAP_UP_MAX,
               f"{gap*100:.2f}%",
               f"≤ {GAP_UP_MAX*100:.0f}%")
        else:
            _c("gap_up", "Gap Up ≤ 4%", "Entry", False, "N/A", "≤ 4%")
            gap = 0.0

        # ── ADX ─────────────────────────────────────────────────────────────
        try:
            with _suppress_ta_stdout():
                adx_res = _ta_adx(df, length=ADX_PERIOD)
            adx_col = "ADX_%d" % ADX_PERIOD
            pdi_col = "DMP_%d" % ADX_PERIOD
            ndi_col = "DMN_%d" % ADX_PERIOD
            if adx_res is not None and not adx_res.empty and \
                    all(c_ in adx_res.columns for c_ in (adx_col, pdi_col, ndi_col)):
                adx_val = float(adx_res[adx_col].iloc[-1])
                pdi_val = float(adx_res[pdi_col].iloc[-1])
                ndi_val = float(adx_res[ndi_col].iloc[-1])
                _adx_note = "" if REQUIRE_ADX_THRESHOLD else " (threshold disabled — only +DI>-DI required)"
                _c("adx_strength", f"ADX > {ADX_MIN}", "Trend",
                   (not pd.isna(adx_val) and adx_val > ADX_MIN) if REQUIRE_ADX_THRESHOLD else True,
                   f"ADX={adx_val:.1f}",
                   f"> {ADX_MIN}{_adx_note}")
                _c("adx_di", "+DI > −DI", "Trend",
                   not (pd.isna(pdi_val) or pd.isna(ndi_val)) and pdi_val > ndi_val,
                   f"+DI={pdi_val:.1f} / −DI={ndi_val:.1f}",
                   "+DI must be above −DI")
            else:
                _c("adx_strength", f"ADX > {ADX_MIN}", "Trend", False, "N/A", f"> {ADX_MIN}", "ADX calc failed")
                _c("adx_di", "+DI > −DI", "Trend", False, "N/A", "+DI > −DI", "ADX calc failed")
                adx_val = pdi_val = ndi_val = 0.0
        except Exception:
            _c("adx_strength", f"ADX > {ADX_MIN}", "Trend", False, "N/A", f"> {ADX_MIN}", "ADX error")
            _c("adx_di", "+DI > −DI", "Trend", False, "N/A", "+DI > −DI", "ADX error")
            adx_val = pdi_val = ndi_val = 0.0

        # ── RS momentum (20D outperformance) ────────────────────────────────
        if bench_df is not None and len(c) > SECTOR_LOOKBACK_DAYS:
            bench_c2 = bench_df["Close"].reindex(c.index, method="ffill").dropna()
            if len(bench_c2) > SECTOR_LOOKBACK_DAYS:
                stock_ret  = float(c.iloc[-1] / c.iloc[-(SECTOR_LOOKBACK_DAYS + 1)] - 1)
                index_ret  = float(bench_c2.iloc[-1] / bench_c2.iloc[-(SECTOR_LOOKBACK_DAYS + 1)] - 1)
                rs_ratio   = (1 + stock_ret) / (1 + index_ret)
                rs_outperf = stock_ret - index_ret
                _c("rs_ratio", "RS Ratio > 1.03 (stock−index > 3%)", "RS",
                   rs_outperf > MOMENTUM_OUTPERFORM_MIN,
                   f"RS={rs_ratio:.4f} / Outperf={rs_outperf*100:+.2f}%",
                   f"stock_ret − index_ret > {MOMENTUM_OUTPERFORM_MIN*100:.0f}%")
            else:
                stock_ret = index_ret = rs_ratio = rs_outperf = 0.0
                _c("rs_ratio", "RS Ratio > 1.03", "RS", False, "N/A", "> 1.03", "Insufficient data")
        else:
            stock_ret = index_ret = rs_ratio = rs_outperf = 0.0
            _c("rs_ratio", "RS Ratio > 1.03", "RS", False, "N/A", "> 1.03", "Benchmark unavailable")

        # ── Fundamentals ────────────────────────────────────────────────────
        # Multi-source: NSE live → Screener.in → Apify → Alpha Vantage → Yahoo
        try:
            sector, debt_equity, market_cap = _fund_client.get(
                ticker,
                yf_fundamentals_fn=self._yf.fundamentals,
            )
        except Exception:
            sector = debt_equity = market_cap = None

        # Enrich with extra fields (ROE, PE, promoter holding, etc.)
        extra_fund: dict = {}
        try:
            extra_fund = _fund_client.get_extra_fundamentals(ticker)
        except Exception:
            pass

        mc = float(market_cap) if market_cap is not None else None
        _fund_note = "" if REQUIRE_FUNDAMENTALS else " (gate disabled for swing — shown for info only)"
        _c("market_cap", f"Market Cap > ₹{round(MARKET_CAP_MIN/1e7)}Cr", "Fundamental",
           (mc is not None and not pd.isna(mc) and mc >= MARKET_CAP_MIN) if REQUIRE_FUNDAMENTALS else True,
           f"₹{mc/1e7:.0f}Cr" if mc else "N/A",
           f"≥ ₹{round(MARKET_CAP_MIN/1e7)}Cr{_fund_note}")

        de = float(debt_equity) if debt_equity is not None else None
        _c("debt_equity", f"D/E < {DEBT_EQUITY_MAX/100:.1f}", "Fundamental",
           (de is None or pd.isna(de) or de < DEBT_EQUITY_MAX) if REQUIRE_FUNDAMENTALS else True,
           f"{de/100:.2f}" if de and not pd.isna(de) else "N/A",
           f"< {DEBT_EQUITY_MAX/100:.1f}{_fund_note}")

        # ── Sector outperformance ───────────────────────────────────────────
        sec_ret = sector_returns.get(sector) if sector else None
        if sec_ret is None and SECTOR_FALLBACK_TO_MARKET:
            sec_ret = market_ret20d
        r20d       = stock_ret * 100 if bench_df is not None else 0.0
        sec_outperf = r20d - sec_ret if sec_ret is not None else None
        _c("sector_outperf", "20D Return > Sector Avg + 2%", "Momentum",
           sec_outperf is not None and sec_outperf >= SECTOR_OUTPERFORM_MIN,
           f"Stock={r20d:+.2f}% vs Sector={sec_ret:.2f}%" if sec_ret is not None else "N/A",
           f"Must outperform sector by ≥ {SECTOR_OUTPERFORM_MIN}%",
           f"Δ={sec_outperf:+.2f}%" if sec_outperf is not None else "")

        # ── Stop loss ───────────────────────────────────────────────────────
        # Same optimised formula as _analyze(): structure-aware + bounded ATR
        if atr14_v > 0:
            _sl_recent_low = float(lo.iloc[-4:-1].min()) if len(lo) >= 4 else cl
            _sl_structural = max(cl, _sl_recent_low)
            _sl_candidate  = max(_sl_structural, cp - 1.0 * atr14_v)
            _sl_lo         = cp - 1.5 * atr14_v
            _sl_hi         = cp - 0.5 * atr14_v
            stop_loss = round(min(max(_sl_candidate, _sl_lo), _sl_hi), 2)
        else:
            stop_loss = round(cl, 2)
        sl_risk   = round((cp - stop_loss) / cp * 100, 2) if cp > 0 else 0.0

        # ── Summary ─────────────────────────────────────────────────────────
        total  = len(criteria)
        passed = sum(1 for c_ in criteria if c_["passed"])
        failed = total - passed

        return {
            "ticker":         ticker,
            "display_ticker": sym,
            "price":          round(cp, 2),
            "stop_loss":      stop_loss,
            "sl_risk_pct":    sl_risk,
            "atr14":          round(atr14_v, 2),
            "sector":         sector or None,
            "market_cap_cr":  round(mc / 1e7, 0) if mc else None,
            "debt_equity":    round(de / 100, 2) if de and not pd.isna(de) else None,
            "rs_ratio":       round(rs_ratio, 4) if rs_ratio else None,
            "return_20d":     round(r20d, 2),
            # ── Extra fundamentals from Screener.in ────────────────────────
            "roe":               extra_fund.get("roe"),
            "current_ratio":     extra_fund.get("current_ratio"),
            "promoter_holding":  extra_fund.get("promoter_holding"),
            "pe_ratio":          extra_fund.get("pe_ratio"),
            "eps":               extra_fund.get("eps"),
            "sales_growth_pct":  extra_fund.get("sales_growth_pct"),
            "dividend_yield":    extra_fund.get("dividend_yield"),
            "52w_high":          extra_fund.get("52w_high"),
            "52w_low":           extra_fund.get("52w_low"),
            "profit_margin_pct": extra_fund.get("profit_margin_pct"),
            # ────────────────────────────────────────────────────────────────
            "criteria":       criteria,
            "summary":        {"total": total, "passed": passed, "failed": failed},
        }

    # -- Main scan pipeline --------------------------------------------------

    def scan(self, target_date=None, progress_cb=None, momentum_only=False):
        """Run a full scan.
        target_date (datetime.date | None): historical mode; live when None.
        progress_cb (callable | None)     : called with a stage string.
        momentum_only (bool)              : when True, apply ONLY the 6 momentum
            criteria (_analyze_momentum) and return those results immediately.
            Skips all Swing Trade filters (_analyze), fundamentals, sector
            outperformance, and composite scoring.  The OHLCV cache is still
            read/written so the momentum scan and swing scan share cached data.
        """
        def _progress(msg):
            if progress_cb:
                progress_cb(msg)

        mode = "historical(%s)" % target_date if target_date else "live"
        lbl = "[%s]" % self.label
        scan_kind = "momentum-only" if momentum_only else "full"
        logger.info("%s === Scan start [%s, %s]: %d tickers ===",
                    lbl, mode, scan_kind, len(self.tickers))

        # Reset regime state
        self.last_regime_ok      = True
        self.last_regime_summary = ""

        try:
            self._yf.init_crumb()
        except Exception as exc:
            logger.warning("Crumb pre-init failed: %s", exc)

        # Step 1 -- Market regime + sector data
        _progress("Checking Nifty500 regime (EMA + RSI)...")
        regime_ok, bench_df, market_ret20d, sector_returns = \
            self._fetch_market_data(target_date=target_date)

        if not regime_ok:
            if REGIME_ABORT_ON_FAIL:
                logger.warning("%s regime FAILED -- scan aborted (REGIME_ABORT_ON_FAIL=True).",
                               self.label)
                _progress("Aborted: %s not in uptrend (EMA or RSI regime)" % self.label)
                self.last_regime_ok      = False
                self.last_regime_summary = "Market regime check failed. Scan aborted."
                return []
            else:
                logger.warning(
                    "%s regime FAILED -- continuing with caution "
                    "(REGIME_ABORT_ON_FAIL=False, soft-warn mode).",
                    self.label,
                )
                _progress("CAUTION: %s regime weak -- scan running with warning" % self.label)

        # Persist regime status so main.py can expose it via scan_state
        self.last_regime_ok = regime_ok
        if regime_ok:
            self.last_regime_summary = (
                "OK: 20EMA > 50EMA | RSI(14) >= %d/%d days above 50"
                % (REGIME_RSI_REQUIRE_DAYS, 3)
            )
        else:
            self.last_regime_summary = (
                "CAUTION: %s regime weak — "
                "RSI(14) below 50 on recent sessions. "
                "Use reduced position sizes." % self.label
            )

        # Step 1b -- Pre-load BSE Bhavcopy for volume/price validation
        _progress("Pre-loading BSE Bhavcopy for latest-day data validation...")
        bse_ref_date = self._bse.load_recent(
            target_date=target_date if target_date else None
        )
        if bse_ref_date:
            logger.info("BSE/NSE EOD data loaded for %s (volume patch active)", bse_ref_date)
        else:
            logger.info("BSE/NSE EOD data unavailable - Yahoo Finance data used as-is")

        # Step 2 -- OHLCV download (cache-first strategy)
        # ─────────────────────────────────────────────────────────────────────
        # Live mode :  classify each ticker as fresh / stale / missing.
        #              • Fresh  (≤ STALE_DAYS old) → loaded from disk instantly
        #              • Stale  (file exists but outdated) → 70-day incremental download
        #              • Missing (no cache file) → full HIST_DAYS download
        # Historical:  always download fresh; cache is never read or written.
        # ─────────────────────────────────────────────────────────────────────
        _progress("Loading OHLCV data (%d tickers)..." % len(self.tickers))
        all_data: dict = {}
        bo_needed: list = []

        ns_tickers        = self.tickers
        _ns_to_download   = []   # no cache → full HIST_DAYS download
        _ns_to_update     = []   # stale cache → 70-day incremental download

        if not target_date:
            # Live mode: check cache for each ticker
            for t in ns_tickers:
                cached = _cache.load(t)
                if _cache.is_fresh(cached):
                    all_data[t] = cached          # instant — no network needed
                elif cached is not None:
                    _ns_to_update.append(t)       # have data, just stale
                else:
                    _ns_to_download.append(t)     # nothing cached yet

            cs = _cache.stats()
            logger.info(
                "%s Cache status: %d tickers served from disk | "
                "%d stale (incremental update) | %d missing (full download) | "
                "cache size: %.1f MB",
                lbl, len(all_data), len(_ns_to_update), len(_ns_to_download),
                cs["size_mb"],
            )
        else:
            # Historical mode: always download everything
            _ns_to_download = list(ns_tickers)

        # ── 2a. Full download for missing tickers ─────────────────────────────
        if _ns_to_download:
            _progress("Downloading full history for %d new tickers..." % len(_ns_to_download))
            total_dl = len(_ns_to_download)
            for batch_start in range(0, total_dl, DOWNLOAD_BATCH_SIZE):
                batch = _ns_to_download[batch_start: batch_start + DOWNLOAD_BATCH_SIZE]
                batch_result = self._download_batch_ns(batch, HIST_DAYS, end_date=target_date)
                batch_good = 0
                for t in batch:
                    df = batch_result.get(t)
                    if self._is_quality_ok(df):
                        all_data[t] = df
                        if not target_date:
                            _cache.save(t, df)    # persist to disk
                        batch_good += 1
                    else:
                        bo_needed.append(t)
                processed = min(batch_start + DOWNLOAD_BATCH_SIZE, total_dl)
                logger.info("%s  NS full: %d/%d downloaded (%d this batch passed, %d total good, %d need fallback)",
                            lbl, processed, total_dl, batch_good, len(all_data), len(bo_needed))
                _progress("Downloading: %d/%d tickers... (%d good, %d need fallback)"
                          % (processed, total_dl, len(all_data), len(bo_needed)))

        # ── 2a-upd. Incremental update for stale tickers ──────────────────────
        if _ns_to_update:
            _progress("Updating %d stale tickers (last %d days)..." % (len(_ns_to_update), CACHE_UPDATE_DAYS))
            total_upd = len(_ns_to_update)
            for batch_start in range(0, total_upd, DOWNLOAD_BATCH_SIZE):
                batch = _ns_to_update[batch_start: batch_start + DOWNLOAD_BATCH_SIZE]
                batch_result = self._download_batch_ns(batch, CACHE_UPDATE_DAYS, end_date=target_date)
                for t in batch:
                    df_new  = batch_result.get(t)
                    cached  = _cache.load(t)
                    if df_new is not None and not df_new.empty:
                        merged = _cache.merge(cached, df_new, max_rows=HIST_DAYS + 150)
                        if self._is_quality_ok(merged):
                            all_data[t] = merged
                            _cache.save(t, merged)
                            continue
                    # Incremental update failed — use stale cache if usable
                    if self._is_quality_ok(cached):
                        all_data[t] = cached
                        logger.debug("%s  Using stale cache for %s (incremental update failed)", lbl, t)
                    else:
                        bo_needed.append(t)
                logger.info("%s  NS update: %d/%d refreshed (%d good so far)",
                            lbl, min(batch_start + DOWNLOAD_BATCH_SIZE, total_upd),
                            total_upd, len(all_data))

        logger.info("%s NSE phase done: %d usable, %d need BSE fallback",
                    lbl, len(all_data), len(bo_needed))

        # ── 2b. BSE (.BO) fallback for tickers with poor NSE data quality ─────
        if bo_needed:
            _progress("BSE fallback for %d tickers..." % len(bo_needed))
            bo_tickers = [t.replace(".NS", ".BO") for t in bo_needed]
            for batch_start in range(0, len(bo_tickers), DOWNLOAD_BATCH_SIZE):
                batch_bo = bo_tickers[batch_start: batch_start + DOWNLOAD_BATCH_SIZE]
                batch_ns = bo_needed[batch_start: batch_start + DOWNLOAD_BATCH_SIZE]
                batch_result = self._download_batch_ns(batch_bo, HIST_DAYS, end_date=target_date)
                for t_bo, t_ns in zip(batch_bo, batch_ns):
                    df = batch_result.get(t_bo)
                    if self._is_quality_ok(df):
                        all_data[t_ns] = df
                        if not target_date:
                            _cache.save(t_ns, df)   # cache the BSE data under NS key
                        logger.debug("Using BSE (.BO) data for %s", t_ns)
                logger.info("%s  BO batch: %d/%d fallback tickers processed",
                            lbl, min(batch_start + DOWNLOAD_BATCH_SIZE, len(bo_tickers)), len(bo_tickers))

        # 2c. TradingView fallback for tickers still missing after BSE batch
        still_missing = [t for t in bo_needed if t not in all_data]
        if still_missing and _tv_client.available:
            _progress("TradingView fallback for %d tickers..." % len(still_missing))
            logger.info("%s TradingView fallback: trying %d quality-failed tickers",
                        lbl, len(still_missing))
            tv_ok = 0
            _tv_total = len(still_missing)
            for _i, t in enumerate(still_missing, 1):
                try:
                    df = _tv_client.get_history(t, HIST_DAYS, end_date=target_date)
                    if self._is_quality_ok(df):
                        all_data[t] = df
                        if not target_date:
                            _cache.save(t, df)
                        tv_ok += 1
                        logger.debug("TradingView OK for %s", t)
                except Exception as exc:
                    logger.debug("TradingView failed for %s: %s", t, exc)
                if _i % 10 == 0 or _i == _tv_total:
                    _progress("TradingView fallback: %d/%d tickers... (%d recovered)"
                              % (_i, _tv_total, tv_ok))
                    logger.info("%s TradingView fallback: %d/%d processed (%d recovered)",
                                lbl, _i, _tv_total, tv_ok)
            if tv_ok:
                logger.info("%s TradingView fallback: recovered %d/%d tickers",
                            lbl, tv_ok, len(still_missing))

        logger.info("%s OHLCV done: %d usable tickers", lbl, len(all_data))

        # ── 2e. Zerodha live quote patch — overwrite today's candle with live data ─
        if not target_date:
            _progress("Fetching live quotes from Zerodha...")
            _apply_zerodha_live_patch(all_data, lbl)

        # Step 2c -- Synthetic benchmark fallback
        # When the ^ index ticker (and all ETF fallbacks) returned no data,
        # build a synthetic Nifty-500-proxy from the equal-weight median of all
        # downloaded stocks.  This ensures market_ret20d and the RS ratio filter
        # always have a valid benchmark even in restrictive proxy environments.
        if bench_df is None and all_data:
            _progress("Building synthetic market benchmark from downloaded stocks...")
            logger.warning(
                "All benchmark index tickers failed.  Computing market_ret20d "
                "from median 20D return of %d downloaded stocks.", len(all_data)
            )
            # Normalised close (base = 1.0) for each stock so all contribute equally
            norm_closes = []
            for df_s in all_data.values():
                c = df_s["Close"].dropna()
                if len(c) > SECTOR_LOOKBACK_DAYS + 1 and float(c.iloc[0]) > 0:
                    norm_closes.append(c / float(c.iloc[0]))
            if norm_closes:
                combined   = pd.concat(norm_closes, axis=1)
                combined   = combined.ffill().bfill()
                bench_close = combined.median(axis=1) * 10_000   # scale to index-like level
                bench_df    = pd.DataFrame({
                    "Open":   bench_close, "High":   bench_close,
                    "Low":    bench_close, "Close":  bench_close,
                    "Volume": pd.Series(0, index=bench_close.index, dtype=float),
                })
                bc = bench_close.dropna()
                if len(bc) > SECTOR_LOOKBACK_DAYS:
                    market_ret20d = float(
                        (bc.iloc[-1] / bc.iloc[-(SECTOR_LOOKBACK_DAYS + 1)] - 1) * 100
                    )
                logger.info(
                    "Synthetic benchmark built from %d stocks -- market_ret20d: %.2f%%",
                    len(norm_closes), market_ret20d
                )

        _progress("Applying technical filters (%d tickers)..." % len(all_data))
        if not all_data:
            self.last_momentum_results = []
            return []

        # Reference date used to anchor last-candle validation inside
        # _analyze_momentum().  We prefer the BSE Bhavcopy date (most accurate
        # signal of what the market settled at), then fall back to target_date
        # (historical mode), and finally None (no validation, allow any latest bar).
        _mom_scan_date = bse_ref_date or target_date   # may be None in live + no BSE

        # Step 3 -- Technical filters (Swing) + Momentum filters in one pass
        # Both filters run on the same downloaded OHLCV data — no second download.
        # pre        : stocks passing all swing-trade filters (→ scan_state["data"])
        # momentum_pre: stocks passing only the 6 momentum criteria (→ state["momentum_data"])
        # When momentum_only=True, _analyze() is skipped entirely — only
        # _analyze_momentum() runs, and we return its results immediately
        # (skipping fundamentals, sector outperformance, and composite scoring).
        pre: list[dict]          = []
        momentum_pre: list[dict] = []
        _all_data_items = list(all_data.items())
        _filter_total   = len(_all_data_items)
        for _fi, (t_key, df) in enumerate(_all_data_items, 1):
            if not momentum_only:
                try:
                    r = self._analyze(t_key, df, bench_df, bse_date=bse_ref_date)
                    if r:
                        pre.append(r)
                except Exception as exc:
                    logger.debug("_analyze(%s) error: %s", t_key, exc)
            # Independent momentum check — not gated by swing result
            try:
                r_mom = self._analyze_momentum(t_key, df, bench_df, scan_date=_mom_scan_date)
                if r_mom:
                    momentum_pre.append(r_mom)
            except Exception as exc:
                logger.debug("_analyze_momentum(%s) error: %s", t_key, exc)
            if _fi % 50 == 0 or _fi == _filter_total:
                if momentum_only:
                    _progress("Momentum filters: %d/%d tickers... (%d passed)"
                              % (_fi, _filter_total, len(momentum_pre)))
                    logger.info("%s Momentum filters: %d/%d tickers processed "
                                "(%d momentum passed so far)",
                                lbl, _fi, _filter_total, len(momentum_pre))
                else:
                    _progress("Technical filters: %d/%d tickers... (%d swing, %d momentum passed)"
                              % (_fi, _filter_total, len(pre), len(momentum_pre)))
                    logger.info("%s Technical filters: %d/%d tickers processed "
                                "(%d swing, %d momentum passed so far)",
                                lbl, _fi, _filter_total, len(pre), len(momentum_pre))

        # Store momentum results on the instance so main.py can read them
        self.last_momentum_results = momentum_pre
        logger.info("%s Momentum-only pass: %d/%d tickers qualify",
                    lbl, len(momentum_pre), len(all_data))

        # When running in momentum-only mode, return immediately — no fundamentals,
        # no sector outperformance filter, no composite scoring.
        if momentum_only:
            logger.info("")
            logger.info("=" * 80)
            logger.info("%s MOMENTUM-ONLY SCAN COMPLETE: %d of %d tickers passed",
                        lbl, len(momentum_pre), len(all_data))
            logger.info("=" * 80)
            logger.info("")
            return momentum_pre

        # ---- Phase 1 summary: Technical Filters --------------------------------
        _SEP = "-" * 115
        logger.info("")
        logger.info("=" * 115)
        logger.info("PHASE 1 - TECHNICAL FILTERS PASSED: %d of %d tickers downloaded",
                    len(pre), len(all_data))
        logger.info("=" * 115)
        if pre:
            logger.info("  %-12s  %8s  %5s  %5s  %5s  %5s  %5s  %5s  %6s  %8s  %9s  %7s  %10s",
                        "Ticker", "Price", "RSI", "wRSI", "ADX", "+DI", "-DI",
                        "VolZ", "RVPct", "EMA20>50", "RS Ratio", "RS Pct", "AvgTV Cr")
            logger.info("  " + _SEP)
            for s in pre:
                logger.info(
                    "  %-12s  %8.2f  %5.1f  %5.1f  %5.1f  %5.1f  %5.1f"
                    "  %5.2f  %5.0f%%  %+7.1f%%  %9.4f  %+6.2f%%  Rs.%-6.1fCr",
                    s["display_ticker"], s["price"],
                    s["rsi"], s["weekly_rsi"],
                    s["adx"], s["pdi"], s["ndi"],
                    s["vol_zscore"], s["rel_vol_pct"],
                    s["ema20_vs_50"], s["rs_ratio"], s["rs_outperf_pct"],
                    s["avg_tv_20d_cr"],
                )
        else:
            logger.info("  (none)")
        logger.info("=" * 115)
        logger.info("")

        _progress("Fetching fundamentals for %d candidates..." % len(pre))
        if not pre:
            return []

        # Step 4 -- Fundamental filters (Market Cap + D/E)
        #           When REQUIRE_FUNDAMENTALS=False (swing default), fundamentals
        #           are still FETCHED for display purposes (sector, MCap, D/E shown in UI)
        #           but do NOT reject stocks.  Price-action signals matter for 1-10 day holds.
        #           Uses multi-source FundamentalsClient:
        #           NSE live → Screener.in → Apify → Alpha Vantage → Yahoo Finance
        #           Parallelised across FUNDAMENTALS_THREADS workers for speed.
        ok: list[dict] = []
        rej_cap, rej_de, rej_nodata = [], [], []

        def _fetch_fund(stock):
            try:
                # Multi-source: NSE live → Screener.in → Apify → Alpha Vantage → Yahoo
                sector, debt_equity, market_cap = _fund_client.get(
                    stock["ticker"],
                    yf_fundamentals_fn=self._yf.fundamentals,   # Yahoo as last resort
                )
                return stock, sector, debt_equity, market_cap, None
            except Exception as exc:
                return stock, None, None, None, exc

        with ThreadPoolExecutor(max_workers=min(FUNDAMENTALS_THREADS, len(pre))) as ex:
            futs = {ex.submit(_fetch_fund, s): s for s in pre}
            for fut in as_completed(futs):
                stock, sector, debt_equity, market_cap, err = fut.result()
                sym = stock["display_ticker"]
                if err:
                    if REQUIRE_FUNDAMENTALS:
                        # Hard-gate mode: can't validate, skip the stock
                        rej_nodata.append(sym)
                        logger.debug("  FUND ERR  %-12s  %s (rejected: REQUIRE_FUNDAMENTALS=True)", sym, err)
                        continue
                    else:
                        # Soft mode: missing data is OK — include with unknown fundamentals
                        logger.debug("  FUND ERR  %-12s  %s (keeping: REQUIRE_FUNDAMENTALS=False)", sym, err)
                        sector = debt_equity = market_cap = None

                mc = float(market_cap) if market_cap is not None else None
                if REQUIRE_FUNDAMENTALS:
                    if mc is None or pd.isna(mc) or mc < MARKET_CAP_MIN:
                        cap_cr = round(mc / 1e7, 0) if mc and not pd.isna(mc) else None
                        rej_cap.append("%s (Rs.%sCr)" % (sym, cap_cr))
                        continue

                de = float(debt_equity) if debt_equity is not None else None
                if REQUIRE_FUNDAMENTALS:
                    if de is not None and not pd.isna(de) and de >= DEBT_EQUITY_MAX:
                        rej_de.append("%s (D/E %.2f)" % (sym, de / 100))
                        continue

                stock["sector"]        = sector or None
                stock["debt_equity"]   = round(de / 100, 2) if de is not None and not pd.isna(de) else None
                stock["market_cap_cr"] = round(mc / 1e7, 0) if mc is not None and not pd.isna(mc) else None
                ok.append(stock)

        # ---- Phase 2 summary: Fundamental Filters ------------------------------
        logger.info("")
        logger.info("=" * 80)
        if REQUIRE_FUNDAMENTALS:
            logger.info("PHASE 2 - FUNDAMENTAL FILTERS PASSED: %d of %d candidates "
                        "(MCap>Rs.%dCr, D/E<%.1f)",
                        len(ok), len(pre),
                        round(MARKET_CAP_MIN / 1e7), DEBT_EQUITY_MAX / 100)
        else:
            logger.info("PHASE 2 - FUNDAMENTAL FILTERS DISABLED (REQUIRE_FUNDAMENTALS=False): "
                        "all %d candidates pass", len(ok))
        logger.info("=" * 80)
        if ok:
            logger.info("  %-12s  %12s  %6s  %s",
                        "Ticker", "MCap", "D/E", "Sector")
            logger.info("  " + "-" * 76)
            for s in ok:
                de_str  = "%.2f" % s["debt_equity"] if s["debt_equity"] is not None else "N/A"
                logger.info("  %-12s  Rs.%-8.0fCr  %6s  %s",
                            s["display_ticker"], s["market_cap_cr"],
                            de_str, s["sector"])
        if rej_cap:
            logger.info("")
            logger.info("  REJECTED - MCap < Rs.1200Cr  [%d]: %s",
                        len(rej_cap), ", ".join(rej_cap))
        if rej_de:
            logger.info("  REJECTED - D/E >= 2.5        [%d]: %s",
                        len(rej_de), ", ".join(rej_de))
        if rej_nodata:
            logger.info("  SKIPPED  - no fund data      [%d]: %s",
                        len(rej_nodata), ", ".join(rej_nodata))
        logger.info("=" * 80)
        logger.info("")

        _progress("Applying sector-momentum filter to %d candidates..." % len(ok))
        if not ok:
            return []

        # Step 5 -- Sector outperformance (20D return > sector avg + 2%)
        final: list[dict] = []
        mom_rej: list[str] = []

        for s in ok:
            sec     = s["sector"]
            sec_ret = sector_returns.get(sec)
            if sec_ret is None and SECTOR_FALLBACK_TO_MARKET:
                sec_ret = market_ret20d

            sec_outperf = s["return_20d"] - sec_ret if sec_ret is not None else None
            if sec_outperf is not None and sec_outperf < SECTOR_OUTPERFORM_MIN:
                mom_rej.append("%s (%.1f%% vs sector)" % (s["display_ticker"], sec_outperf))
                continue

            s["market_return"]       = round(market_ret20d, 2)
            s["sector_index_return"] = round(sec_ret, 2) if sec_ret is not None else None
            s["stock_vs_sector"]     = round(sec_outperf, 2) if sec_outperf is not None else None
            s["stock_vs_market"]     = round(s["return_20d"] - market_ret20d, 2)
            final.append(s)

        if mom_rej:
            logger.info("  REJECTED - 20D return < sector avg +2%%  [%d]: %s",
                        len(mom_rej), ", ".join(mom_rej))
        logger.info("Sector-momentum filter: %d of %d passed", len(final), len(ok))
        if not final:
            return []

        # Step 6 -- Composite score
        for s in final:
            vol_score  = min(max(s["vol_zscore"], 0.0) / VOLUME_SCORE_CAP, 1.0)
            rsi_score  = max(0.0, (s["rsi"] - RSI_MIN) / max(100 - RSI_MIN, 1))
            # EMA: penalty grows as price moves above 20 EMA
            ema_score  = max(0.0, 1.0 - s["price_vs_ema20"] / EMA_PCT_SCORE_CAP)
            adx_score  = min(s["adx"] / ADX_SCORE_CAP, 1.0)
            mom_score  = min(max(s["stock_vs_market"], 0.0) / MOMENTUM_SCORE_CAP, 1.0)
            rs_score   = min(max(s["rs_ratio"] - 1.0, 0.0) / RS_SCORE_CAP, 1.0)

            s["score"] = round(
                vol_score  * WEIGHT_VOLUME
                + rsi_score * WEIGHT_RSI
                + ema_score  * WEIGHT_EMA
                + adx_score  * WEIGHT_ADX
                + mom_score  * WEIGHT_MOMENTUM
                + rs_score   * WEIGHT_RS,
                4,
            )

        final.sort(key=lambda x: x["score"], reverse=True)
        top = final[:TOP_N]
        for i, s in enumerate(top, 1):
            s["rank"] = i

        logger.info("")
        logger.info("=" * 115)
        logger.info("FINAL RESULTS - TOP %d RANKED BY COMPOSITE SCORE  (%d qualified)",
                    len(top), len(final))
        logger.info("=" * 115)
        logger.info("  %-3s  %-12s  %8s  %8s  %5s  %5s  %8s  %7s  %6s  %7s  %8s  %s",
                    "#", "Ticker", "Price", "StopLoss", "Score", "RSI",
                    "20D Ret", "vs Sec", "ADX", "RS Out", "MCap Cr", "Sector")
        logger.info("  " + "-" * 111)
        for s in top:
            logger.info(
                "  %-3d  %-12s  %8.2f  %8.2f  %5.3f  %5.1f"
                "  %+7.1f%%  %+6.1f%%  %6.1f  %+6.2f%%  Rs.%-5.0fCr  %s",
                s["rank"], s["display_ticker"],
                s["price"], s["stop_loss"], s["score"], s["rsi"],
                s["return_20d"], s.get("stock_vs_sector") or 0.0,
                s["adx"], s["rs_outperf_pct"],
                s.get("market_cap_cr", 0), s.get("sector", ""),
            )
        logger.info("=" * 115)
        logger.info("")
        return top

    # -- Morning Star pattern scan (all tickers, cache-first) --------------------

    def scan_morning_star(
        self,
        target_date: "dt_mod.date | None" = None,
        progress_cb=None,
    ) -> "list[dict]":
        """
        Scan ALL tickers for the Morning Star 3-candle bullish-reversal pattern.

        Key differences from scan() / _analyze_momentum():
          • No regime check, no sector data, no benchmark required.
          • Checks ONLY _is_morning_star() — no momentum/swing criteria.
          • Cache-first strategy for BOTH live and historical dates:
              - Cached data that already covers target_date → slice, no download.
              - Cached data that is older than target_date  → incremental delta dl.
              - No cache at all                             → full HIST_DAYS dl.
            This means changing the date filter in the UI is nearly instant once
            the OHLCV cache has been populated by any previous scan.
          • Returns a lightweight dict per match (price, RSI, 20D return, etc.).
        """
        def _progress(msg):
            if progress_cb:
                progress_cb(msg)

        mode = "historical(%s)" % target_date if target_date else "live"
        lbl  = "[%s MorningStar]" % self.label
        logger.info("%s === Scan start [%s]: %d tickers ===", lbl, mode, len(self.tickers))

        # ── Step 1: OHLCV loading (cache-first, delta download for misses) ────────
        _progress("Loading OHLCV data (%d tickers, cache-first)..." % len(self.tickers))
        all_data: dict        = {}
        delta_update: list    = []   # (ticker, days_needed)  — partial cache refresh
        full_download: list   = []   # tickers with no cache at all

        for t in self.tickers:
            cached = _cache.load(t)
            if cached is not None and not cached.empty:
                try:
                    last_cached = (
                        cached.index[-1].date()
                        if hasattr(cached.index[-1], "date")
                        else cached.index[-1]
                    )
                except Exception:
                    last_cached = None

                if target_date is None:
                    # Live mode: check freshness exactly like normal scan
                    if _cache.is_fresh(cached):
                        all_data[t] = cached
                    else:
                        delta_update.append(t)
                else:
                    if last_cached is not None and last_cached >= target_date:
                        # Cache already covers target_date — slice later, no download
                        all_data[t] = cached
                    else:
                        # Cache exists but doesn't reach target_date — download delta
                        delta_update.append(t)
            else:
                full_download.append(t)

        logger.info(
            "%s Cache: %d from disk | %d delta fetch | %d full download",
            lbl, len(all_data), len(delta_update), len(full_download),
        )

        # ── 1a. Delta updates (stale/partial cache) ───────────────────────────────
        if delta_update:
            _progress("Updating %d stale / short tickers..." % len(delta_update))
            for i in range(0, len(delta_update), DOWNLOAD_BATCH_SIZE):
                batch = delta_update[i: i + DOWNLOAD_BATCH_SIZE]
                res   = self._download_batch_ns(
                    batch, CACHE_UPDATE_DAYS, end_date=target_date
                )
                for t in batch:
                    df_new = res.get(t)
                    cached  = _cache.load(t)
                    if df_new is not None and not df_new.empty:
                        merged = _cache.merge(cached, df_new) if cached is not None else df_new
                        all_data[t] = merged
                        if target_date is None:          # only persist live-mode updates
                            _cache.save(t, merged)
                    elif cached is not None:
                        all_data[t] = cached             # use stale cache as fallback

        # ── 1b. Full downloads (no cache at all) ──────────────────────────────────
        if full_download:
            _progress("Full download for %d new tickers..." % len(full_download))
            for i in range(0, len(full_download), DOWNLOAD_BATCH_SIZE):
                batch = full_download[i: i + DOWNLOAD_BATCH_SIZE]
                res   = self._download_batch_ns(
                    batch, HIST_DAYS, end_date=target_date
                )
                for t in batch:
                    df = res.get(t)
                    if df is not None and not df.empty and self._is_quality_ok(df):
                        all_data[t] = df
                        if target_date is None:
                            _cache.save(t, df)

        logger.info("%s OHLCV done: %d usable tickers", lbl, len(all_data))

        # Live candle patch — overwrite today's candle with Zerodha /quote data
        if target_date is None:
            _apply_zerodha_live_patch(all_data, lbl)

        if not all_data:
            return []

        # ── Step 1b: Load benchmark close series for RS calculation (cache-only) ─
        bench_c_ms = None
        for _bt in ([self.benchmark_ticker] + list(self.benchmark_etf_fallbacks or [])):
            _bdf = _cache.load(_bt)
            if _bdf is not None and not _bdf.empty:
                _bc = _bdf["Close"].dropna()
                if len(_bc) > SECTOR_LOOKBACK_DAYS:
                    bench_c_ms = _bc
                    break

        # ── Step 1c: Synthetic benchmark fallback ────────────────────────────
        # When no benchmark index PKL file exists in cache/ohlcv (e.g. fresh
        # deployment or after a full cache clear), build an equal-weight
        # synthetic market proxy from the median-normalised close of all
        # downloaded stocks.  This ensures the "RS vs Idx" column is always
        # populated rather than showing empty values.
        if bench_c_ms is None and all_data:
            _progress("Building synthetic benchmark for RS (no index cache)...")
            _norm_closes = []
            for _df_s in all_data.values():
                _c_s = _df_s["Close"].dropna()
                if len(_c_s) > SECTOR_LOOKBACK_DAYS + 1 and float(_c_s.iloc[0]) > 0:
                    _norm_closes.append(_c_s / float(_c_s.iloc[0]))
            if _norm_closes:
                _combined_s  = pd.concat(_norm_closes, axis=1).ffill().bfill()
                bench_c_ms   = _combined_s.median(axis=1).dropna()
                logger.info(
                    "%s Synthetic benchmark built from %d stocks for RS calculation",
                    lbl, len(_norm_closes),
                )

        # ── Step 2: Apply Morning Star + quality filters ────────────────────
        scan_date = target_date or _ist_today()   # IST date — avoids UTC vs IST discrepancy
        _progress("Checking Morning Star pattern (%d tickers)..." % len(all_data))

        results:   list = []
        _total    = len(all_data)
        for _fi, (t_key, df) in enumerate(all_data.items(), 1):
            try:
                # Slice to scan_date (critical for historical accuracy)
                df_s = df[df.index.normalize() <= pd.Timestamp(scan_date)]
                if df_s.empty:
                    continue

                c  = df_s["Close"].dropna()
                if len(c) < 20:       # need enough bars for indicators
                    continue

                # Freshness: reject if last candle is stale (>5 calendar days)
                candle_date = c.index[-1].date()
                if (scan_date - candle_date).days > 5:
                    continue

                cp  = float(c.iloc[-1])
                if cp <= 0:
                    continue
                sym = t_key.replace(".NS", "")
                v   = df_s["Volume"].reindex(c.index).fillna(0)
                tv  = c * v

                # ── Q1. Liquidity gate ───────────────────────────────────────
                avg_tv_20d = float(tv.iloc[-VOLUME_AVG_DAYS:].mean())
                if avg_tv_20d < MS_TV_MIN_CR * 1e7:
                    continue

                # ── Q2. Prior downtrend: trough must fall ≥ MS_PRIOR_DROP_PCT%
                # below the close ~10 bars before the pattern
                pre_close   = 0.0
                decline_pct = 0.0
                if len(c) >= 14:
                    pre_close  = float(c.iloc[-12])
                    trough_cls = float(c.iloc[-7:-1].min())
                    if pre_close > 0:
                        decline_pct = (pre_close - trough_cls) / pre_close * 100
                        if decline_pct < MS_PRIOR_DROP_PCT:
                            continue

                # ── Q3. Morning Star candlestick check (strict) ─────────────
                ms_info = self._is_morning_star(df_s)
                if ms_info is None:
                    continue

                # ── Q4. Volume on reversal >= MS_VOL_CONFIRM_X × 20D avg ────
                avg_vol    = 0.0
                recent_vol = 0.0
                if len(v) >= VOLUME_AVG_DAYS + 4:
                    avg_vol    = float(v.iloc[-(VOLUME_AVG_DAYS + 3):-3].mean())
                    recent_vol = float(v.iloc[-3:].mean())
                    if avg_vol > 0 and recent_vol < avg_vol * MS_VOL_CONFIRM_X:
                        continue

                # ── RSI (compute for display + scoring only, no gate) ────────
                rsi_val = None
                if len(c) >= RSI_PERIOD + 1:
                    rsi_s   = self._rsi(c, RSI_PERIOD)
                    rsi_val = round(float(rsi_s.iloc[-1]), 2)

                # ── EMA-50 (compute for display + scoring only, no gate) ─────
                price_vs_ema50 = None
                ema50          = None
                if len(c) >= EMA_PERIOD:
                    _ema50 = float(
                        c.ewm(span=EMA_PERIOD, adjust=False,
                              min_periods=EMA_PERIOD).mean().iloc[-1]
                    )
                    if _ema50 > 0:
                        ema50          = _ema50
                        price_vs_ema50 = (_ema50 and (cp / _ema50 - 1) * 100)

                # ── Stop loss (ATR14-based structural stop) ──────────────────
                lo_ms = df_s["Low"].reindex(c.index)
                cl_ms = float(lo_ms.iloc[-1])
                atr14_ms_val = None
                stop_loss_ms = round(cp * 0.95, 2)  # 5% fallback
                try:
                    _atr14_s = _ta_atr(df_s, length=14)
                    if _atr14_s is not None and not _atr14_s.dropna().empty:
                        _av = float(_atr14_s.iloc[-1])
                        if _av > 0 and not pd.isna(_av):
                            atr14_ms_val = round(_av, 2)
                            _sl_recent = float(lo_ms.iloc[-4:-1].min()) if len(lo_ms) >= 4 else cl_ms
                            _sl_struct = max(cl_ms, _sl_recent)
                            _sl_cand   = max(_sl_struct, cp - 1.0 * _av)
                            stop_loss_ms = round(
                                min(max(_sl_cand, cp - 1.5 * _av), cp - 0.5 * _av), 2
                            )
                except Exception:
                    pass

                # ── Stock qualifies — compute display metrics ────────────────
                # 20-day return
                r20 = (
                    round(float(c.iloc[-1] / c.iloc[-(SECTOR_LOOKBACK_DAYS + 1)] - 1) * 100, 2)
                    if len(c) > SECTOR_LOOKBACK_DAYS else 0.0
                )

                # 3-month return
                r3m = (
                    round(float(c.iloc[-1] / c.iloc[-(RETURN_3M_DAYS + 1)] - 1) * 100, 2)
                    if len(c) > RETURN_3M_DAYS else 0.0
                )

                # EMA20 position
                price_vs_ema20 = None
                ema20_vs_50    = None
                if len(c) >= EMA_SHORT_PERIOD:
                    ema20 = float(
                        c.ewm(span=EMA_SHORT_PERIOD, adjust=False,
                              min_periods=EMA_SHORT_PERIOD).mean().iloc[-1]
                    )
                    if ema20 > 0:
                        price_vs_ema20 = round((cp / ema20 - 1) * 100, 2)
                    if ema50 is not None and ema20 > 0 and ema50 > 0:
                        ema20_vs_50 = round((ema20 / ema50 - 1) * 100, 2)

                # Volume Z-score
                vol_zscore = None
                if len(v) >= VOLUME_AVG_DAYS + 20 + 1:
                    cur_vol = float(v.iloc[-3:].mean())
                    w       = v.iloc[-(VOLUME_AVG_DAYS + 20):-3]
                    wstd    = float(w.std())
                    if wstd > 0:
                        vol_zscore = round((cur_vol - float(w.mean())) / wstd, 2)

                # ── Weekly RSI-14 ─────────────────────────────────────────────
                w_rsi_val = None
                try:
                    weekly_ms = (
                        df_s.resample("W-FRI")
                            .agg({"Open": "first", "High": "max", "Low": "min",
                                  "Close": "last", "Volume": "sum"})
                            .dropna(subset=["Close"])
                    )
                    if len(weekly_ms) >= 15:
                        w_rsi_s = self._rsi(weekly_ms["Close"], 14)
                        if not w_rsi_s.dropna().empty:
                            w_rsi_val = round(float(w_rsi_s.iloc[-1]), 2)
                except Exception:
                    pass

                # ── ADX-14, +DI, -DI ─────────────────────────────────────────
                adx_val = pdi_val = ndi_val = None
                try:
                    with _suppress_ta_stdout():
                        adx_res = _ta_adx(df_s, length=ADX_PERIOD)
                    if adx_res is not None and not adx_res.empty:
                        adx_col = "ADX_%d" % ADX_PERIOD
                        pdi_col = "DMP_%d" % ADX_PERIOD
                        ndi_col = "DMN_%d" % ADX_PERIOD
                        if all(c_ in adx_res.columns for c_ in (adx_col, pdi_col, ndi_col)):
                            _av = float(adx_res[adx_col].iloc[-1])
                            _pv = float(adx_res[pdi_col].iloc[-1])
                            _nv = float(adx_res[ndi_col].iloc[-1])
                            if not pd.isna(_av):
                                adx_val = round(_av, 2)
                                pdi_val = round(_pv, 2)
                                ndi_val = round(_nv, 2)
                except Exception:
                    pass

                # ── RS vs benchmark ───────────────────────────────────────────
                rs_ratio_val   = None
                rs_outperf_val = None
                if bench_c_ms is not None and len(c) > SECTOR_LOOKBACK_DAYS:
                    try:
                        bc_ms = bench_c_ms[bench_c_ms.index <= c.index[-1]]
                        if len(bc_ms) > SECTOR_LOOKBACK_DAYS:
                            stk_ret = float(c.iloc[-1] / c.iloc[-(SECTOR_LOOKBACK_DAYS + 1)] - 1)
                            idx_ret = float(bc_ms.iloc[-1] / bc_ms.iloc[-(SECTOR_LOOKBACK_DAYS + 1)] - 1)
                            rs_ratio_val   = round((1 + stk_ret) / (1 + idx_ret), 4)
                            rs_outperf_val = round((stk_ret - idx_ret) * 100, 2)
                    except Exception:
                        pass

                # ── Star Quality Score (0-100) ───────────────────────────────
                # 4 components (weights sum to 1.0):
                #   penetration (0.30) — 50%→0  to 100%+→100  (full engulf = 100)
                #   volume surge (0.25) — 1.0×avg→0  to 2.5×avg→100
                #   oversold entry (0.25) — RSI tent: peak at RSI 30-50
                #   prior decline (0.20) — 3%→0  to 15%+→100

                # 1. Penetration: min is now 50%, scale to 100% engulf
                pen      = ms_info["penetration"]
                pen_sc   = min(1.0, max(0.0, (pen - 0.50) / 0.50))
                if ms_info["full_engulf"]:
                    pen_sc = 1.0

                # 2. Volume surge: 1.0×→0.0, 2.5×→1.0
                if avg_vol > 0 and recent_vol > 0:
                    vol_ratio = recent_vol / avg_vol
                    vol_sc    = min(1.0, max(0.0, (vol_ratio - 1.0) / 1.5))
                else:
                    vol_sc = 0.2

                # 3. Oversold entry — tent function peaking at RSI 30-50
                if rsi_val is not None:
                    if rsi_val <= 50:
                        rsi_sc = max(0.0, min(1.0, (rsi_val - 20.0) / 30.0))
                    else:
                        rsi_sc = max(0.0, (75.0 - rsi_val) / 25.0)
                else:
                    rsi_sc = 0.4

                # 4. Prior decline depth: 3%→0.0, 15%+→1.0
                decline_sc = min(1.0, max(0.0, (decline_pct - 3.0) / 12.0))

                star_score = round((
                    pen_sc     * 0.30
                    + vol_sc   * 0.25
                    + rsi_sc   * 0.25
                    + decline_sc * 0.20
                ) * 100, 1)

                results.append({
                    "ticker":          t_key,
                    "display_ticker":  sym,
                    "name":            sym,
                    "price":           round(cp, 2),
                    "candle_date":     candle_date.isoformat(),
                    "return_20d":      r20,
                    "return_1m":       r20,
                    "return_3m":       r3m,
                    "avg_tv_20d_cr":   round(avg_tv_20d / 1e7, 2),
                    "rsi":             rsi_val,
                    "weekly_rsi":      w_rsi_val,
                    "adx":             adx_val,
                    "pdi":             pdi_val,
                    "ndi":             ndi_val,
                    "price_vs_ema20":  price_vs_ema20,
                    "ema20_vs_50":     ema20_vs_50,
                    "vol_zscore":      vol_zscore,
                    "rel_vol_pct":     None,
                    "rs_ratio":        rs_ratio_val,
                    "rs_outperf_pct":  rs_outperf_val,
                    "stop_loss":       stop_loss_ms,
                    "atr14":           atr14_ms_val,
                    "morning_star":    True,
                    "mom_score":       star_score,
                })
            except Exception as exc:
                logger.debug("MorningStar(%s) error: %s", t_key, exc)

            if _fi % 100 == 0 or _fi == _total:
                _progress(
                    "Morning Star: %d/%d tickers checked (%d found)"
                    % (_fi, _total, len(results))
                )
                logger.info("%s  %d/%d checked, %d Morning Star found",
                            lbl, _fi, _total, len(results))

        logger.info(
            "%s === Morning Star scan complete: %d/%d tickers qualify ===",
            lbl, len(results), len(all_data),
        )
        return results

