"""
data_sources.py -- Multi-source data fetching with priority fallbacks.

Priority chain per data type
-----------------------------
Historical OHLCV:
  1. yfinance NSE (.NS)         [primary – fast batch in scanner.py]
  2. TradingView NSE / BSE      [excellent quality, no auth required]
  3. nsepython historical       [authoritative NSE direct API]
  4. Alpha Vantage              [good quality, rate-limited, needs free key]
  5. yfinance BSE (.BO)         [existing fallback in scanner.py]
  6. NSE / BSE Bhavcopy CSV     [latest-day volume/price patch]

Live / latest-day candle:
  1. NSE live quote API  (real-time price, VWAP, issued shares)
  2. NSE Bhavcopy EOD
  3. BSE Bhavcopy EOD

Market Cap:
  1. NSE live API  (issuedSize × lastPrice  — most accurate)
  2. Screener.in   (market_cap_cr field)
  3. Alpha Vantage company overview
  4. yfinance quoteSummary

D/E Ratio:
  1. Screener.in  HTML scrape   (quarterly filing data)
  2. Apify screener actor       (if APIFY_API_KEY is set)
  3. Alpha Vantage overview     (if ALPHA_VANTAGE_API_KEY is set)
  4. yfinance quoteSummary

Sector:
  1. NSE live API  industryInfo
  2. Alpha Vantage overview
  3. yfinance quoteSummary
"""

from __future__ import annotations

import datetime as dt_mod
import logging
import re
import ssl
import threading
import time
import warnings

import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared SSL-bypass session  (corporate proxy safe)
# ---------------------------------------------------------------------------
ssl._create_default_https_context = ssl._create_unverified_context  # type: ignore[attr-defined]

_SESSION = requests.Session()
_SESSION.verify = False
_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/",
})

# Warm NSE cookies once at module load (non-fatal if it fails)
try:
    _SESSION.get("https://www.nseindia.com", timeout=10)
except Exception:
    pass


# ---------------------------------------------------------------------------
# 1. NSE Quote Client  (live price + market cap)
# ---------------------------------------------------------------------------

class NSEQuoteClient:
    """
    Fetches live equity quote from NSE India API.
    Provides real-time: lastPrice, OHLC, VWAP, volume, market cap.
    Market cap = issuedSize × lastPrice  (far more accurate than Yahoo Finance).
    """

    BASE_URL  = "https://www.nseindia.com/api/quote-equity?symbol={sym}"
    _last_call: float = 0.0
    _min_gap:   float = 0.5

    def get(self, nse_symbol: str) -> dict | None:
        sym = nse_symbol.upper().replace(".NS", "").replace(".BO", "")
        try:
            gap = time.time() - self._last_call
            if gap < self._min_gap:
                time.sleep(self._min_gap - gap)
            self._last_call = time.time()

            r = _SESSION.get(self.BASE_URL.format(sym=sym), timeout=12)
            if r.status_code != 200:
                return None

            raw = r.json()
            pi  = raw.get("priceInfo",    {}) or {}
            ii  = raw.get("industryInfo", {}) or {}
            si  = raw.get("securityInfo", {}) or {}
            md  = raw.get("metadata",     {}) or {}

            last_price    = pi.get("lastPrice") or pi.get("close")
            issued_shares = si.get("issuedSize")

            market_cap_inr = None
            if last_price and issued_shares and float(issued_shares) > 0:
                market_cap_inr = float(last_price) * float(issued_shares)

            intra = pi.get("intraDayHighLow") or {}

            return {
                "last_price":     float(last_price)    if last_price    else None,
                "open":           float(pi.get("open")  or 0) or None,
                "high":           float(intra.get("max") or pi.get("high")  or 0) or None,
                "low":            float(intra.get("min") or pi.get("low")   or 0) or None,
                "close":          float(pi.get("close") or last_price or 0) or None,
                "vwap":           float(pi.get("vwap")  or 0) or None,
                "prev_close":     float(pi.get("previousClose") or 0) or None,
                "market_cap_inr": market_cap_inr,
                "issued_shares":  float(issued_shares) if issued_shares else None,
                "sector":         ii.get("macro") or ii.get("sector"),
                "industry":       ii.get("industry") or ii.get("basicIndustry"),
                "pe_ratio":       md.get("pdSymbolPe"),
                "isin":           md.get("isin"),
                "symbol":         sym,
            }

        except Exception as exc:
            logger.debug("NSEQuoteClient.get(%s): %s", sym, exc)
            return None


# ---------------------------------------------------------------------------
# 2. Screener.in Client  (D/E ratio + quarterly fundamentals via HTML scrape)
# ---------------------------------------------------------------------------

class ScreenerClient:
    """
    Scrapes screener.in for fundamental data more accurate for Indian stocks
    than Yahoo Finance (updated quarterly from BSE/NSE filings).

    Data extracted:
      - Market Cap (Rs Cr)
      - Debt to Equity ratio
      - Current Ratio
      - Return on Equity (ROE %)
      - Promoter Holding (%)
      - Sales Growth (%)
    """

    BASE_URL   = "https://www.screener.in/company/{sym}/"
    _cache:    dict = {}
    _cache_ts: dict = {}
    _TTL = 3600  # 1 hour

    def get(self, nse_symbol: str) -> dict | None:
        sym = nse_symbol.upper().replace(".NS", "").replace(".BO", "")

        now = time.time()
        if sym in self._cache and (now - self._cache_ts.get(sym, 0)) < self._TTL:
            return self._cache[sym]

        try:
            r = _SESSION.get(self.BASE_URL.format(sym=sym), timeout=20)
            if r.status_code == 404:
                r = _SESSION.get(
                    f"https://www.screener.in/company/{sym}/consolidated/", timeout=20
                )
            if r.status_code != 200:
                return None

            html   = r.text
            result = {}

            mc = self._extract(html, r"Market Cap\s*</span[^>]*>.*?<span[^>]*>([\d,\.]+)")
            if mc:
                result["market_cap_cr"] = float(mc.replace(",", ""))

            de = self._extract(html, r"Debt to equity\s*</span[^>]*>.*?<span[^>]*>([\d,\.]+)")
            if not de:
                de = self._extract(html, r"Debt / Equity\s*</span[^>]*>.*?<span[^>]*>([\d,\.]+)")
            if de:
                result["debt_equity"] = float(de.replace(",", ""))

            cr = self._extract(html, r"Current Ratio\s*</span[^>]*>.*?<span[^>]*>([\d,\.]+)")
            if cr:
                result["current_ratio"] = float(cr.replace(",", ""))

            roe = self._extract(html, r"Return on equity\s*</span[^>]*>.*?<span[^>]*>(-?[\d,\.]+)")
            if not roe:
                roe = self._extract(html, r"ROE\s*</span[^>]*>.*?<span[^>]*>(-?[\d,\.]+)")
            if roe:
                result["roe"] = float(roe.replace(",", ""))

            ph = self._extract(html, r"Promoter Holding\s*</span[^>]*>.*?<span[^>]*>([\d,\.]+)")
            if ph:
                result["promoter_holding"] = float(ph.replace(",", ""))

            sg = self._extract(html, r"Sales growth\s*</span[^>]*>.*?<span[^>]*>(-?[\d,\.]+)")
            if sg:
                result["sales_growth_pct"] = float(sg.replace(",", ""))

            if result:
                self._cache[sym]    = result
                self._cache_ts[sym] = now
                return result
            return None

        except Exception as exc:
            logger.debug("ScreenerClient.get(%s): %s", sym, exc)
            return None

    @staticmethod
    def _extract(html: str, pattern: str) -> str | None:
        m = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        return m.group(1).strip() if m else None


# ---------------------------------------------------------------------------
# 3. TradingView Client  (historical OHLCV via tvDatafeed library)
# ---------------------------------------------------------------------------

class TradingViewClient:
    """
    Fetches historical daily OHLCV from TradingView via the tvDatafeed library.

    - Excellent data quality for Indian stocks on NSE and BSE.
    - No API key required (anonymous session used by default).
    - Optional TradingView login for premium symbols.
    - Rate-throttled to avoid blocks.

    Install: pip install tvDatafeed
    """

    _last_call: float = 0.0
    _min_gap:   float = 0.4   # ~2.5 calls/second
    _lock = threading.Lock()

    def __init__(self, username: str = "", password: str = ""):
        self._available = False
        self._tv        = None
        self._Interval  = None
        try:
            from tvDatafeed import TvDatafeed, Interval  # type: ignore[import]
            if username and password:
                self._tv = TvDatafeed(username=username, password=password)
            else:
                self._tv = TvDatafeed()
            self._Interval  = Interval
            self._available = True
            logger.info("TradingView data source: available")
        except ImportError:
            logger.info(
                "tvDatafeed not installed — TradingView source disabled. "
                "Run: pip install tvDatafeed"
            )
        except Exception as exc:
            logger.warning("TradingView init failed: %s", exc)

    @property
    def available(self) -> bool:
        return self._available

    def get_history(self, nse_symbol: str, days: int = 600,
                    end_date=None) -> pd.DataFrame | None:
        """
        Returns daily OHLCV DataFrame (Open/High/Low/Close/Volume).
        Tries NSE exchange first, then BSE as fallback.
        """
        if not self._available:
            return None

        sym    = nse_symbol.upper().replace(".NS", "").replace(".BO", "")
        n_bars = min(days + 150, 5000)

        with self._lock:
            gap = time.time() - self._last_call
            if gap < self._min_gap:
                time.sleep(self._min_gap - gap)
            self._last_call = time.time()

        for exchange in ("NSE", "BSE"):
            try:
                raw = self._tv.get_hist(
                    symbol=sym,
                    exchange=exchange,
                    interval=self._Interval.in_daily,
                    n_bars=n_bars,
                )
                if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):
                    continue

                raw.index = pd.to_datetime(raw.index).tz_localize(None).normalize()

                col_map: dict = {}
                for c in raw.columns:
                    cl = c.lower()
                    if "open"   in cl:              col_map[c] = "Open"
                    elif "high" in cl:              col_map[c] = "High"
                    elif "low"  in cl:              col_map[c] = "Low"
                    elif "close" in cl and "adj" not in cl: col_map[c] = "Close"
                    elif "volume" in cl:            col_map[c] = "Volume"
                raw = raw.rename(columns=col_map)

                needed = [c for c in ("Open", "High", "Low", "Close", "Volume")
                          if c in raw.columns]
                if len(needed) < 5:
                    continue

                df = raw[needed].copy()
                for col in needed:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df.dropna(subset=["Close"])
                df = df[df["Close"] > 0].sort_index()

                if end_date is not None:
                    ed = (end_date if isinstance(end_date, dt_mod.date)
                          else dt_mod.date.fromisoformat(str(end_date)))
                    df = df[df.index.normalize() <= pd.Timestamp(ed)]

                if not df.empty and len(df) >= 20:
                    logger.debug("TradingView(%s/%s): %d rows", sym, exchange, len(df))
                    return df

            except Exception as exc:
                logger.debug("TradingViewClient(%s/%s): %s", sym, exchange, exc)

        return None


# ---------------------------------------------------------------------------
# 4. Alpha Vantage Client  (OHLCV + comprehensive fundamentals)
# ---------------------------------------------------------------------------

class AlphaVantageClient:
    """
    Fetches historical OHLCV and company fundamentals from Alpha Vantage API.

    - Free tier: 25 API calls / day.
    - Premium tiers: 500+/min.
    - Supports Indian stocks with .NSE and .BSE suffixes.

    Set ALPHA_VANTAGE_API_KEY in config.py or as the env-var
    ALPHA_VANTAGE_API_KEY.  Get a free key at https://alphavantage.co
    """

    BASE_URL   = "https://www.alphavantage.co/query"
    _cache:    dict = {}
    _cache_ts: dict = {}
    _TTL_OHLCV  = 3600 * 6    # 6 h cache for OHLCV
    _TTL_FUND   = 3600 * 24   # 24 h cache for fundamentals
    _last_call: float = 0.0
    _min_gap:   float = 13.0   # ~4.5 calls/min  (free-tier safe)
    _lock = threading.Lock()

    def __init__(self, api_key: str = ""):
        self._key       = api_key.strip()
        self._available = bool(self._key)
        if self._available:
            logger.info("Alpha Vantage data source: enabled")
        else:
            logger.info("Alpha Vantage: no API key -- set ALPHA_VANTAGE_API_KEY to enable")

    @property
    def available(self) -> bool:
        return self._available

    def _throttle(self) -> None:
        with self._lock:
            gap = time.time() - self._last_call
            if gap < self._min_gap:
                time.sleep(self._min_gap - gap)
            self._last_call = time.time()

    # ── OHLCV ─────────────────────────────────────────────────────────────────

    def get_ohlcv(self, nse_symbol: str, days: int = 600,
                  end_date=None) -> pd.DataFrame | None:
        """Fetch split/dividend-adjusted daily OHLCV. Tries .NSE then .BSE."""
        if not self._available:
            return None

        sym = nse_symbol.upper().replace(".NS", "").replace(".BO", "")

        for av_sym in (f"{sym}.NSE", f"{sym}.BSE"):
            cache_key = f"ohlcv_{av_sym}"
            now = time.time()

            if cache_key in self._cache and \
                    (now - self._cache_ts.get(cache_key, 0)) < self._TTL_OHLCV:
                df = self._cache[cache_key]
            else:
                df = self._fetch_daily(av_sym)
                if df is not None:
                    self._cache[cache_key]    = df
                    self._cache_ts[cache_key] = now

            if df is not None and len(df) >= 20:
                result = df.copy()
                if end_date is not None:
                    ed = (end_date if isinstance(end_date, dt_mod.date)
                          else dt_mod.date.fromisoformat(str(end_date)))
                    result = result[result.index.normalize() <= pd.Timestamp(ed)]
                if not result.empty:
                    logger.debug("AlphaVantage OHLCV(%s): %d rows", av_sym, len(result))
                    return result

        return None

    def _fetch_daily(self, av_symbol: str) -> pd.DataFrame | None:
        self._throttle()
        try:
            r = _SESSION.get(self.BASE_URL, params={
                "function":   "TIME_SERIES_DAILY_ADJUSTED",
                "symbol":     av_symbol,
                "outputsize": "full",
                "apikey":     self._key,
            }, timeout=30)

            if r.status_code != 200:
                return None

            data = r.json()

            if "Note" in data or "Information" in data:
                logger.warning("Alpha Vantage rate limit for %s", av_symbol)
                return None

            ts = data.get("Time Series (Daily)", {})
            if not ts:
                return None

            records = []
            for date_str, v in ts.items():
                try:
                    raw_close = float(v.get("4. close", 0))
                    adj_close = float(v.get("5. adjusted close", raw_close))
                    ratio = (adj_close / raw_close) if raw_close > 0 else 1.0
                    records.append({
                        "date":   pd.to_datetime(date_str),
                        "Open":   float(v.get("1. open", 0)) * ratio,
                        "High":   float(v.get("2. high", 0)) * ratio,
                        "Low":    float(v.get("3. low",  0)) * ratio,
                        "Close":  adj_close,
                        "Volume": float(v.get("6. volume", 0)),
                    })
                except Exception:
                    pass

            if not records:
                return None

            df = pd.DataFrame(records).set_index("date").sort_index()
            df.index = pd.to_datetime(df.index).normalize()
            df = df.dropna(subset=["Close"])
            df = df[df["Close"] > 0]
            return df if not df.empty else None

        except Exception as exc:
            logger.debug("AlphaVantage._fetch_daily(%s): %s", av_symbol, exc)
            return None

    # ── Fundamentals ──────────────────────────────────────────────────────────

    def get_fundamentals(self, nse_symbol: str) -> dict | None:
        """
        Returns dict with:
          sector, market_cap_inr, debt_equity_ratio (raw, not ×100),
          pe_ratio, eps, dividend_yield, 52w_high, 52w_low,
          profit_margin_pct, return_on_equity_ttm, book_value
        """
        if not self._available:
            return None

        sym       = nse_symbol.upper().replace(".NS", "").replace(".BO", "")
        cache_key = f"fund_{sym}"
        now       = time.time()

        if cache_key in self._cache and \
                (now - self._cache_ts.get(cache_key, 0)) < self._TTL_FUND:
            return self._cache[cache_key]

        for av_sym in (f"{sym}.NSE", f"{sym}.BSE"):
            result = self._fetch_overview(av_sym)
            if result:
                self._cache[cache_key]    = result
                self._cache_ts[cache_key] = now
                return result

        return None

    def _fetch_overview(self, av_symbol: str) -> dict | None:
        self._throttle()
        try:
            r = _SESSION.get(self.BASE_URL, params={
                "function": "OVERVIEW",
                "symbol":   av_symbol,
                "apikey":   self._key,
            }, timeout=30)

            if r.status_code != 200:
                return None

            data = r.json()

            if "Note" in data or "Information" in data:
                logger.warning("Alpha Vantage rate limit (overview %s)", av_symbol)
                return None

            if not data or "Symbol" not in data:
                return None

            def _sf(key) -> float | None:
                try:
                    v = float(data.get(key, ""))
                    return None if (v == 0 or np.isnan(v)) else v
                except Exception:
                    return None

            result: dict = {}

            sector = data.get("Sector", "")
            if sector and sector not in ("None", "-", "", "N/A"):
                result["sector"] = sector

            # Market cap — AV reports in INR for BSE/NSE symbols
            mc = _sf("MarketCapitalization")
            if mc:
                result["market_cap_inr"] = mc

            # D/E ratio (raw ratio — NOT ×100)
            de = _sf("DebtToEquityRatio")
            if de is not None:
                result["debt_equity_ratio"] = de

            for k, field in (("pe_ratio", "PERatio"), ("eps", "EPS"),
                              ("dividend_yield", "DividendYield"),
                              ("book_value", "BookValue"),
                              ("52w_high", "52WeekHigh"),
                              ("52w_low",  "52WeekLow")):
                v = _sf(field)
                if v is not None:
                    result[k] = v

            pm = _sf("ProfitMargin")
            if pm is not None:
                result["profit_margin_pct"] = round(pm * 100, 2)

            roe = _sf("ReturnOnEquityTTM")
            if roe is not None:
                result["return_on_equity_ttm"] = round(roe * 100, 2)

            return result if result else None

        except Exception as exc:
            logger.debug("AlphaVantage._fetch_overview(%s): %s", av_symbol, exc)
            return None


# ---------------------------------------------------------------------------
# 5. Apify Screener Client  (screener.in via Apify — structured fundamental data)
# ---------------------------------------------------------------------------

class ApifyScreenerClient:
    """
    Fetches fundamental data from screener.in via an Apify actor.
    More reliable than direct HTML scraping when screener.in changes its layout.

    Set APIFY_API_KEY and (optionally) APIFY_SCREENER_ACTOR_ID in config.py.
    The actor should accept {"symbols": ["RELIANCE"]} and return item list.

    Returned keys (best-effort, actor-dependent):
      market_cap_cr, debt_equity, current_ratio, roe, promoter_holding,
      sales_growth_pct, net_profit_growth_pct, pe_ratio
    """

    RUN_SYNC_URL = (
        "https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items"
        "?token={token}&timeout=60&memory=256"
    )
    _cache:    dict = {}
    _cache_ts: dict = {}
    _TTL = 3600 * 6  # 6 hours

    def __init__(self, api_key: str = "", actor_id: str = ""):
        self._key     = api_key.strip()
        self._actor   = actor_id.strip() or "emastra~screener-stock-data-scraper"
        self._available = bool(self._key)
        if self._available:
            logger.info("Apify screener: enabled (actor=%s)", self._actor)
        else:
            logger.info("Apify: no API key -- set APIFY_API_KEY to enable")

    @property
    def available(self) -> bool:
        return self._available

    def get(self, nse_symbol: str) -> dict | None:
        if not self._available:
            return None

        sym = nse_symbol.upper().replace(".NS", "").replace(".BO", "")
        now = time.time()
        if sym in self._cache and (now - self._cache_ts.get(sym, 0)) < self._TTL:
            return self._cache[sym]

        try:
            url = self.RUN_SYNC_URL.format(actor=self._actor, token=self._key)
            r   = _SESSION.post(url,
                                json={"symbols": [sym], "maxItems": 1},
                                timeout=90)
            if r.status_code not in (200, 201):
                logger.debug("Apify(%s): HTTP %d", sym, r.status_code)
                return None

            items = r.json()
            if not items or not isinstance(items, list):
                return None

            item   = items[0]
            result: dict = {}

            # Flexible key mapping (actor responses vary)
            _field_map = {
                "market_cap_cr":         ("marketCap", "market_cap", "mktCap"),
                "debt_equity":           ("debtToEquity", "debt_to_equity", "de"),
                "current_ratio":         ("currentRatio", "current_ratio"),
                "roe":                   ("roe", "returnOnEquity", "return_on_equity"),
                "promoter_holding":      ("promoterHolding", "promoter_holding"),
                "sales_growth_pct":      ("salesGrowth", "sales_growth"),
                "net_profit_growth_pct": ("npGrowth", "net_profit_growth"),
                "pe_ratio":              ("pe", "peRatio", "priceToEarnings"),
            }
            for out_key, candidates in _field_map.items():
                for candidate in candidates:
                    if candidate in item:
                        try:
                            result[out_key] = float(item[candidate])
                        except Exception:
                            pass
                        break

            if result:
                self._cache[sym]    = result
                self._cache_ts[sym] = now
                return result
            return None

        except Exception as exc:
            logger.debug("ApifyScreenerClient.get(%s): %s", sym, exc)
            return None


# ---------------------------------------------------------------------------
# 6. NSE Historical Data Client  ("Indian-Stock-Market-API GitHub")
#    Uses nsepython's API URL pattern with our SSL-bypassing session so it
#    works correctly in corporate proxy environments.
# ---------------------------------------------------------------------------

class NSEPythonHistClient:
    """
    Fetches historical daily OHLCV directly from NSE India's official API.

    Endpoint:
      https://www.nseindia.com/api/historical/cm/equity?symbol=X&series=["EQ"]&from=DD-MM-YYYY&to=DD-MM-YYYY

    Inspired by: https://github.com/swapniljariwala/nsepython  (nsepython library)

    Uses our shared SSL-bypassing session (_SESSION) so this works in
    corporate proxy environments where the nsepython library's own session
    would fail due to certificate verification errors.

    - Authoritative NSE prices; no adjusted-price discrepancies.
    - API fetches max ~40-day windows; loops to cover longer periods.
    - Rate-throttled per NSE's unofficial limits.
    """

    # NSE historical API — same endpoint nsepython uses internally
    _API_URL   = (
        "https://www.nseindia.com/api/historical/cm/equity"
        "?symbol={sym}&series=[%22EQ%22]&from={start}&to={end}"
    )
    _CHUNK_DAYS = 38          # NSE API max window per call (~40 days)
    _cache:    dict = {}
    _cache_ts: dict = {}
    _TTL      = 3600 * 2      # 2 h cache
    _last_call: float = 0.0
    _min_gap:   float = 0.8   # NSE rate-limit safety (per call)
    _lock = threading.Lock()

    def __init__(self):
        # Always available — uses _SESSION (shared SSL-bypass) directly
        self._available = True
        logger.info("NSE historical data source: available (direct API)")

    @property
    def available(self) -> bool:
        return self._available

    def get_history(self, nse_symbol: str, days: int = 600,
                    end_date=None) -> pd.DataFrame | None:
        """
        Returns daily OHLCV DataFrame (Open/High/Low/Close/Volume) from
        NSE India's official historical data API.

        Calls the API in up to ~40-day chunks to respect NSE's window limit,
        then concatenates the results.
        """
        sym = nse_symbol.upper().replace(".NS", "").replace(".BO", "")

        ed = (end_date if isinstance(end_date, dt_mod.date)
              else (dt_mod.date.fromisoformat(str(end_date))
                    if end_date else dt_mod.date.today()))
        sd = ed - dt_mod.timedelta(days=days + 10)

        cache_key = f"{sym}_{sd}_{ed}"
        now       = time.time()

        if cache_key in self._cache and \
                (now - self._cache_ts.get(cache_key, 0)) < self._TTL:
            return self._cache[cache_key]

        # Split date range into _CHUNK_DAYS chunks
        chunks: list[tuple[dt_mod.date, dt_mod.date]] = []
        chunk_start = sd
        while chunk_start <= ed:
            chunk_end = min(chunk_start + dt_mod.timedelta(days=self._CHUNK_DAYS - 1), ed)
            chunks.append((chunk_start, chunk_end))
            chunk_start = chunk_end + dt_mod.timedelta(days=1)

        frames: list[pd.DataFrame] = []
        for c_start, c_end in chunks:
            chunk_df = self._fetch_chunk(sym, c_start, c_end)
            if chunk_df is not None and not chunk_df.empty:
                frames.append(chunk_df)

        if not frames:
            return None

        df = pd.concat(frames).sort_index()
        df = df[~df.index.duplicated(keep="last")]
        df = df.dropna(subset=["Close"])
        df = df[df["Close"] > 0]

        if not df.empty:
            self._cache[cache_key]    = df
            self._cache_ts[cache_key] = now
            logger.debug("NSEHistDirect(%s): %d rows (%d chunks)", sym, len(df), len(chunks))
            return df

        return None

    def _fetch_chunk(self, sym: str, start: dt_mod.date,
                     end: dt_mod.date) -> "pd.DataFrame | None":
        with self._lock:
            gap = time.time() - self._last_call
            if gap < self._min_gap:
                time.sleep(self._min_gap - gap)
            self._last_call = time.time()
        try:
            url = self._API_URL.format(
                sym   = sym,
                start = start.strftime("%d-%m-%Y"),
                end   = end.strftime("%d-%m-%Y"),
            )
            r = _SESSION.get(url, timeout=15)
            if r.status_code != 200:
                logger.debug("NSEHistDirect(%s) chunk %s-%s: HTTP %d",
                             sym, start, end, r.status_code)
                return None

            payload = r.json()
            data    = payload.get("data", [])
            if not data:
                return None

            records = []
            for row in data:
                try:
                    date_val = pd.to_datetime(
                        row.get("CH_TIMESTAMP") or row.get("mDATA_DT_DATETIME", ""),
                        errors="coerce",
                    )
                    records.append({
                        "date":   date_val,
                        "Open":   float(row.get("CH_OPENING_PRICE",   0) or 0),
                        "High":   float(row.get("CH_TRADE_HIGH_PRICE", 0) or 0),
                        "Low":    float(row.get("CH_TRADE_LOW_PRICE",  0) or 0),
                        "Close":  float(row.get("CH_CLOSING_PRICE",    0) or 0),
                        "Volume": float(row.get("CH_TOT_TRADED_QTY",   0) or 0),
                    })
                except Exception:
                    pass

            if not records:
                return None

            df = pd.DataFrame(records).set_index("date")
            df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
            df = df.dropna(subset=["Close"])
            df = df[df["Close"] > 0].sort_index()
            return df if not df.empty else None

        except Exception as exc:
            logger.debug("NSEHistDirect._fetch_chunk(%s %s-%s): %s",
                         sym, start, end, exc)
            return None


# ---------------------------------------------------------------------------
# 7. Unified Fundamentals Fetcher  (full multi-source priority chain)
# ---------------------------------------------------------------------------

class FundamentalsClient:
    """
    Returns fundamentals for a stock using the best available source.

    Priority:
      market_cap  : NSE live API → Screener.in → Apify → Alpha Vantage → yfinance
      debt_equity : Screener.in  → Apify        → Alpha Vantage        → yfinance
      sector      : NSE live API → Alpha Vantage → yfinance
    """

    def __init__(
        self,
        alpha_key:   str = "",
        apify_key:   str = "",
        apify_actor: str = "",
    ):
        self._nse      = NSEQuoteClient()
        self._screener = ScreenerClient()
        self._av       = AlphaVantageClient(api_key=alpha_key)
        self._apify    = ApifyScreenerClient(api_key=apify_key, actor_id=apify_actor)

    def get(
        self,
        ticker: str,
        yf_fundamentals_fn=None,
    ) -> tuple[str | None, float | None, float | None]:
        """
        Returns (sector, debt_equity_x100, market_cap_inr).

        debt_equity_x100 : ratio × 100  (Yahoo legacy format — 250 = D/E 2.5)
        market_cap_inr   : absolute INR (e.g. 12e9 = Rs.1200 Cr)
        Any value may be None if unavailable from all sources.
        """
        sym = ticker.upper().replace(".NS", "").replace(".BO", "")

        # ── 1. NSE live (market cap + sector) ───────────────────────────────
        nse_data   = None
        try:
            nse_data = self._nse.get(sym)
        except Exception:
            pass

        market_cap = nse_data.get("market_cap_inr") if nse_data else None
        sector     = nse_data.get("sector")          if nse_data else None

        # ── 2. Screener.in (D/E + market cap cross-check) ───────────────────
        debt_equity = None
        try:
            sc = self._screener.get(sym)
            if sc:
                if "debt_equity" in sc:
                    debt_equity = sc["debt_equity"] * 100   # → ×100 format
                if market_cap is None and "market_cap_cr" in sc:
                    market_cap = sc["market_cap_cr"] * 1e7
        except Exception:
            pass

        # ── 3. Apify screener (D/E fallback) ────────────────────────────────
        if debt_equity is None and self._apify.available:
            try:
                ap = self._apify.get(sym)
                if ap:
                    if "debt_equity" in ap:
                        debt_equity = ap["debt_equity"] * 100
                    if market_cap is None and "market_cap_cr" in ap:
                        market_cap = ap["market_cap_cr"] * 1e7
            except Exception:
                pass

        # ── 4. Alpha Vantage (sector + market cap + D/E fallback) ────────────
        if (market_cap is None or sector is None or debt_equity is None) \
                and self._av.available:
            try:
                av = self._av.get_fundamentals(sym)
                if av:
                    if sector is None and "sector" in av:
                        sector = av["sector"]
                    if market_cap is None and "market_cap_inr" in av:
                        market_cap = av["market_cap_inr"]
                    if debt_equity is None and "debt_equity_ratio" in av:
                        debt_equity = av["debt_equity_ratio"] * 100  # → ×100
            except Exception:
                pass

        # ── 5. Yahoo Finance fallback (last resort) ───────────────────────────
        if (market_cap is None or sector is None or debt_equity is None) \
                and yf_fundamentals_fn is not None:
            try:
                yf_sector, yf_de, yf_mc = yf_fundamentals_fn(ticker)
                if sector      is None: sector      = yf_sector
                if debt_equity is None: debt_equity = yf_de
                if market_cap  is None: market_cap  = yf_mc
            except Exception:
                pass

        return sector, debt_equity, market_cap

    def get_live_candle(self, nse_symbol: str) -> dict | None:
        """Returns today's live OHLCV candle from NSE live API."""
        try:
            return self._nse.get(nse_symbol)
        except Exception:
            return None

    def get_extra_fundamentals(self, nse_symbol: str) -> dict:
        """
        Returns enriched display-only fields (non-critical for filters).
        Sources: Screener.in + Alpha Vantage.
        Keys: roe, current_ratio, promoter_holding, pe_ratio, eps,
              sales_growth_pct, dividend_yield, 52w_high, 52w_low,
              profit_margin_pct, return_on_equity_ttm, book_value
        """
        sym    = nse_symbol.upper().replace(".NS", "").replace(".BO", "")
        extras: dict = {}

        try:
            sc = self._screener.get(sym)
            if sc:
                for k in ("roe", "current_ratio", "promoter_holding",
                          "sales_growth_pct"):
                    if k in sc:
                        extras[k] = sc[k]
        except Exception:
            pass

        if self._av.available:
            try:
                av = self._av.get_fundamentals(sym)
                if av:
                    for k in ("pe_ratio", "eps", "dividend_yield", "52w_high",
                              "52w_low", "profit_margin_pct",
                              "return_on_equity_ttm", "book_value"):
                        if k in av and k not in extras:
                            extras[k] = av[k]
            except Exception:
                pass

        return extras


# ---------------------------------------------------------------------------
# Module-level singleton factory helpers
# ---------------------------------------------------------------------------

def _build_fundamentals_client() -> FundamentalsClient:
    try:
        from config import (                        # type: ignore[import]
            ALPHA_VANTAGE_API_KEY, APIFY_API_KEY,
            APIFY_SCREENER_ACTOR_ID,
            ENABLE_ALPHA_VANTAGE, ENABLE_APIFY_SCREENER,
        )
        return FundamentalsClient(
            alpha_key   = ALPHA_VANTAGE_API_KEY   if ENABLE_ALPHA_VANTAGE  else "",
            apify_key   = APIFY_API_KEY            if ENABLE_APIFY_SCREENER else "",
            apify_actor = APIFY_SCREENER_ACTOR_ID  if ENABLE_APIFY_SCREENER else "",
        )
    except Exception:
        return FundamentalsClient()


def _build_tradingview_client() -> TradingViewClient:
    try:
        from config import (                        # type: ignore[import]
            TRADINGVIEW_USERNAME, TRADINGVIEW_PASSWORD, ENABLE_TRADINGVIEW,
        )
        if not ENABLE_TRADINGVIEW:
            c = TradingViewClient.__new__(TradingViewClient)
            c._available = False
            return c
        return TradingViewClient(username=TRADINGVIEW_USERNAME,
                                  password=TRADINGVIEW_PASSWORD)
    except Exception:
        return TradingViewClient()


def _build_nse_hist_client() -> NSEPythonHistClient:
    try:
        from config import ENABLE_NSE_PYTHON_HIST  # type: ignore[import]
        if not ENABLE_NSE_PYTHON_HIST:
            c = NSEPythonHistClient.__new__(NSEPythonHistClient)
            c._available = False
            return c
    except Exception:
        pass
    return NSEPythonHistClient()


def _build_alpha_vantage_client() -> AlphaVantageClient:
    try:
        from config import (                        # type: ignore[import]
            ALPHA_VANTAGE_API_KEY, ENABLE_ALPHA_VANTAGE,
        )
        return AlphaVantageClient(
            api_key=ALPHA_VANTAGE_API_KEY if ENABLE_ALPHA_VANTAGE else ""
        )
    except Exception:
        return AlphaVantageClient()


# ---------------------------------------------------------------------------
# Module-level singletons — import these in scanner.py / other modules
# ---------------------------------------------------------------------------
_nse_quote    = NSEQuoteClient()
_screener     = ScreenerClient()
fundamentals  = _build_fundamentals_client()      # multi-source fundamentals
tv_client     = _build_tradingview_client()        # TradingView OHLCV
nse_hist      = _build_nse_hist_client()           # nsepython NSE historical
alpha_vantage = _build_alpha_vantage_client()      # Alpha Vantage standalone
