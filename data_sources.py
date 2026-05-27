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
  1. Screener.in  HTML scrape  (quarterly filing data — sole source)

Sector:
  1. Screener.in  HTML scrape  (sole source)
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
# Increase connection pool for screener.in to match the 12-thread bg-worker
_screener_adapter = requests.adapters.HTTPAdapter(
    pool_connections=12,
    pool_maxsize=12,
)
_SESSION.mount("https://www.screener.in", _screener_adapter)
_SESSION.mount("http://www.screener.in", _screener_adapter)

# Separate session for Yahoo Finance (no NSE referer — Yahoo rejects it)
_YF_SESSION = requests.Session()
_YF_SESSION.verify = False
_YF_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
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
    Scrapes screener.in for fundamental data (quarterly filing-accurate for Indian stocks).

    Consolidated page is tried first; standalone is used as fallback.

    Fields extracted:
      market_cap_cr, current_price, pe_ratio, book_value, roce, roe,
      dividend_yield, debt_equity, current_ratio, peg_ratio, pb_ratio,
      face_value, eps, industry_pe, week52_high, week52_low,
      quick_ratio, interest_coverage,
      promoter_holding, fii_holding, dii_holding, public_holding,
      sales_growth_pct, sales_growth_5y, sales_growth_10y, sales_growth_ttm,
      profit_growth_3y, profit_growth_5y, profit_growth_10y, profit_growth_ttm,
      roe_5y, roe_10y,
      cash_from_operations,
      opm  (Operating Profit Margin % — from P&L "OPM %" row),
      net_profit_margin  (computed as Net Profit / Sales × 100 from P&L; ratios table fallback)
    """

    _BASE   = "https://www.screener.in/company/{sym}/{mode}/"
    _cache:    dict = {}
    _cache_ts: dict = {}
    _TTL = 3600  # 1 hour in-memory TTL

    def get(self, nse_symbol: str) -> dict | None:
        sym = nse_symbol.upper().replace(".NS", "").replace(".BO", "")

        now = time.time()
        if sym in self._cache and (now - self._cache_ts.get(sym, 0)) < self._TTL:
            return self._cache[sym]

        result: dict = {}
        for mode in ("consolidated", ""):    # consolidated preferred; "" = standalone
            url = self._BASE.format(sym=sym, mode=mode).rstrip("/") + "/"
            try:
                r = _SESSION.get(url, timeout=20)
                if r.status_code == 404:
                    continue
                if r.status_code != 200:
                    continue
                html = r.text

                # ── 1. Key metrics block ──────────────────────────────────────────────
                kv = re.findall(
                    r'<span\s+class="name">\s*(.*?)\s*</span>.*?<span\s+class="number">([\d,\.]+)',
                    html, re.DOTALL,
                )
                for raw_key, raw_val in kv:
                    key = re.sub(r'\s+', ' ', raw_key).strip()
                    key_l = key.lower()          # ← case-insensitive comparisons
                    try:
                        v = float(raw_val.replace(',', ''))
                    except ValueError:
                        continue
                    if 'market cap' in key_l:
                        result.setdefault('market_cap_cr', v)
                    elif 'current price' in key_l:
                        result.setdefault('current_price', v)
                    elif 'stock p/e' in key_l or key_l == 'p/e' or key_l == 'pe ratio':
                        result.setdefault('pe_ratio', v)
                    elif 'book value' in key_l:
                        result.setdefault('book_value', v)
                    elif 'return on capital' in key_l or 'roce' in key_l:
                        result.setdefault('roce', v)
                    elif 'return on equity' in key_l or key_l == 'roe':
                        result.setdefault('roe', v)
                    elif 'dividend yield' in key_l:
                        result.setdefault('dividend_yield', v)
                    elif 'debt to equity' in key_l or 'debt / equity' in key_l or 'debt/equity' in key_l:
                        result.setdefault('debt_equity', v)
                    elif 'current ratio' in key_l:
                        result.setdefault('current_ratio', v)
                    elif 'face value' in key_l or key_l == 'face val':
                        result.setdefault('face_value', v)
                    elif key_l in ('eps', 'eps (ttm)') or ('earnings per share' in key_l):
                        result.setdefault('eps', v)
                    elif 'industry p/e' in key_l or 'industry pe' in key_l:
                        result.setdefault('industry_pe', v)
                    elif '52 week high' in key_l or 'high / low' in key_l or '52w high' in key_l:
                        result.setdefault('week52_high', v)
                    elif '52 week low' in key_l or '52w low' in key_l:
                        result.setdefault('week52_low', v)
                    elif 'peg' in key_l and 'ratio' in key_l:
                        result.setdefault('peg_ratio', v)
                    elif 'price to book' in key_l or key_l == 'p/b':
                        result.setdefault('pb_ratio', v)
                    elif 'net profit margin' in key_l or key_l == 'npm' or key_l == 'npm %':
                        result.setdefault('net_profit_margin', v)
                    elif 'opm' in key_l or 'operating profit margin' in key_l:
                        result.setdefault('opm', v)
                    elif 'quick ratio' in key_l:
                        result.setdefault('quick_ratio', v)
                    elif 'interest coverage' in key_l:
                        result.setdefault('interest_coverage', v)

                # ── 1b.  D/E from Balance Sheet rows (primary source) ─────────────
                # Screener.in never labels "Debt to equity" in key metrics.
                # We compute it: D/E = Borrowings / (Equity Share Capital + Reserves)
                def _last_bs_val(label: str) -> float | None:
                    """Extract the most-recent-year value for a balance-sheet row."""
                    idx2 = html.find(label)
                    if idx2 == -1:
                        return None
                    rs2 = html.rfind('<tr', max(0, idx2 - 300), idx2)
                    re2 = html.find('</tr>', idx2)
                    chunk2 = (html[rs2: re2 + 5]
                              if rs2 != -1 and re2 != -1
                              else html[idx2: idx2 + 800])
                    nums2 = []
                    for tv in re.findall(r'<td[^>]*>\s*(-?[\d,]+(?:\.\d+)?)\s*</td>', chunk2):
                        try:
                            nums2.append(float(tv.replace(',', '')))
                        except ValueError:
                            pass
                    return nums2[-1] if nums2 else None

                if 'debt_equity' not in result:
                    borrowings  = _last_bs_val('Borrowings')
                    reserves    = _last_bs_val('Reserves')
                    share_cap   = (_last_bs_val('Equity Share Capital')
                                   or _last_bs_val('Share Capital'))
                    if borrowings is not None and borrowings >= 0:
                        equity = (share_cap or 0.0) + (reserves or 0.0)
                        if equity > 0:
                            result.setdefault('debt_equity',
                                              round(borrowings / equity, 2))
                        elif borrowings == 0.0:
                            result.setdefault('debt_equity', 0.0)

                # ── 2. Promoter + FII + DII from shareholding section ─────────────
                sh_idx = html.find('id="shareholding"')
                if sh_idx != -1:
                    sh_chunk = html[sh_idx: sh_idx + 6000]
                    # Try multiple label variants for each holder type
                    _sh_patterns = [
                        ('promoter_holding',
                         ['Promoters', 'Promoter', 'Promoter &amp; Promoter Group']),
                        ('fii_holding',
                         ['FII', 'Foreign Institutional Investors', 'FII / FPI',
                          'Foreign Portfolio', 'FPI']),
                        ('dii_holding',
                         ['DII', 'Domestic Institutional', 'Domestic Institutional Investors',
                          'MF', 'Mutual Fund']),
                        ('public_holding',
                         ['Public', 'Retail']),
                    ]
                    for field, labels in _sh_patterns:
                        for label in labels:
                            m = re.search(
                                rf'{re.escape(label)}.*?<td[^>]*>\s*([\d\.]+)%?\s*</td>',
                                sh_chunk, re.DOTALL,
                            )
                            if m:
                                result.setdefault(field, float(m.group(1)))
                                break

                # ── 3. Compounded growth tables (Sales + Profit) ──────────────────
                for section, pref_3y, pref_5y, pref_10y, pref_ttm in [
                    ('Compounded Sales Growth',
                     'sales_growth_pct', 'sales_growth_5y', 'sales_growth_10y', 'sales_growth_ttm'),
                    ('Compounded Profit Growth',
                     'profit_growth_3y', 'profit_growth_5y', 'profit_growth_10y', 'profit_growth_ttm'),
                ]:
                    g_idx = html.find(section)
                    if g_idx != -1:
                        chunk = html[g_idx: g_idx + 500]
                        rows = re.findall(
                            r'<td>\s*([^<]+?)\s*</td>\s*<td>\s*(-?[\d]+)\s*%\s*</td>',
                            chunk,
                        )
                        for period, pct in rows:
                            p = period.strip().lower()
                            if '10 year' in p:
                                result.setdefault(pref_10y,  float(pct))
                            elif '5 year' in p:
                                result.setdefault(pref_5y,   float(pct))
                            elif '3 year' in p:
                                result.setdefault(pref_3y,   float(pct))
                            elif 'ttm' in p or 'trailing' in p:
                                result.setdefault(pref_ttm,  float(pct))

                # ── 4. Return on Equity history (10Y avg = moat signal) ───────────
                roe_idx = html.find('Return on Equity')
                if roe_idx != -1:
                    chunk = html[roe_idx: roe_idx + 400]
                    rows = re.findall(
                        r'<td>\s*([^<]+?)\s*</td>\s*<td>\s*(-?[\d]+)\s*%\s*</td>',
                        chunk,
                    )
                    for period, pct in rows:
                        p = period.strip().lower()
                        if '10 year' in p:
                            result.setdefault('roe_10y', float(pct))
                        elif '5 year' in p:
                            result.setdefault('roe_5y',  float(pct))

                # ── 5. Cash from Operations — cash flow section ───────────────────
                _cfo_labels = (
                    'Cash from Operating Activity',
                    'Cash from Operations',
                    'Cash from operating activity',
                    'Cash From Operating Activity',
                    'Cash from Operating Activity +',
                    'Cash from operating activities',
                    'Net Cash from Operating Activities',
                    'Net Cash from Operations',
                    'Operating Activities',
                )
                for cfo_label in _cfo_labels:
                    cf_idx = html.find(cfo_label)
                    if cf_idx == -1:
                        continue

                    row_start = html.rfind('<tr', max(0, cf_idx - 300), cf_idx)
                    row_end   = html.find('</tr>', cf_idx)
                    if row_start != -1 and row_end != -1:
                        row_chunk = html[row_start: row_end + 5]
                    else:
                        row_chunk = html[cf_idx: cf_idx + 800]

                    td_vals = re.findall(
                        r'<td[^>]*>\s*(-?[\d,]+(?:\.\d+)?)\s*</td>',
                        row_chunk,
                    )
                    nums: list = []
                    for tv in td_vals:
                        try:
                            nums.append(float(tv.replace(',', '')))
                        except ValueError:
                            pass

                    non_zero = [n for n in nums if n != 0.0]
                    if non_zero:
                        result.setdefault('cash_from_operations', non_zero[-1])
                        break

                # ── 6. OPM + NPM from Profit & Loss section ──────────────────────
                # Uses the full P&L section (up to ratios section or 70 KB).
                # OPM is the "OPM %" row; NPM is computed as Net Profit / Sales
                # from the last column since screener.in no longer shows NPM %.
                _pl_end = html.find('id="ratios"')
                pl_idx  = html.find('id="profit-loss"')
                if pl_idx != -1:
                    _pl_limit = (_pl_end if _pl_end > pl_idx else pl_idx + 70000)
                    pl_chunk  = html[pl_idx: _pl_limit]

                    def _last_row_val(label: str) -> float | None:
                        """Return the last numeric cell value of the P&L row with given label."""
                        lbl_idx = pl_chunk.find(label)
                        if lbl_idx == -1:
                            return None
                        rs = pl_chunk.rfind('<tr', max(0, lbl_idx - 500), lbl_idx)
                        re_ = pl_chunk.find('</tr>', lbl_idx)
                        if rs == -1 or re_ == -1:
                            return None
                        row = pl_chunk[rs: re_ + 5]
                        nums = []
                        for v in re.findall(
                                r'<td[^>]*>\s*(-?[\d,]+(?:\.\d+)?)\s*%?\s*</td>',
                                row, re.DOTALL):
                            try:
                                nums.append(float(v.replace(',', '')))
                            except ValueError:
                                pass
                        return nums[-1] if nums else None

                    if 'opm' not in result:
                        opm_val = _last_row_val('OPM %')
                        if opm_val is not None:
                            result.setdefault('opm', opm_val)

                    if 'net_profit_margin' not in result:
                        # NPM = Net Profit (last year) / Sales (last year) * 100
                        # screener.in uses &nbsp; entity in row labels (not decoded by requests)
                        net_profit = (_last_row_val('Net Profit&nbsp;')
                                      or _last_row_val('Net Profit\xa0')
                                      or _last_row_val('Net Profit '))
                        sales      = (_last_row_val('Sales&nbsp;')
                                      or _last_row_val('Sales\xa0')
                                      or _last_row_val('Sales '))
                        if net_profit is not None and sales and sales > 0:
                            result.setdefault('net_profit_margin',
                                              round(net_profit / sales * 100, 1))

                # ── 7. Net profit margin from ratios table (fallback) ─────────────
                if 'net_profit_margin' not in result:
                    r_idx = html.find('id="ratios"')
                    if r_idx != -1:
                        r_chunk = html[r_idx: r_idx + 15000]
                        for npm_label in ('Net Profit margin', 'NPM', 'Net profit margin',
                                          'Net Profit Margin', 'Net Profit %', 'NPM %'):
                            npm_idx = r_chunk.find(npm_label)
                            if npm_idx == -1:
                                continue
                            npm_rs  = r_chunk.rfind('<tr', max(0, npm_idx - 500), npm_idx)
                            npm_re  = r_chunk.find('</tr>', npm_idx)
                            if npm_rs != -1 and npm_re != -1:
                                npm_row = r_chunk[npm_rs: npm_re + 5]
                                npm_vs  = re.findall(
                                    r'<td[^>]*>\s*(-?[\d,\.]+)\s*%?\s*</td>',
                                    npm_row, re.DOTALL)
                                npm_ns  = []
                                for nv in npm_vs:
                                    try:
                                        npm_ns.append(float(nv.replace(',', '')))
                                    except ValueError:
                                        pass
                                if npm_ns:
                                    result.setdefault('net_profit_margin', npm_ns[-1])
                                    break

                if result:
                    break   # data found — no need to try alternate URL

            except Exception as exc:
                logger.debug("ScreenerClient.get(%s) [%s]: %s", sym, mode, exc)
                continue

        if result:
            self._cache[sym]    = result
            self._cache_ts[sym] = time.time()
            return result
        return None

    def get_sme(self, nse_symbol: str) -> dict | None:
        """Like get() but also tries the '-SME' URL slug used by NSE Emerge stocks.

        Many NSE Emerge companies are indexed on screener.in as:
          https://www.screener.in/company/TICKER-SME/
        instead of the regular:
          https://www.screener.in/company/TICKER/
        """
        sym = nse_symbol.upper().replace(".NS", "").replace(".BO", "")

        # 1. Try normal URL first (works for graduated / dual-listed SME stocks)
        result = self.get(sym)
        if result:
            return result

        # 2. Try with -SME suffix (NSE Emerge stocks on screener.in)
        sme_sym = sym + "-SME"
        result2 = self.get(sme_sym)
        if result2:
            # Also cache under the plain symbol so future calls are fast
            self._cache[sym]    = result2
            self._cache_ts[sym] = time.time()
        return result2

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
# 7a. Yahoo Finance Fundamentals Client  (sector + D/E + market cap via crumb API)
# ---------------------------------------------------------------------------

class YahooFundamentalsClient:
    """
    Fetches sector, debt-to-equity (×100 format) and market cap from
    Yahoo Finance's v10 quoteSummary API using crumb-based auth.

    Used as the FIRST source in FundamentalsClient.get(); Screener.in is the
    fallback when Yahoo is blocked / rate-limited / returns no data.

    Cache TTL: 4 hours so repeated calls within a scan cycle are instant.
    """

    _SUMMARY_URL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{t}"
    _HOME_URL    = "https://finance.yahoo.com/"
    _CRUMB_TTL   = 3600          # re-fetch crumb every hour

    _cache:    dict  = {}
    _cache_ts: dict  = {}
    _TTL      = 3600 * 4         # 4-hour in-memory cache

    # Class-level crumb state (shared across all instances / threads)
    _crumb:      "str | None" = None
    _crumb_ts:   float        = 0.0
    _crumb_lock: threading.Lock = threading.Lock()

    # ── crumb management ───────────────────────────────────────────────────

    def _init_crumb(self) -> None:
        """Obtain / refresh the Yahoo Finance crumb (thread-safe)."""
        with self._crumb_lock:
            if self._crumb and time.time() - self._crumb_ts < self._CRUMB_TTL:
                return
            try:
                r = _YF_SESSION.get(self._HOME_URL, timeout=20)
                r.raise_for_status()
                hits = re.compile(r'"crumb":"([^"]+)"').findall(r.text)
                if not hits:
                    raise RuntimeError("crumb not found in Yahoo Finance HTML")
                self.__class__._crumb    = hits[0]
                self.__class__._crumb_ts = time.time()
                logger.debug("YahooFundamentalsClient: crumb refreshed")
            except Exception as exc:
                logger.debug("YahooFundamentalsClient._init_crumb: %s", exc)
                raise

    # ── public API ─────────────────────────────────────────────────────────

    def get(self, nse_symbol: str) -> "dict | None":
        """
        Returns dict (any key may be absent if unavailable):
          sector          : str
          debt_equity_x100: float  — Yahoo ×100 format (250 = D/E 2.5)
          market_cap_inr  : float  — absolute INR
        Returns None on error / no data.
        """
        sym = nse_symbol.upper().replace(".NS", "").replace(".BO", "")
        now = time.time()

        if sym in self._cache and (now - self._cache_ts.get(sym, 0)) < self._TTL:
            return self._cache[sym]

        # Try NSE ticker (.NS) first; BO ticker as fallback
        for ticker in (sym + ".NS", sym + ".BO"):
            try:
                self._init_crumb()
                r = _YF_SESSION.get(
                    self._SUMMARY_URL.format(t=ticker),
                    params={"modules": "summaryDetail,assetProfile,financialData",
                            "crumb":   self._crumb},
                    timeout=15,
                )
                if r.status_code != 200:
                    continue

                res = r.json().get("quoteSummary", {}).get("result")
                if not res:
                    continue

                sd = res[0].get("summaryDetail",  {}) or {}
                ap = res[0].get("assetProfile",   {}) or {}
                fd = res[0].get("financialData",  {}) or {}

                def _raw(d: dict, key: str):
                    v = d.get(key, {})
                    return v.get("raw") if isinstance(v, dict) else v

                result: dict = {}

                sector = ap.get("sector", "")
                if sector and sector not in ("None", "-", "", "N/A"):
                    result["sector"] = sector

                de = _raw(fd, "debtToEquity")   # already in ×100 format
                if de is not None:
                    try:
                        result["debt_equity_x100"] = float(de)
                    except Exception:
                        pass

                mc = _raw(sd, "marketCap")
                if mc is not None:
                    try:
                        result["market_cap_inr"] = float(mc)
                    except Exception:
                        pass

                if result:
                    self._cache[sym]    = result
                    self._cache_ts[sym] = now
                    logger.debug("YahooFundamentals(%s): sector=%s de=%s mc=%s",
                                 sym,
                                 result.get("sector"),
                                 result.get("debt_equity_x100"),
                                 result.get("market_cap_inr"))
                    return result

            except Exception as exc:
                logger.debug("YahooFundamentalsClient.get(%s) [%s]: %s", sym, ticker, exc)

        return None


# ---------------------------------------------------------------------------
# 7. Unified Fundamentals Fetcher  (full multi-source priority chain)
# ---------------------------------------------------------------------------

class FundamentalsClient:
    """
    Returns fundamentals for a stock using Screener.in as the sole data source.

    All fields — sector, D/E, market cap, ROCE, ROE, promoter holding,
    sales/profit growth, PE, cash flow, etc. — are scraped from Screener.in.
    This ensures consistent results regardless of deployment environment
    (no dependency on Yahoo Finance crumbs or NSE live API availability).
    """

    def __init__(self, **kwargs):
        # Accept (and ignore) legacy alpha_key / apify_key / nse keyword args
        self._screener = ScreenerClient()

    def get(
        self,
        ticker: str,
        yf_fundamentals_fn=None,   # kept for backward compat — not used
    ) -> "tuple[str | None, float | None, float | None]":
        """
        Returns (sector, debt_equity_x100, market_cap_inr) from Screener.in.

        debt_equity_x100 : ratio × 100  (legacy format — 250 = D/E 2.5)
        market_cap_inr   : absolute INR (market_cap_cr × 1e7)
        Any value may be None if Screener.in did not return it.
        """
        sym = ticker.upper().replace(".NS", "").replace(".BO", "")

        sector:      "str | None"   = None
        debt_equity: "float | None" = None
        market_cap:  "float | None" = None

        try:
            sc = self._screener.get(sym)
            if sc:
                sector = sc.get("sector") or None
                if "debt_equity" in sc and sc["debt_equity"] is not None:
                    debt_equity = sc["debt_equity"] * 100   # → ×100 format
                if "market_cap_cr" in sc and sc["market_cap_cr"] is not None:
                    market_cap = sc["market_cap_cr"] * 1e7
        except Exception:
            pass

        return sector, debt_equity, market_cap

    def get_live_candle(self, nse_symbol: str) -> dict | None:
        """Live candle not available from Screener.in — returns None."""
        return None

    def get_extra_fundamentals(self, nse_symbol: str) -> dict:
        """
        Returns all enriched display + analysis fields from Screener.in.
        Keys (all optional): roce, roe, roe_5y, roe_10y, promoter_holding,
          fii_holding, dii_holding, public_holding, sales_growth_pct, sales_growth_5y,
          sales_growth_10y, profit_growth_3y, profit_growth_5y,
          profit_growth_10y, pe_ratio, book_value, dividend_yield,
          debt_equity, market_cap_cr, current_price, opm, net_profit_margin,
          face_value, eps, week52_high, week52_low, industry_pe,
          current_ratio, cash_from_operations
        """
        sym    = nse_symbol.upper().replace(".NS", "").replace(".BO", "")
        extras: dict = {}

        try:
            sc = self._screener.get(sym)
            if sc:
                for k in ("roce", "roe", "roe_5y", "roe_10y",
                          "promoter_holding", "fii_holding", "dii_holding", "public_holding",
                          "sales_growth_pct", "sales_growth_5y", "sales_growth_10y",
                          "profit_growth_3y", "profit_growth_5y", "profit_growth_10y",
                          "sales_growth_ttm", "profit_growth_ttm",
                          "pe_ratio", "book_value", "dividend_yield",
                          "debt_equity", "market_cap_cr", "current_price",
                          "current_ratio", "cash_from_operations",
                          "opm", "net_profit_margin", "face_value", "eps",
                          "week52_high", "week52_low", "industry_pe",
                          "pb_ratio", "peg_ratio", "quick_ratio", "interest_coverage"):
                    if k in sc and sc[k] is not None:
                        extras[k] = sc[k]
        except Exception:
            pass

        return extras

    def get_extra_sme_fundamentals(self, nse_symbol: str) -> dict:
        """
        Like get_extra_fundamentals() but uses SME-aware Screener.in scraping.

        Additionally tries the '-SME' URL variant used by NSE Emerge stocks
        (e.g. screener.in/company/TICKER-SME/) as a fallback when the regular
        URL returns no data.
        """
        sym    = nse_symbol.upper().replace(".NS", "").replace(".BO", "")
        extras: dict = {}

        try:
            sc = self._screener.get_sme(sym)
            if sc:
                for k in ("roce", "roe", "roe_5y", "roe_10y",
                          "promoter_holding", "fii_holding", "dii_holding", "public_holding",
                          "sales_growth_pct", "sales_growth_5y", "sales_growth_10y",
                          "profit_growth_3y", "profit_growth_5y", "profit_growth_10y",
                          "sales_growth_ttm", "profit_growth_ttm",
                          "pe_ratio", "book_value", "dividend_yield",
                          "debt_equity", "market_cap_cr", "current_price",
                          "current_ratio", "cash_from_operations",
                          "opm", "net_profit_margin", "face_value", "eps",
                          "week52_high", "week52_low", "industry_pe",
                          "pb_ratio", "peg_ratio", "quick_ratio", "interest_coverage"):
                    if k in sc and sc[k] is not None:
                        extras[k] = sc[k]
        except Exception:
            pass

        return extras


# ---------------------------------------------------------------------------
# Module-level singleton factory helpers
# ---------------------------------------------------------------------------

def _build_fundamentals_client() -> FundamentalsClient:
    """Create the fundamentals client (NSE live + Screener.in only)."""
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
