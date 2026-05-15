"""
config.py -- All tunable parameters for the Nifty 500 Stock Scanner.

Active filter set  (MOMENTUM-FOCUSED swing — 1-10 day, 3-10 results)
----------------------------------------------------------------------
Philosophy: very soft liquidity gates (3 Cr avg TV, Z > 0.7, pct > 50) act
only as an illiquidity sanity check, while tight momentum criteria (RSI 62+,
weekly RSI 58+, ADX 25+, 4% market outperform, 3% sector outperform, closing
range 65%) are the real quality gate. Stocks that pass momentum will almost
always have adequate liquidity; the floor prevents completely dead counters.

Regime     : Nifty500 20 EMA > 50 EMA
             Nifty500 RSI(14) > 50 for ≥ REGIME_RSI_REQUIRE_DAYS of last 3 days
Technical  : Avg Traded Value 20D > Rs.3 Cr        (softened from 5 Cr)
             Rel Volume Percentile > 50             (softened from 50)
             Volume Z-Score > 0.7   (3-day avg)    (softened from 0.7)
             20 EMA > 50 EMA                        (daily uptrend)
             Weekly close > weekly 20 EMA          (REQUIRE_WEEKLY_EMA = True)
             Close <= 20 EMA + 2.0 x ATR14         (buy the setup, not the blow-off)
             Weekly RSI(14) > 58                    (tightened from 55)
             RSI(14) > 62  AND  RSI SMA(3) rising  (tightened from 58)
             Gap-up from prev close <= 4%           (avoid gap blow-offs)
             +DI > -DI  AND  ADX(14) > 25          (tightened from 22)
             Closing range >= 65%                   (tightened from 60%)
Fundamental: Market Cap > Rs.500 Cr  (info only; gate disabled)
             D/E < 3.0               (info only; gate disabled)
Momentum   : stock_ret20d - index_ret20d > 4%      (tightened from 3%)
             Stock 20D return > sector avg + 3%    (tightened from 2%)
Stop loss  : max(candle Low, Entry - 1.5 x ATR14)
"""

# --------------------------------------------------------------------------- #
# Fundamental filter thresholds
# --------------------------------------------------------------------------- #
MARKET_CAP_MIN          = 5_000_000_000    # Rs.500 Cr
DEBT_EQUITY_MAX         = 300.0            # D/E < 3.0  (Yahoo format: ratio x100)

# --------------------------------------------------------------------------- #
# Volume / traded-value thresholds
# --------------------------------------------------------------------------- #
# Rs.3 Cr avg daily traded value: minimum floor that still ensures a retail
# swing trader can enter/exit a position. The tight momentum gates (RSI 62+,
# ADX 25+, 4% market outperform, 65% closing range) are the real quality
# filter — the liquidity bar is just an exit-ability sanity check.
AVG_TRADED_VALUE_20D_MIN    = 30_000_000   # Rs.3 Cr  (softened from 5 Cr)
MEDIAN_TRADED_VALUE_20D_MIN = 10_000_000   # Rs.1 Cr  (softened from 2 Cr)

# 50th percentile = above-average volume day. The tight momentum criteria
# (RSI 62+, ADX 25+, closing range 65%) already weed out weak setups, so the
# volume bar can be the median level rather than the top-40% level.
REL_VOL_PERCENTILE_MIN      = 50           # softened from 60
# 0.7 standard deviations above baseline. Steady institutional accumulation in
# momentum leaders often shows consistent 0.5–1.0σ volume — not sudden spikes.
VOLUME_ZSCORE_MIN           = 0.7          # softened from 1.0

# Number of recent candles to average for volume checks (Filters 3 & 12a).
# Using 3 days instead of 1 eliminates single-candle noise and handles intraday
# scans where today's volume is still accumulating (incomplete candle would make
# every stock fail a single-candle volume check).
VOLUME_LOOKBACK_DAYS = 3

# --------------------------------------------------------------------------- #
# Volatility / ATR thresholds
# --------------------------------------------------------------------------- #
ATR_RATIO_MAX       = 0.88
# 2.0×ATR ceiling: buy the setup near the EMA, not the overextended blow-off.
# A stock within 2 ATRs of its 20 EMA still has room to run; beyond that the
# risk:reward deteriorates rapidly for a 1-10 day hold.
EMA_ATR_MULTIPLIER  = 2.0    # was 2.5
REQUIRE_EMA_ATR_CEILING = True

# --------------------------------------------------------------------------- #
# Structural filter toggles
# --------------------------------------------------------------------------- #
REQUIRE_HH20_BREAKOUT    = False  # keep off — allows pullback-to-EMA entries
REQUIRE_ATR_CONTRACTION  = False  # keep off — strong trends have expanding ATR
REQUIRE_RSI_SMA3_RISING  = True   # ON — RSI must be accelerating, not flat/falling
                                   # Eliminates stocks where RSI > 58 but momentum stalling

REQUIRE_MEDIAN_TV_20D   = False
REQUIRE_CLOSING_RANGE   = True    # ON — close must be in upper 60% of day's range
                                   # Stocks closing in the lower half signal weak hands
REQUIRE_MEDIAN_TV_TREND = False
REQUIRE_PRICE_PROXIMITY = False

# --------------------------------------------------------------------------- #
# Swing-trade early-entry / confirmation flags
# --------------------------------------------------------------------------- #
REQUIRE_WEEKLY_EMA    = True   # ON — weekly close must be above weekly 20 EMA.
                                #   Ensures the larger weekly trend is fully confirmed.
                                #   Combined with weekly RSI > 55 this filters out
                                #   stocks in weekly downtrends making day-trade bounces.
REQUIRE_RS_UPTREND    = False  # keep off — 3% momentum outperform already covers RS
REQUIRE_ADX_THRESHOLD = True   # ON — ADX > 22 (established, not just starting trend)
REQUIRE_FUNDAMENTALS  = False  # keep off — swing trade, price action matters

# --------------------------------------------------------------------------- #
# RSI thresholds
# --------------------------------------------------------------------------- #
# RSI 62+ = stock is clearly in a strong momentum phase with room to continue.
# Weekly RSI 58+ = weekly chart is in a firmly bullish momentum phase.
RSI_MIN        = 62   # tightened from 58
WEEKLY_RSI_MIN = 58   # tightened from 55

# --------------------------------------------------------------------------- #
# ADX threshold
# --------------------------------------------------------------------------- #
# ADX > 25 = clearly directional trend with conviction (not boundary zone).
ADX_MIN = 25   # tightened from 22

# --------------------------------------------------------------------------- #
# Momentum / RS thresholds
# --------------------------------------------------------------------------- #
# 4% outperformance = stock is a clear market leader, not just keeping pace.
MOMENTUM_OUTPERFORM_MIN = 0.04   # tightened from 0.03 (4% above index over 20 days)
# 3% above sector average = dominant name within its sector, not just top-quartile.
SECTOR_OUTPERFORM_MIN   = 3.0    # tightened from 2.0

# --------------------------------------------------------------------------- #
# Price structure thresholds
# --------------------------------------------------------------------------- #
# 65% closing range = close in upper 35% of the day's High-Low range.
# Stronger threshold confirms dominant intraday buying and institutional accumulation.
CLOSING_RANGE_MIN   = 0.65   # tightened from 0.60
PRICE_PROXIMITY_MAX = 0.30
# 4% gap-up limit: larger gaps often fill within 1-5 days, creating immediate
# stop-out risk for swing traders entering on the gap day.
GAP_UP_MAX          = 0.04   # was 0.05

# --------------------------------------------------------------------------- #
# Regime check behaviour
# --------------------------------------------------------------------------- #
REGIME_ABORT_ON_FAIL    = False
REGIME_RSI_REQUIRE_DAYS = 2

# --------------------------------------------------------------------------- #
# Indicator periods
# --------------------------------------------------------------------------- #
RSI_PERIOD           = 14
EMA_PERIOD           = 50
EMA_SHORT_PERIOD     = 20
ADX_PERIOD           = 14
VOLUME_AVG_DAYS      = 20
RETURN_3M_DAYS       = 63
SECTOR_LOOKBACK_DAYS = 20

# --------------------------------------------------------------------------- #
# Scoring weights (must sum to 1.0)
# --------------------------------------------------------------------------- #
# Rebalanced for momentum-first 3-10 stock output:
#   Momentum vs market is the strongest predictor of 1-10 day continuation.
#   RS vs sector separates leaders from coat-tailers in a rising sector.
#   ADX/trend strength eliminates false breakouts.
#   Volume weight reduced since liquidity gates are relaxed.
WEIGHT_VOLUME   = 0.15   # reduced from 0.20 (liquidity gate relaxed)
WEIGHT_RSI      = 0.10
WEIGHT_EMA      = 0.08
WEIGHT_ADX      = 0.20   # slight reduction from 0.22
WEIGHT_MOMENTUM = 0.32   # increased from 0.27 (momentum is the primary signal)
WEIGHT_RS       = 0.15   # increased from 0.13 (sector leadership matters more)

# --------------------------------------------------------------------------- #
# Scoring normalisation caps
# --------------------------------------------------------------------------- #
# Tighter caps → better score differentiation within the filtered group.
# Z-score 3 = max volume score  (was 5 — too wide a range to differentiate)
# 5% outperformance = full momentum score  (was 10 — targets are closer to 3-5%)
VOLUME_SCORE_CAP   = 3.0    # was 5 — full score at Z=3 (better differentiation)
EMA_PCT_SCORE_CAP  = 5.0    # was 6 — full penalty at 5% above EMA
ADX_SCORE_CAP      = 35.0   # was 40
RS_SCORE_CAP       = 0.08   # was 0.10 — RS ratio excess cap tightened
MOMENTUM_SCORE_CAP = 5.0    # was 10 — max score at 5% outperformance (was 10%)

# --------------------------------------------------------------------------- #
# Scan behaviour
# --------------------------------------------------------------------------- #
SCAN_INTERVAL_MINUTES = 15
HIST_DAYS             = 600
TOP_N                 = 10   # show top 10 — with tight filters typically 1-5 survive
MIN_DATA_ROWS         = 120
CACHE_UPDATE_DAYS     = 75
AUTO_RESCAN           = False

# --------------------------------------------------------------------------- #
# Download / API settings
# --------------------------------------------------------------------------- #
DOWNLOAD_THREADS     = 20
DOWNLOAD_BATCH_SIZE  = 100
FUNDAMENTALS_THREADS = 5
FUNDAMENTALS_DELAY   = 0.0
DOWNLOAD_THROTTLE    = 0
CRUMB_TTL            = 3500

# --------------------------------------------------------------------------- #
# Market benchmark  (Nifty 500 scanner)
# --------------------------------------------------------------------------- #
MARKET_BENCHMARK_TICKER   = "^CRSLDX"
MARKET_BENCHMARK_ETF_FALLBACKS = [
    "NIFTYBEES.NS",
    "ICICINIFTY.NS",
    "MOM50.NS",
]

# --------------------------------------------------------------------------- #
# Microcap 250 benchmark
# --------------------------------------------------------------------------- #
MICROCAP_BENCHMARK_TICKER = "^CNXMC250"
MICROCAP_BENCHMARK_ETF_FALLBACKS = [
    "MICROCAP.NS",
    "MAFSETF.NS",
    "NIFTYBEES.NS",
    "ICICINIFTY.NS",
]

# ETF fallback for each sector index
SECTOR_INDEX_ETF_FALLBACKS = {
    "^CNXBANK":    ["BANKBEES.NS",   "ICICIB22.NS"],
    "^CNXIT":      ["ITBEES.NS",     "ITETF.NS"],
    "^CNXPHARMA":  ["PHARMABEES.NS"],
    "^CNXAUTO":    ["AUTOBEES.NS"],
    "^CNXFMCG":    ["FMCGIETF.NS"],
    "^CNXENERGY":  ["ENERGIETF.NS"],
    "^CNXINFRA":   ["INFRABEES.NS",  "INFRAIETF.NS"],
    "^CNXMETAL":   ["METALBEES.NS",  "METAL.NS"],
    "^CNXREALTY":  ["REALTYBEES.NS"],
    "^CNXMEDIA":   [],
    "^CNXSERVICE": [],
    "^CNXCMDT":    [],
    "^CNXFINANCE": ["FINIETF.NS"],
    "^CNXPSE":     [],
    "^CRSLDX":     ["NIFTYBEES.NS",  "ICICINIFTY.NS"],
}

SECTOR_INDEX_MAP = {
    "Technology":                     "^CNXIT",
    "Information Technology":         "^CNXIT",
    "Software":                       "^CNXIT",
    "Financial Services":             "^CNXFINANCE",
    "Banks":                          "^CNXBANK",
    "Banking":                        "^CNXBANK",
    "Insurance":                      "^CNXFINANCE",
    "Asset Management":               "^CNXFINANCE",
    "Capital Markets":                "^CNXFINANCE",
    "Credit Services":                "^CNXFINANCE",
    "Healthcare":                     "^CNXPHARMA",
    "Pharmaceuticals":                "^CNXPHARMA",
    "Biotechnology":                  "^CNXPHARMA",
    "Drug Manufacturers - General":   "^CNXPHARMA",
    "Drug Manufacturers":             "^CNXPHARMA",
    "Medical Devices":                "^CNXPHARMA",
    "Diagnostics & Research":         "^CNXPHARMA",
    "Health Information Services":    "^CNXPHARMA",
    "Consumer Cyclical":              "^CNXAUTO",
    "Automobile":                     "^CNXAUTO",
    "Auto":                           "^CNXAUTO",
    "Auto Parts":                     "^CNXAUTO",
    "Auto Components":                "^CNXAUTO",
    "Textile":                        "^CRSLDX",
    "Apparel Manufacturing":          "^CRSLDX",
    "Apparel Retail":                 "^CRSLDX",
    "Apparel":                        "^CRSLDX",
    "Specialty Retail":               "^CRSLDX",
    "Leisure":                        "^CRSLDX",
    "Hotels":                         "^CRSLDX",
    "Hospitality":                    "^CRSLDX",
    "Restaurants":                    "^CRSLDX",
    "Gambling":                       "^CRSLDX",
    "Personal Products":              "^CNXFMCG",
    "Consumer Electronics":           "^CRSLDX",
    "Consumer Defensive":             "^CNXFMCG",
    "FMCG":                           "^CNXFMCG",
    "Packaged Foods":                 "^CNXFMCG",
    "Beverages - Non-Alcoholic":      "^CNXFMCG",
    "Beverages - Alcoholic":          "^CNXFMCG",
    "Tobacco":                        "^CNXFMCG",
    "Household & Personal Products":  "^CNXFMCG",
    "Agricultural":                   "^CNXFMCG",
    "Food Distribution":              "^CNXFMCG",
    "Energy":                         "^CNXENERGY",
    "Oil & Gas":                      "^CNXENERGY",
    "Oil & Gas Integrated":           "^CNXENERGY",
    "Oil & Gas Exploration & Production": "^CNXENERGY",
    "Oil & Gas Refining & Marketing": "^CNXENERGY",
    "Oil & Gas Equipment & Services": "^CNXENERGY",
    "Power":                          "^CNXENERGY",
    "Utilities":                      "^CNXENERGY",
    "Renewable Utilities":            "^CNXENERGY",
    "Regulated Electric":             "^CNXENERGY",
    "Diversified Utilities":          "^CNXENERGY",
    "Independent Power Producers":    "^CNXENERGY",
    "Industrials":                    "^CNXINFRA",
    "Infrastructure":                 "^CNXINFRA",
    "Cement":                         "^CNXINFRA",
    "Construction":                   "^CNXINFRA",
    "Engineering & Construction":     "^CNXINFRA",
    "Capital Goods":                  "^CNXINFRA",
    "Industrial Machinery":           "^CNXINFRA",
    "Specialty Industrial Machinery": "^CNXINFRA",
    "Electrical Equipment":           "^CNXINFRA",
    "Electronic Components":          "^CNXINFRA",
    "Aerospace & Defense":            "^CNXINFRA",
    "Building Products & Equipment":  "^CNXINFRA",
    "Integrated Freight & Logistics": "^CNXINFRA",
    "Trucking":                       "^CNXINFRA",
    "Marine Shipping":                "^CNXINFRA",
    "Railroads":                      "^CNXINFRA",
    "Waste Management":               "^CNXINFRA",
    "Basic Materials":                "^CNXMETAL",
    "Metals & Mining":                "^CNXMETAL",
    "Mining":                         "^CNXMETAL",
    "Steel":                          "^CNXMETAL",
    "Aluminum":                       "^CNXMETAL",
    "Copper":                         "^CNXMETAL",
    "Other Industrial Metals & Mining": "^CNXMETAL",
    "Chemicals":                      "^CNXCMDT",
    "Commodities":                    "^CNXCMDT",
    "Specialty Chemicals":            "^CNXCMDT",
    "Agricultural Inputs":            "^CNXCMDT",
    "Fertilizers":                    "^CNXCMDT",
    "Coking Coal":                    "^CNXCMDT",
    "Paper & Paper Products":         "^CNXCMDT",
    "Real Estate":                    "^CNXREALTY",
    "Realty":                         "^CNXREALTY",
    "Real Estate - Development":      "^CNXREALTY",
    "Real Estate - Services":         "^CNXREALTY",
    "Communication Services":         "^CNXMEDIA",
    "Media":                          "^CNXMEDIA",
    "Telecom":                        "^CNXMEDIA",
    "Telecommunications":             "^CNXMEDIA",
    "Telecommunication Services":     "^CNXMEDIA",
    "Broadcasting":                   "^CNXMEDIA",
    "Entertainment":                  "^CNXMEDIA",
    "Services":                       "^CNXSERVICE",
    "Diversified":                    "^CNXSERVICE",
    "Conglomerates":                  "^CNXSERVICE",
    "Staffing & Employment Services": "^CNXSERVICE",
    "IT Services":                    "^CNXIT",
    "Public Sector":                  "^CNXPSE",
    "PSU":                            "^CNXPSE",
    "Unknown":                        "^CRSLDX",
}

SECTOR_FALLBACK_TO_MARKET = True

# --------------------------------------------------------------------------- #
# Multi-source Data API Keys
# --------------------------------------------------------------------------- #
import os

ALPHA_VANTAGE_API_KEY   = os.getenv("ALPHA_VANTAGE_API_KEY", "")
APIFY_API_KEY           = os.getenv("APIFY_API_KEY", "")
APIFY_SCREENER_ACTOR_ID = os.getenv("APIFY_SCREENER_ACTOR_ID",
                                     "emastra~screener-stock-data-scraper")
TRADINGVIEW_USERNAME = os.getenv("TRADINGVIEW_USERNAME", "")
TRADINGVIEW_PASSWORD = os.getenv("TRADINGVIEW_PASSWORD", "")

# --------------------------------------------------------------------------- #
# Source enable / disable flags
# --------------------------------------------------------------------------- #
ENABLE_TRADINGVIEW     = True
ENABLE_ALPHA_VANTAGE   = True
ENABLE_APIFY_SCREENER  = True
ENABLE_NSE_PYTHON_HIST = True
