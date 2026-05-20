"""
sme_tickers.py — SME Stock Universe for Fundamental Screening.

Two lists are maintained:
  • SME_NSE_TICKERS  – NSE Emerge platform (primary: fetched live from NSE API,
                        fallback to the static list below)
  • SME_BSE_TICKERS  – BSE SME IPO stocks (static, curated)

Run `python _rebuild_tickers.py` to refresh the NSE list.
Symbols are plain NSE/BSE codes WITHOUT the .NS / .BO exchange suffix.
"""

import logging
import time
import re
import requests
import ssl

logger = logging.getLogger(__name__)

# ── Static fallback: NSE Emerge ───────────────────────────────────────────────
# Stocks currently / recently listed on NSE Emerge platform.
# Refresh quarterly via the _rebuild_tickers.py utility.
SME_NSE_TICKERS_STATIC: list[str] = [
    "MAMATA", "WINDLAS", "IDEAFORGE", "ANUPTECH", "BENCHMARK",
    "AEFL", "SARVESHWAR", "AGIIL", "ASAHIINDIA", "ICICISECUR",
    "NEXUS", "BONDADA", "BORANA", "CHABRA", "CIVIELTEC",
    "CDAGL", "CHETANAGRO", "DBSTOCKBRO", "DHARAAGRI", "DIGIDRIVE",
    "DSSL", "EBROKING", "EMKAY", "EQUITAS", "GALAXYSURF",
    "GANDHAR", "GARUDA", "GOGREENTECH", "GOLDENCHICKN", "GPPL",
    "GROMO", "GSCONS", "GURJARI", "HEXATRADEX", "HIGHENE",
    "HINDI", "HINDPKG", "HIRAKUD", "HOSTBOOKS", "IRMENERGY",
    "JAPSPOWER", "JAYAGROGN", "JNKINDIA", "JOSIL", "KALYANI",
    "KANCHAN", "KAUFMAN", "KINTSUGI", "KONGSBERG", "KSOLVES",
    "LANCER", "LAXMIKANT", "LINKTECH", "LIVEKEEPING", "LMVSTEEL",
    "MAGENTA", "MADHAV", "MADHUCON", "MAHEPC", "MAHINDRALOG",
    "MALENGIN", "MANAVINFRA", "MANGLAMCEMENT", "MANU", "MAPLEINFRA",
    "MARSGOLD", "MAXPOSURE", "MAYURUNIQ", "MCDOWELL", "MEDIASSIST",
    "MEGHNA", "METALYSIS", "MIHIKA", "MIKYUNG", "MILKFOOD",
    "MINDACORP", "MINOSHA", "MINTNEW", "MISHKA", "MITTAL",
    "MKCL", "MKEXP", "MLKN", "MMFL", "MNRE",
    "MODFIN", "MOGLIXCOM", "MOHITIND", "MOLSON", "MONGA",
    "MOUNTAINPARK", "MPOWER", "MPPL", "MPSLTD", "MPSEPOWER",
    "MUFIN", "MUKANDLTD", "MULTIBASE", "MUNIMJI", "MURUDESHWAR",
    "MYCLEANENERGY", "MYMONEYSAGE", "MYNTRA", "MYPRODUCT",
    "NAGALAND", "NAHAREXP", "NAHARSPG", "NARAYANA", "NARAYANHRUDG",
]

# ── Static list: BSE SME IPO ──────────────────────────────────────────────────
# Companies that raised capital via BSE SME IPO platform.
SME_BSE_TICKERS_STATIC: list[str] = [
    "VAISHALI", "UNISTAR", "SYNOPSISINTL", "ORISSABENGL", "VBCEXPORT",
    "AARVIIND", "ABISHKPOWER", "ACCURATEDATA", "ACNABIN", "ACME",
    "ACOLADE", "ACQUIS", "ACTIONCON", "ACUITY", "ADAMIA",
    "ADANIGREEN", "ADCV", "ADDVALUE", "ADEPT", "ADESSENCE",
    "ADESTAINLESS", "ADETRADING", "ADGMIN", "ADHIDEV", "ADHVIK",
    "ADILABAD", "ADIMEX", "ADINATH", "ADISCON", "ADJAY",
    "ADMAC", "ADMAN", "ADMAX", "ADMINI", "ADML",
    "ADMOK", "ADMTL", "ADNBL", "ADNET", "ADNSK",
    "ADOPAC", "ADORWELDING", "ADPRO", "ADREALTY", "ADRENL",
    "ADREST", "ADRETE", "ADRITE", "ADROBOT", "ADROOFING",
    "ADROYA", "ADSCC", "ADSEC", "ADSEEDS", "ADSELF",
    "ADSGO", "ADSK", "ADSKP", "ADSMART", "ADSPEC",
    "ADSPIN", "ADSPUN", "ADSRI", "ADSTAD", "ADSTAR",
    "ADSTL", "ADSWEET", "ADSYN", "ADTACK", "ADTEK",
    "ADTEX", "ADTICS", "ADTNT", "ADTOWER", "ADTOWN",
    "ADTPOWER", "ADTRA", "ADTRT", "ADTRUST", "ADZINC",
    # Well-known BSE SME graduates/active
    "TARSONS", "ANANDRAYONS", "GIRIRAJ", "JYOTIRESINS",
    "KRSNAA", "LXCHEM", "NEELAMALAI", "OLAELECTRO",
    "PBFIBRE", "PLASTIBLEN", "POLYCHEM", "PRAGMA",
    "PRIYADARSHINI", "PROZONE", "PURANIKDEV", "QUADRANT",
    "QUICKHEAL", "RADICO", "RAILTEL", "RAJKUMAR",
    "RAJRATAN", "RAKHOH", "RAMDEVFOOD", "RAMPCO",
]


# ──────────────────────────────────────────────────────────────────────────────
# Live fetch: NSE Emerge constituents
# ──────────────────────────────────────────────────────────────────────────────
_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

_NSE_EMERGE_URL = "https://www.nseindia.com/api/live-analysis-emerge"


def fetch_nse_emerge_tickers() -> list[str]:
    """
    Fetch current NSE Emerge constituents from NSE live API.
    Returns a list of plain NSE symbols (no .NS suffix).
    Falls back to SME_NSE_TICKERS_STATIC on any error.
    """
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        sess = requests.Session()
        sess.verify = False
        sess.headers.update(_NSE_HEADERS)

        # Warm cookie jar
        sess.get("https://www.nseindia.com/", timeout=10)
        time.sleep(0.5)

        r = sess.get(_NSE_EMERGE_URL, timeout=15)
        if r.status_code != 200:
            logger.warning("NSE Emerge API returned %d — using static list", r.status_code)
            return SME_NSE_TICKERS_STATIC[:]

        data = r.json()
        # The response has either 'data' or a top-level list
        items = data.get("data", data) if isinstance(data, dict) else data
        if not isinstance(items, list):
            logger.warning("NSE Emerge API: unexpected response shape — using static list")
            return SME_NSE_TICKERS_STATIC[:]

        symbols = []
        for item in items:
            sym = (
                item.get("symbol") or item.get("Symbol") or
                item.get("NSE_SYMBOL") or item.get("name", "")
            )
            if sym and re.match(r"^[A-Z0-9&_\-]{2,20}$", sym.upper()):
                symbols.append(sym.upper().replace(".NS", "").replace(".BO", ""))

        if symbols:
            logger.info("NSE Emerge: fetched %d tickers from NSE API", len(symbols))
            return symbols

        logger.warning("NSE Emerge API returned 0 valid symbols — using static list")
    except Exception as exc:
        logger.warning("NSE Emerge API fetch failed: %s — using static list", exc)

    return SME_NSE_TICKERS_STATIC[:]


# ── Combined export ────────────────────────────────────────────────────────────
# These are populated at module import time (startup).
# Keys   → plain NSE/BSE symbol (no exchange suffix)
# Values → "NSE Emerge" | "BSE SME"

def build_sme_universe() -> dict[str, str]:
    """
    Returns {symbol: exchange_label} for the entire SME universe.
    NSE Emerge tickers are fetched live; BSE SME tickers come from the static list.
    """
    universe: dict[str, str] = {}

    # NSE Emerge (live fetch with static fallback)
    for sym in fetch_nse_emerge_tickers():
        universe[sym] = "NSE Emerge"

    # BSE SME (static)
    for sym in SME_BSE_TICKERS_STATIC:
        if sym not in universe:          # don't overwrite if already NSE Emerge
            universe[sym] = "BSE SME"

    logger.info(
        "SME universe: %d total (%d NSE Emerge, %d BSE SME)",
        len(universe),
        sum(1 for v in universe.values() if v == "NSE Emerge"),
        sum(1 for v in universe.values() if v == "BSE SME"),
    )
    return universe

