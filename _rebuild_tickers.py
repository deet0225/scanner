"""
_rebuild_tickers.py -- Fetches full Nifty 500 list from NSE India and
writes tickers.py.  Run manually: python _rebuild_tickers.py
"""
import requests, warnings, sys

warnings.filterwarnings("ignore")

def fetch_nifty500():
    s = requests.Session()
    s.verify = False
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nseindia.com/",
        "Accept-Language": "en-US,en;q=0.9",
    })
    # Warm-up request to get cookies
    s.get("https://www.nseindia.com", timeout=15)
    r = s.get(
        "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500",
        timeout=20,
    )
    r.raise_for_status()
    stocks = r.json().get("data", [])
    symbols = sorted(set(
        x["symbol"].strip()
        for x in stocks
        if x.get("symbol") and x["symbol"] != "NIFTY 500"
    ))
    return [sym + ".NS" for sym in symbols]


def write_tickers(tickers, path="tickers.py"):
    header = (
        '"""\n'
        "tickers.py -- Full Nifty 500 constituent list (Yahoo Finance .NS format).\n"
        "Auto-generated from NSE India API. Refresh quarterly by running:\n"
        "  python _rebuild_tickers.py\n"
        '"""\n\n'
        "NIFTY500_TICKERS = [\n"
    )
    rows = []
    for i in range(0, len(tickers), 8):
        chunk = tickers[i : i + 8]
        rows.append("    " + ", ".join('"' + t + '"' for t in chunk) + ",")
    footer = "]\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(header + "\n".join(rows) + "\n" + footer)


if __name__ == "__main__":
    print("Fetching Nifty 500 from NSE India...")
    tickers = fetch_nifty500()
    print(f"Got {len(tickers)} tickers")
    out = sys.argv[1] if len(sys.argv) > 1 else "tickers.py"
    write_tickers(tickers, out)
    print(f"Written to {out}")

