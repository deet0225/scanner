"""
_gen_sme_static.py -- Fetches live NSE Emerge tickers and prints them
formatted as a Python list literal for pasting into sme_tickers.py.
Run: python _gen_sme_static.py
"""
import warnings
warnings.filterwarnings("ignore")
from sme_tickers import fetch_nse_emerge_tickers

live = fetch_nse_emerge_tickers()
live_sorted = sorted(live)
print(f"# {len(live_sorted)} NSE Emerge tickers fetched from live NSE API")
for i in range(0, len(live_sorted), 5):
    chunk = live_sorted[i:i+5]
    line = "    " + ", ".join('"' + t + '"' for t in chunk) + ","
    print(line)
print(f"\n# Total: {len(live_sorted)}")

