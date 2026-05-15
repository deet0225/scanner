"""Test nsepython deeper - quote_equity full structure and screener.in fundamentals."""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import warnings, json, requests
warnings.filterwarnings("ignore")

import nsepython as nse

print("=== Full quote_equity for RELIANCE ===")
try:
    q = nse.quote_equity("RELIANCE")
    # priceInfo
    pi = q.get("priceInfo", {})
    print("priceInfo:", json.dumps({k: v for k,v in pi.items() if not isinstance(v, dict)}, indent=2))
    # industryInfo
    ii = q.get("industryInfo", {})
    print("industryInfo:", json.dumps(ii, indent=2))
    # securityInfo
    si = q.get("securityInfo", {})
    print("securityInfo:", json.dumps({k: v for k,v in si.items() if not isinstance(v, dict)}, indent=2, default=str))
    # metadata
    md = q.get("metadata", {})
    print("metadata:", json.dumps({k: v for k,v in md.items() if not isinstance(v, (dict, list))}, indent=2, default=str))
except Exception as e:
    print("error:", e)

print()
print("=== NSE API for market cap (indices) ===")
try:
    # Try to get market cap from NSE equity info API
    import requests, ssl
    ssl._create_default_https_context = ssl._create_unverified_context
    session = requests.Session()
    session.verify = False
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Referer": "https://www.nseindia.com/",
    })
    session.get("https://www.nseindia.com", timeout=10)
    r = session.get("https://www.nseindia.com/api/quote-equity?symbol=RELIANCE", timeout=15)
    data = r.json()
    print("marketDeptOrderBook:", data.get("marketDeptOrderBook", {}).get("tradeInfo", {}))
except Exception as e:
    print("NSE API error:", e)

print()
print("=== Screener.in fundamentals (RELIANCE) ===")
try:
    import requests, ssl
    ssl._create_default_https_context = ssl._create_unverified_context
    s2 = requests.Session()
    s2.verify = False
    s2.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml",
    })
    r2 = s2.get("https://www.screener.in/company/RELIANCE/", timeout=20)
    print("HTTP status:", r2.status_code)
    # Look for market cap in HTML
    import re
    mc_match = re.search(r'Market Cap.*?<span[^>]*>([\d,\.]+)', r2.text, re.DOTALL | re.IGNORECASE)
    print("Market cap snippet:", mc_match.group(0)[:100] if mc_match else "Not found")
    de_match = re.search(r'Debt to equity.*?<span[^>]*>([\d,\.]+)', r2.text, re.DOTALL | re.IGNORECASE)
    print("D/E snippet:", de_match.group(0)[:100] if de_match else "Not found")
except Exception as e:
    print("Screener.in error:", e)

