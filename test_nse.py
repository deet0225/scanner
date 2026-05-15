"""Test nsepython API availability and data quality."""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import warnings, json
warnings.filterwarnings("ignore")

import nsepython as nse

print("=== Testing quote_equity (RELIANCE) ===")
try:
    q = nse.quote_equity("RELIANCE")
    print("Type:", type(q))
    if isinstance(q, dict):
        print("Top keys:", list(q.keys())[:10])
        pi = q.get("priceInfo", {})
        print("lastPrice:", pi.get("lastPrice"))
        print("open:", pi.get("open"))
        print("intraDayHighLow:", pi.get("intraDayHighLow"))
        meta = q.get("metadata", {}) or q.get("info", {})
        print("meta keys:", list(meta.keys())[:10])
except Exception as e:
    print("quote_equity error:", e)

print()
print("=== Testing nse_eq (RELIANCE) ===")
try:
    q2 = nse.nse_eq("RELIANCE")
    print("Type:", type(q2))
    if isinstance(q2, dict):
        print("Top keys:", list(q2.keys())[:10])
except Exception as e:
    print("nse_eq error:", e)

print()
print("=== Testing equity_history (RELIANCE) ===")
try:
    df = nse.equity_history("RELIANCE", "EQ", "01-04-2026", "13-05-2026")
    print("Type:", type(df))
    if hasattr(df, "shape"):
        print("Shape:", df.shape)
        print("Columns:", df.columns.tolist())
        print(df.tail(3).to_string())
except Exception as e:
    print("equity_history error:", e)

print()
print("=== Testing nse_quote_ltp (RELIANCE) ===")
try:
    ltp = nse.nse_quote_ltp("RELIANCE")
    print("LTP:", ltp)
except Exception as e:
    print("nse_quote_ltp error:", e)

