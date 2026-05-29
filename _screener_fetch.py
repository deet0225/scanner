import sys, warnings, re
warnings.filterwarnings('ignore')
import requests

s = requests.Session()
s.verify = False
s.headers.update({'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
url = 'https://www.screener.in/company/INFY/consolidated/'
r = s.get(url, timeout=20)

with open('_screener_infy.html', 'w', encoding='utf-8', errors='replace') as f:
    f.write(f'STATUS: {r.status_code}\n')
    f.write(r.text[:10000])

