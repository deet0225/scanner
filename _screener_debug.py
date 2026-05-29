import sys, warnings, re
warnings.filterwarnings('ignore')
import requests

s = requests.Session()
s.verify = False
s.headers.update({'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
url = 'https://www.screener.in/company/INFY/consolidated/'
print('Fetching', url)
r = s.get(url, timeout=20)
print('status:', r.status_code)
html = r.text
print('html len:', len(html))

# Find sector/industry info
idx = html.find('/screen')
print('first /screen at:', idx)
if idx >= 0:
    snippet = html[max(0, idx-300):idx+400]
    print('SNIPPET:', snippet)

# Try to find sector/industry text
for kw in ['sector', 'industry', 'Sector', 'Industry', 'SECTOR']:
    i2 = html.lower().find(kw.lower())
    if i2 >= 0:
        print(f'\nFound "{kw}" at {i2}:')
        print(html[max(0,i2-50):i2+150])
        break

