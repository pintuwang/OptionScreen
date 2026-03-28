"""
fetch_options.py
Yahoo Finance requires a crumb token since 2023.
Flow: visit finance.yahoo.com to set cookies, fetch crumb, then append
?crumb=xxx to every API call.
"""
import os, json, math, time, datetime, requests

TICKER      = os.environ.get('TICKER',      'MSTR').upper()
OPTION_TYPE = os.environ.get('OPTION_TYPE', 'call').lower()
STRIKE      = float(os.environ.get('STRIKE', '400'))
RANGE       = float(os.environ.get('RANGE',  '5'))

HEADERS = {
    'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Accept':          'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Origin':          'https://finance.yahoo.com',
    'Referer':         'https://finance.yahoo.com/',
}

S = requests.Session()
S.headers.update(HEADERS)

# ── Step 1: get crumb ─────────────────────────────────────────────────────
print('Fetching Yahoo Finance crumb...')
# Visit homepage to set consent cookies
S.get('https://finance.yahoo.com', timeout=15)
time.sleep(1.5)

crumb = None
for crumb_url in [
    'https://query2.finance.yahoo.com/v1/test/getcrumb',
    'https://query1.finance.yahoo.com/v1/test/getcrumb',
]:
    try:
        r = S.get(crumb_url, timeout=15)
        text = r.text.strip()
        if text and '<' not in text and len(text) < 50:
            crumb = text
            print(f'  Got crumb: {crumb[:12]}...')
            break
    except Exception as e:
        print(f'  Crumb attempt failed: {e}')

if not crumb:
    raise RuntimeError('Could not obtain Yahoo Finance crumb. Try re-running the workflow.')


def get(url, retries=3):
    sep      = '&' if '?' in url else '?'
    full_url = f'{url}{sep}crumb={crumb}'
    for i in range(retries):
        try:
            r = S.get(full_url, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f'  attempt {i+1} failed: {e}')
            if i < retries - 1:
                time.sleep(2 ** i)
    raise RuntimeError(f'Failed after {retries} attempts: {url}')


def calc_hv(closes, w=30):
    if len(closes) < w + 1:
        return []
    hvs = []
    for i in range(w, len(closes)):
        sl = closes[i - w: i + 1]
        lr = [math.log(sl[j] / sl[j-1]) for j in range(1, len(sl)) if sl[j] > 0 and sl[j-1] > 0]
        if len(lr) < 2:
            continue
        mean = sum(lr) / len(lr)
        v    = sum((x - mean) ** 2 for x in lr) / (len(lr) - 1)
        hvs.append(math.sqrt(v * 252))
    return hvs


def pctile(arr, val):
    return round(sum(1 for x in arr if x <= val) / len(arr) * 100) if arr else 0


# ── Step 2: price history + HV ────────────────────────────────────────────
print(f'\nFetching 1-year history for {TICKER}...')
ch     = get(f'https://query1.finance.yahoo.com/v8/finance/chart/{TICKER}?interval=1d&range=1y')
res    = ch['chart']['result'][0]
closes = [c for c in res['indicators']['quote'][0].get('close', []) if c]
current_price = res['meta'].get('regularMarketPrice', 0)
print(f'  Price: ${current_price:.2f}  |  Closes: {len(closes)}')

hv_ctx = None
if len(closes) >= 35:
    hvs = calc_hv(closes, 30)
    if hvs:
        cur    = hvs[-1]
        hv_ctx = {
            'currentHV': round(cur, 6),
            'hvPct':     pctile(hvs, cur),
            'hvMin':     round(min(hvs), 6),
            'hvMax':     round(max(hvs), 6),
        }
        print(f'  HV30: {cur*100:.1f}%  |  Percentile: {hv_ctx["hvPct"]}th')


# ── Step 3: expiry dates ──────────────────────────────────────────────────
print(f'\nFetching expiry dates for {TICKER}...')
od       = get(f'https://query2.finance.yahoo.com/v7/finance/options/{TICKER}')
ores     = od['optionChain']['result'][0]
if not current_price:
    current_price = ores['quote'].get('regularMarketPrice', 0)
expiries = ores.get('expirationDates', [])
print(f'  Found {len(expiries)} expiry dates')


# ── Step 4: fetch each expiry ─────────────────────────────────────────────
today     = datetime.date.today()
contracts = []

for i, ts in enumerate(expiries):
    exp  = datetime.date.fromtimestamp(ts)
    dte  = max(1, (exp - today).days)
    estr = exp.isoformat()
    print(f'  [{i+1}/{len(expiries)}] {estr}  DTE={dte}', end='  ')
    try:
        d    = get(f'https://query2.finance.yahoo.com/v7/finance/options/{TICKER}?date={ts}')
        r    = d['optionChain']['result'][0]
        rows = r['options'][0].get('calls' if OPTION_TYPE == 'call' else 'puts', [])
        hit  = [o for o in rows if o.get('strike') and abs(o['strike'] - STRIKE) <= RANGE]
        print(f'{len(hit)} matched')
        for o in hit:
            bid = o.get('bid') or 0
            ask = o.get('ask') or 0
            mid = (bid + ask) / 2
            iv  = o.get('impliedVolatility')
            if iv and iv > 5:
                iv = iv / 100
            contracts.append({
                'option_type':      OPTION_TYPE,
                'expiration_date':  estr,
                'dte':              dte,
                'strike':           o['strike'],
                'bid':              round(bid, 4),
                'ask':              round(ask, 4),
                'mid':              round(mid, 4),
                'premium_per_day':  round(mid / dte, 6) if dte > 0 else 0,
                'delta_from_price': round(o['strike'] - current_price, 2),
                'open_interest':    o.get('openInterest') or 0,
                'volume':           o.get('volume') or 0,
                'implied_volatility': round(iv, 6) if iv else None,
                'iv_vs_hv':         round(iv / hv_ctx['currentHV'], 4) if iv and hv_ctx else None,
                'in_the_money':     bool(o.get('inTheMoney', False)),
            })
        time.sleep(0.2)
    except Exception as e:
        print(f'  ERROR: {e}')
        continue

contracts = [c for c in contracts if c['mid'] > 0 or c['bid'] > 0]
iv_vals   = [c['implied_volatility'] for c in contracts if c['implied_volatility']]
avg_iv    = round(sum(iv_vals) / len(iv_vals), 6) if iv_vals else None
print(f'\n✓ {len(contracts)} contracts collected')


# ── Step 5: save ──────────────────────────────────────────────────────────
os.makedirs('data', exist_ok=True)
output = {
    'fetched_at':    datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
    'ticker':        TICKER,
    'option_type':   OPTION_TYPE,
    'strike':        STRIKE,
    'range':         RANGE,
    'current_price': round(current_price, 2),
    'hv_context':    hv_ctx,
    'avg_iv':        avg_iv,
    'contracts':     contracts,
}
json.dump(output, open('data/options.json', 'w'), indent=2)
print('✓ Saved to data/options.json')
