"""
fetch_options.py  —  uses yfinance which handles Yahoo Finance auth automatically.
"""
import os, json, math, datetime
import yfinance as yf

TICKER      = os.environ.get('TICKER',      'MSTR').upper()
OPTION_TYPE = os.environ.get('OPTION_TYPE', 'call').lower()
STRIKE      = float(os.environ.get('STRIKE', '400'))
RANGE       = float(os.environ.get('RANGE',  '5'))

print(f'Fetching data for {TICKER}  |  {OPTION_TYPE}s  |  strike ${STRIKE} +/- ${RANGE}')

stock = yf.Ticker(TICKER)

# 1. Current price
info          = stock.fast_info
current_price = getattr(info, 'last_price', None) or getattr(info, 'regular_market_price', None) or 0
print(f'Current price: ${current_price:.2f}')

# 2. HV from 1-year daily history
print('Fetching 1-year price history...')
hist   = stock.history(period='1y', interval='1d')
closes = hist['Close'].dropna().tolist()
print(f'  {len(closes)} daily closes')

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

# 3. Options chain
print('Fetching options expiry dates...')
expiries = stock.options
print(f'  Found {len(expiries)} expiry dates')

today     = datetime.date.today()
contracts = []

for i, exp_str in enumerate(expiries):
    exp_date = datetime.date.fromisoformat(exp_str)
    dte      = max(1, (exp_date - today).days)
    print(f'  [{i+1}/{len(expiries)}] {exp_str}  DTE={dte}', end='  ')
    try:
        chain = stock.option_chain(exp_str)
        rows  = chain.calls if OPTION_TYPE == 'call' else chain.puts
        hit   = rows[abs(rows['strike'] - STRIKE) <= RANGE]
        print(f'{len(hit)} matched')
        for _, o in hit.iterrows():
            bid = float(o.get('bid', 0) or 0)
            ask = float(o.get('ask', 0) or 0)
            mid = (bid + ask) / 2
            iv  = float(o.get('impliedVolatility', 0) or 0)
            if iv and iv > 5:
                iv = iv / 100
            contracts.append({
                'option_type':      OPTION_TYPE,
                'expiration_date':  exp_str,
                'dte':              dte,
                'strike':           float(o['strike']),
                'bid':              round(bid, 4),
                'ask':              round(ask, 4),
                'mid':              round(mid, 4),
                'premium_per_day':  round(mid / dte, 6) if dte > 0 else 0,
                'delta_from_price': round(float(o['strike']) - current_price, 2),
                'open_interest':    int(o.get('openInterest', 0) or 0),
                'volume':           int(o.get('volume', 0) or 0),
                'implied_volatility': round(iv, 6) if iv else None,
                'iv_vs_hv':         round(iv / hv_ctx['currentHV'], 4) if iv and hv_ctx else None,
                'in_the_money':     bool(o.get('inTheMoney', False)),
            })
    except Exception as e:
        print(f'  ERROR: {e}')
        continue

contracts = [c for c in contracts if c['mid'] > 0 or c['bid'] > 0]
iv_vals   = [c['implied_volatility'] for c in contracts if c['implied_volatility']]
avg_iv    = round(sum(iv_vals) / len(iv_vals), 6) if iv_vals else None
print(f'\nTotal contracts collected: {len(contracts)}')

# 4. Save
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
print('Saved to data/options.json')
