"""
fetch_options.py
Runs inside GitHub Actions — no CORS restrictions.
Fetches Yahoo Finance options chain + 1-year price history,
calculates HV percentile, saves everything to data/options.json
"""

import os, json, math, time, datetime, requests

TICKER      = os.environ.get("TICKER",      "MSTR").upper()
OPTION_TYPE = os.environ.get("OPTION_TYPE", "call").lower()
STRIKE      = float(os.environ.get("STRIKE", "400"))
RANGE       = float(os.environ.get("RANGE",  "5"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://finance.yahoo.com/",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def yf_get(url, retries=3):
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to fetch: {url}")


def calc_hv(closes, window=30):
    if len(closes) < window + 1:
        return []
    hvs = []
    for i in range(window, len(closes)):
        sl = closes[i - window: i + 1]
        log_rets = [
            math.log(sl[j] / sl[j - 1])
            for j in range(1, len(sl))
            if sl[j] > 0 and sl[j - 1] > 0
        ]
        if len(log_rets) < 2:
            continue
        mean = sum(log_rets) / len(log_rets)
        variance = sum((x - mean) ** 2 for x in log_rets) / (len(log_rets) - 1)
        hvs.append(math.sqrt(variance * 252))
    return hvs


def percentile(series, val):
    if not series:
        return 0
    return round(sum(1 for x in series if x <= val) / len(series) * 100)


# ── 1. Fetch 1-year price history for HV ─────────────────────────────────
print(f"Fetching 1-year price history for {TICKER}…")
chart_url = (
    f"https://query1.finance.yahoo.com/v8/finance/chart/"
    f"{TICKER}?interval=1d&range=1y"
)
chart_data  = yf_get(chart_url)
chart_result = chart_data["chart"]["result"][0]
closes = [
    c for c in chart_result["indicators"]["quote"][0].get("close", [])
    if c is not None and c > 0
]
current_price = chart_result["meta"].get("regularMarketPrice", 0)
print(f"  Current price: ${current_price:.2f}  |  {len(closes)} daily closes")

hv_ctx = None
if len(closes) >= 35:
    hv_series = calc_hv(closes, 30)
    if hv_series:
        cur_hv = hv_series[-1]
        hv_ctx = {
            "currentHV":  round(cur_hv, 6),
            "hvPct":      percentile(hv_series, cur_hv),
            "hvMin":      round(min(hv_series), 6),
            "hvMax":      round(max(hv_series), 6),
        }
        print(f"  HV30: {cur_hv*100:.1f}%  |  Percentile: {hv_ctx['hvPct']}th")


# ── 2. Get expiry dates ───────────────────────────────────────────────────
print(f"\nFetching options expiry dates for {TICKER}…")
opts_url  = f"https://query1.finance.yahoo.com/v7/finance/options/{TICKER}"
opts_data = yf_get(opts_url)
opt_result = opts_data["optionChain"]["result"][0]

if not current_price:
    current_price = opt_result["quote"].get("regularMarketPrice", 0)

expiries = opt_result.get("expirationDates", [])
print(f"  Found {len(expiries)} expiry dates")


# ── 3. Fetch each expiry ──────────────────────────────────────────────────
today    = datetime.date.today()
contracts = []

for i, ts in enumerate(expiries):
    exp_date = datetime.date.fromtimestamp(ts)
    dte      = max(1, (exp_date - today).days)
    exp_str  = exp_date.isoformat()

    print(f"  [{i+1}/{len(expiries)}] {exp_str} (DTE {dte})…", end=" ")

    try:
        d = yf_get(
            f"https://query1.finance.yahoo.com/v7/finance/options/"
            f"{TICKER}?date={ts}"
        )
        result = d["optionChain"]["result"][0]
        rows   = (
            result["options"][0].get("calls", [])
            if OPTION_TYPE == "call"
            else result["options"][0].get("puts", [])
        )
        matched = [
            o for o in rows
            if o.get("strike") is not None
            and abs(o["strike"] - STRIKE) <= RANGE
        ]
        print(f"{len(matched)} contracts matched")

        for o in matched:
            bid = o.get("bid", 0) or 0
            ask = o.get("ask", 0) or 0
            mid = (bid + ask) / 2
            iv  = o.get("impliedVolatility")
            if iv and iv > 5:          # normalise % → decimal
                iv = iv / 100
            iv_vs_hv = (
                round(iv / hv_ctx["currentHV"], 4)
                if iv and hv_ctx
                else None
            )
            contracts.append({
                "option_type":      OPTION_TYPE,
                "expiration_date":  exp_str,
                "dte":              dte,
                "strike":           o["strike"],
                "bid":              round(bid, 4),
                "ask":              round(ask, 4),
                "mid":              round(mid, 4),
                "premium_per_day":  round(mid / dte, 6) if dte > 0 else 0,
                "delta_from_price": round(o["strike"] - current_price, 2),
                "open_interest":    o.get("openInterest", 0) or 0,
                "volume":           o.get("volume", 0) or 0,
                "implied_volatility": round(iv, 6) if iv else None,
                "iv_vs_hv":         iv_vs_hv,
                "in_the_money":     bool(o.get("inTheMoney", False)),
            })

        time.sleep(0.2)   # be polite to Yahoo's servers

    except Exception as e:
        print(f"ERROR: {e}")
        continue

# Filter zero-premium ghost rows
contracts = [c for c in contracts if c["mid"] > 0 or c["bid"] > 0]

# Avg IV
iv_vals = [c["implied_volatility"] for c in contracts if c["implied_volatility"]]
avg_iv  = round(sum(iv_vals) / len(iv_vals), 6) if iv_vals else None

print(f"\n✓ {len(contracts)} contracts collected")


# ── 4. Save output ────────────────────────────────────────────────────────
os.makedirs("data", exist_ok=True)

output = {
    "fetched_at":    datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    "ticker":        TICKER,
    "option_type":   OPTION_TYPE,
    "strike":        STRIKE,
    "range":         RANGE,
    "current_price": round(current_price, 2),
    "hv_context":    hv_ctx,
    "avg_iv":        avg_iv,
    "contracts":     contracts,
}

with open("data/options.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"✓ Saved to data/options.json")
