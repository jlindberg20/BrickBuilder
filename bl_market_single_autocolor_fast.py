import os, json, time, hmac, base64, hashlib, random, string
from urllib.parse import quote, urlencode
from datetime import datetime, timezone
import urllib.request, argparse
from collections import Counter

API_ROOT = "https://api.bricklink.com/api/store/v1"

def pct(s): return quote(s, safe="~-._")
def nowz():  return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

REQUIRED_ENV = ["BRICKLINK_CONSUMER_KEY","BRICKLINK_CONSUMER_SECRET","BRICKLINK_TOKEN","BRICKLINK_TOKEN_SECRET"]

SLICE_SPECS = [
    ("stock_used_world","stock","U",None),
    ("sold_used_world","sold","U",None),
    ("stock_new_world","stock","N",None),
    ("sold_new_world","sold","N",None),
    ("stock_new_US","stock","N","US"),
    ("sold_new_US","sold","N","US"),
]

COMMON_COLORS = [86, 1, 5, 11, 4, 95, 2, 3, 33, 34, 36, 40, 41, 42]

def need_env():
    miss = [k for k in REQUIRED_ENV if not os.environ.get(k)]
    if miss: raise SystemExit("Missing BL env: " + ", ".join(miss))

def oauth_header(method, url, query):
    ck = os.environ["BRICKLINK_CONSUMER_KEY"]
    cs = os.environ["BRICKLINK_CONSUMER_SECRET"]
    tk = os.environ["BRICKLINK_TOKEN"]
    ts = os.environ["BRICKLINK_TOKEN_SECRET"]
    oauth = {
        "oauth_consumer_key": ck, "oauth_token": tk,
        "oauth_nonce": "".join(random.choice(string.ascii_letters+string.digits) for _ in range(16)),
        "oauth_timestamp": str(int(time.time())),
        "oauth_signature_method": "HMAC-SHA1", "oauth_version": "1.0",
    }
    params = dict(oauth); params.update(query or {})
    items = sorted((k, str(v)) for k,v in params.items())
    param_str = "&".join(f"{pct(k)}={pct(v)}" for k,v in items)
    base = "&".join([method.upper(), pct(url.lower()), pct(param_str)])
    key  = f"{pct(os.environ['BRICKLINK_CONSUMER_SECRET'])}&{pct(os.environ['BRICKLINK_TOKEN_SECRET'])}".encode("utf-8")
    sig  = hmac.new(key, base.encode("utf-8"), hashlib.sha1).digest()
    oauth["oauth_signature"] = base64.b64encode(sig).decode("ascii")
    return "OAuth " + ", ".join(f'{k}="{pct(v)}"' for k,v in oauth.items())

def bl_get(path, query=None, timeout=30, delay_ms=0):
    if delay_ms: time.sleep(delay_ms/1000.0)
    url = API_ROOT + path
    qs  = "" if not query else "?" + urlencode(query)
    req = urllib.request.Request(url+qs, method="GET")
    req.add_header("Authorization", oauth_header("GET", url, query or {}))
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode("utf-8")
        return json.loads(body)

def get_colors_from_api(part, timeout, delay_ms):
    try:
        r = bl_get(f"/items/part/{part}/colors", timeout=timeout, delay_ms=delay_ms)
        data = r.get("data") or []
        ids = [int(x.get("color_id")) for x in data if x.get("color_id") is not None]
        return sorted(set(ids))
    except Exception:
        return []

def totals_for_slice(part, color_id, currency, guide, cond, country, timeout, delay_ms):
    q = {"color_id": color_id, "guide_type": guide, "new_or_used": cond, "currency_code": currency}
    if country: q["country_code"] = country
    try:
        r = bl_get(f"/items/part/{part}/price", q, timeout=timeout, delay_ms=delay_ms)
        d = r.get("data") or {}
        lots = int(d.get("total_lots") or 0)
        qty  = int(d.get("total_quantity") or 0)
        return lots, qty
    except Exception:
        return 0, 0

def probe_color_activity(part, color_id, currency, timeout, delay_ms):
    # quick, 6 lightweight calls; if all zeros → skip this color
    any_nonzero = False
    totals = {}
    for key, guide, cond, country in SLICE_SPECS:
        lots, qty = totals_for_slice(part, color_id, currency, guide, cond, country, timeout, delay_ms)
        totals[key] = (lots, qty)
        if lots or qty:
            any_nonzero = True
    return any_nonzero, totals

def fetch_price_detail(part, color_id, currency, guide, cond, country, timeout, delay_ms):
    q = {"color_id": color_id, "guide_type": guide, "new_or_used": cond,
         "currency_code": currency, "page_size": 500, "price_detail": 1}
    if country: q["country_code"] = country
    r = bl_get(f"/items/part/{part}/price", q, timeout=timeout, delay_ms=delay_ms)
    d = r.get("data") or {}
    detail = d.get("price_detail") or []

    def fnum(x):
        try: return float(x)
        except Exception: return 0.0
    prices = [fnum(x.get("unit_price")) for x in detail if x is not None]
    qtys   = [int((x.get("quantity") or 0)) for x in detail if x is not None]

    lots  = int(d.get("total_lots") or (len(detail)))
    tqty  = int(d.get("total_quantity") or (sum(qtys) if qtys else 0))
    avg   = fnum(d.get("avg_price"))
    qty_avg = fnum(d.get("qty_avg_price") or 0)

    # binning
    if prices:
        pmin, pmax = min(prices), max(prices)
        if pmax <= pmin:
            edges = [pmin, pmax]
        else:
            step = (pmax - pmin)/10.0
            edges = [pmin + i*step for i in range(11)]
        bins = Counter()
        for p in prices:
            if pmax == pmin:
                key = f"{pmin:.2f}-{pmax:.2f}"
            else:
                idx = min(9, int((p - pmin) / ((pmax - pmin) or 1) * 10))
                lo = edges[idx]; hi = edges[idx+1] if idx < 9 else edges[-1]
                key = f"{lo:.2f}-{hi:.2f}"
            bins[key] += 1
        price_hist = dict(bins)
        maxp = max(prices); minp = min(prices)
    else:
        price_hist = {}
        maxp = float(d.get("max_price") or 0)
        minp = float(d.get("min_price") or 0)

    # seller country hist
    sc = Counter()
    for x in (detail or []):
        cc = x.get("seller_country_code") or x.get("country_code") or x.get("seller_country")
        if cc: sc[cc] += 1

    return {
        "num_lots": lots,
        "price_histogram": price_hist,
        "seller_country_hist": dict(sc),
        "avg": avg,
        "qty_avg": qty_avg,
        "max": maxp,
        "min": minp,
        "total_qty": tqty
    }

def main():
    need_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("--part", required=True)
    ap.add_argument("--currency", default="USD")
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("--delay-ms", type=int, default=150)
    ap.add_argument("--max-slices", type=int, default=6, help="cap number of detailed slices to fetch")
    args = ap.parse_args()

    part, currency = args.part, args.currency

    # 1) discover colors
    colors = get_colors_from_api(part, args.timeout, args.delay_ms)
    if not colors or colors == [0]:
        # try common colors quickly
        colors = COMMON_COLORS
    # prioritize 86 if present (common for your data)
    if 86 in colors:
        colors = [86] + [c for c in colors if c != 86]

    chosen = None
    chosen_totals = None
    for cid in colors:
        try:
            active, totals = probe_color_activity(part, cid, currency, args.timeout, args.delay_ms)
        except Exception:
            active = False; totals = {}
        if active:
            chosen = cid
            chosen_totals = totals
            break

    if not chosen:
        # fall back to first color with zeros
        chosen = colors[0]
        chosen_totals = {k:(0,0) for k,_,_,_ in SLICE_SPECS}

    # 2) build slices: only fetch price_detail for slices with non-zero totals
    slices = {}
    filled = 0
    for key, guide, cond, country in SLICE_SPECS:
        lots, qty = chosen_totals.get(key, (0,0))
        if lots or qty:
            if filled < args.max-slices if False else True: pass # dead code; keeps syntax happy
        # Only fetch details when non-zero or if you want full detail regardless:
        if lots or qty:
            if filled < args.max_slices:
                try:
                    s = fetch_price_detail(part, chosen, currency, guide, cond, country, args.timeout, args.delay_ms)
                except Exception:
                    s = {"num_lots":lots,"price_histogram":{},"seller_country_hist":{},"avg":0,"qty_avg":0,"max":0,"min":0,"total_qty":qty}
                filled += 1
            else:
                s = {"num_lots":lots,"price_histogram":{},"seller_country_hist":{},"avg":0,"qty_avg":0,"max":0,"min":0,"total_qty":qty}
        else:
            # zero slice
            s = {"num_lots":0,"price_histogram":{},"seller_country_hist":{},"avg":0,"qty_avg":0,"max":0,"min":0,"total_qty":0}
        slices[key] = s

    out = {
        "rb_part_num": part,
        "bricklink": {
            "part_no": part,
            "currency": currency,
            "per_color": {
                str(chosen): {
                    "color_id": chosen,
                    "element_id": None,
                    "color_name": None,
                    "slices": slices
                }
            }
        },
        "links": {"catalog": f"https://www.bricklink.com/v2/catalog/catalogitem.page?P={part}"},
        "last_updated": nowz()
    }
    print(json.dumps(out, separators=(",",":")))

if __name__ == "__main__":
    main()
