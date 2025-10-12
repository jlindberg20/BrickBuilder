import os, json, time, hmac, base64, hashlib, random, string
from urllib.parse import quote, urlencode
from datetime import datetime, timezone
import urllib.request, argparse
from collections import Counter

API_ROOT = "https://api.bricklink.com/api/store/v1"

def pct(s): return quote(s, safe="~-._")

def need_env():
    req = ["BRICKLINK_CONSUMER_KEY","BRICKLINK_CONSUMER_SECRET","BRICKLINK_TOKEN","BRICKLINK_TOKEN_SECRET"]
    miss = [k for k in req if not os.environ.get(k)]
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
    key  = f"{pct(cs)}&{pct(ts)}".encode("utf-8")
    sig  = hmac.new(key, base.encode("utf-8"), hashlib.sha1).digest()
    oauth["oauth_signature"] = base64.b64encode(sig).decode("ascii")
    return "OAuth " + ", ".join(f'{k}="{pct(v)}"' for k,v in oauth.items())

def bl_get(path, query=None, timeout=30):
    url = API_ROOT + path
    qs  = "" if not query else "?" + urlencode(query)
    req = urllib.request.Request(url+qs, method="GET")
    req.add_header("Authorization", oauth_header("GET", url, query or {}))
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode("utf-8")
        return json.loads(body)

def get_colors_from_api(part):
    try:
        r = bl_get(f"/items/part/{part}/colors")
        data = r.get("data") or []
        ids = [int(x.get("color_id")) for x in data if x.get("color_id") is not None]
        return sorted(set(ids))
    except Exception:
        return []

COMMON_COLORS = [1,2,3,4,5,7,8,9,11,14,15,19,21,22,23,24,25,26,27,28,33,34,36,40,41,42,85,86,88,95,99,102,110,112,115,120]

def has_activity(part, color_id, currency):
    # lightweight totals probe (no price_detail) to detect any activity quickly
    def tot(guide, cond, country=None):
        q = {"color_id": color_id, "guide_type": guide, "new_or_used": cond, "currency_code": currency}
        if country: q["country_code"] = country
        r = bl_get(f"/items/part/{part}/price", q)
        d = r.get("data") or {}
        lots = int(d.get("total_lots") or 0)
        qty  = int(d.get("total_quantity") or 0)
        return (lots, qty)
    checks = [
        ("stock","U",None),
        ("sold","U",None),
        ("stock","N",None),
        ("sold","N",None),
        ("stock","N","US"),
        ("sold","N","US"),
    ]
    for g,c,cn in checks:
        lots, qty = tot(g,c,cn)
        if lots or qty:
            return True
    return False

def fetch_slice(part, color_id, currency, guide, cond, country=None):
    q = {"color_id": color_id, "guide_type": guide, "new_or_used": cond, "currency_code": currency, "page_size": 500, "price_detail": 1}
    if country: q["country_code"] = country
    r = bl_get(f"/items/part/{part}/price", q)
    d = r.get("data") or {}
    detail = d.get("price_detail") or []

    # coerce numbers safely
    def fnum(x):
        try:
            return float(x)
        except Exception:
            return 0.0

    prices = [fnum(x.get("unit_price")) for x in detail if x is not None]
    qtys   = [int(x.get("quantity") or 0) for x in detail if x is not None]
    lots   = int(d.get("total_lots") or (len(detail)))
    total_qty = int(d.get("total_quantity") or (sum(qtys) if qtys else 0))

    if prices:
        pmin, pmax = min(prices), max(prices)
        if pmax <= pmin:
            edges = [pmin, pmax]
        else:
            step = (pmax - pmin) / 10.0
            edges = [pmin + i*step for i in range(11)]
        # bin counts
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
    else:
        price_hist = {}

    # seller country histogram if available
    sc_hist = Counter()
    for x in (detail or []):
        cc = x.get("seller_country_code") or x.get("country_code") or x.get("seller_country") or None
        if cc: sc_hist[cc] += 1
    seller_hist = dict(sc_hist)

    # averages
    avg = fnum(d.get("avg_price"))
    qty_avg = fnum(d.get("qty_avg_price") or d.get("unit_price"))  # fallback if not present

    return {
        "num_lots": lots,
        "price_histogram": price_hist,
        "seller_country_hist": seller_hist,
        "avg": avg,
        "qty_avg": qty_avg,
        "max": max(prices) if prices else (fnum(d.get("max_price")) if d.get("max_price") else 0),
        "min": min(prices) if prices else (fnum(d.get("min_price")) if d.get("min_price") else 0),
        "total_qty": total_qty
    }

def build_payload(part, currency, color_id):
    slices = {
        "stock_used_world": fetch_slice(part, color_id, currency, "stock", "U", None),
        "sold_new_US":      fetch_slice(part, color_id, currency, "sold",  "N", "US"),
        "sold_used_world":  fetch_slice(part, color_id, currency, "sold",  "U", None),
        "stock_new_world":  fetch_slice(part, color_id, currency, "stock", "N", None),
        "sold_new_world":   fetch_slice(part, color_id, currency, "sold",  "N", None),
        "stock_new_US":     fetch_slice(part, color_id, currency, "stock", "N", "US"),
    }
    # simple activity check
    any_nonzero = any(v.get("num_lots") or v.get("total_qty") for v in slices.values())
    return any_nonzero, {
        "color_id": color_id,
        "element_id": None,
        "color_name": None,
        "slices": slices
    }

def main():
    need_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("--part", required=True)
    ap.add_argument("--currency", default="USD")
    args = ap.parse_args()

    part = args.part
    currency = args.currency

    # 1) Try official colors
    colors = get_colors_from_api(part)
    # 2) If blank or just [0], probe common colors to discover activity
    if not colors or colors == [0]:
        colors = []
        for cid in COMMON_COLORS:
            try:
                if has_activity(part, cid, currency):
                    colors.append(cid)
            except Exception:
                pass
        colors = sorted(set(colors))

    if not colors:
        # Nothing usable
        line = {
            "rb_part_num": part,
            "bricklink": {
                "part_no": part,
                "currency": currency,
                "per_color": {}
            },
            "links": {"catalog": f"https://www.bricklink.com/v2/catalog/catalogitem.page?P={part}"},
            "last_updated": datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
        }
        print(json.dumps(line, separators=(",",":")))
        return

    chosen = None
    payload = None
    for cid in colors:
        try:
            ok, per_color_obj = build_payload(part, currency, cid)
        except Exception:
            ok = False; per_color_obj = None
        if ok:
            chosen = cid
            payload = per_color_obj
            break

    if not payload:
        # Fallback: take first color even if zeroes
        cid = colors[0]
        _, per_color_obj = build_payload(part, currency, cid)
        payload = per_color_obj

    out = {
        "rb_part_num": part,
        "bricklink": {
            "part_no": part,
            "currency": currency,
            "per_color": {
                str(payload["color_id"]): payload
            }
        },
        "links": {"catalog": f"https://www.bricklink.com/v2/catalog/catalogitem.page?P={part}"},
        "last_updated": datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    }
    print(json.dumps(out, separators=(",",":")))

if __name__ == "__main__":
    main()
