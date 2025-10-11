import os, sys, hmac, hashlib, time, random, string, json, argparse, urllib.parse, base64
from datetime import datetime
import urllib.request
from collections import Counter

BL_BASE = "https://api.bricklink.com/api/store/v1"

# ---------- OAuth ----------
def _pct(s: str) -> str:
    return urllib.parse.quote(s, safe="~-._")

def oauth_header(method: str, url: str, query: dict) -> str:
    ck  = os.environ.get("BRICKLINK_CONSUMER_KEY", "")
    cs  = os.environ.get("BRICKLINK_CONSUMER_SECRET", "")
    tk  = os.environ.get("BRICKLINK_TOKEN", "")
    ts  = os.environ.get("BRICKLINK_TOKEN_SECRET", "")
    if not all([ck, cs, tk, ts]):
        raise RuntimeError("Missing BrickLink OAuth env vars.")
    nonce = "".join(random.choice(string.ascii_letters+string.digits) for _ in range(20))
    ts_epoch = str(int(time.time()))
    oauth_params = {
        "oauth_consumer_key": ck,
        "oauth_nonce": nonce,
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": ts_epoch,
        "oauth_token": tk,
        "oauth_version": "1.0",
    }
    all_params = {**oauth_params, **{k: str(v) for (k, v) in (query or {}).items()}}
    param_str = "&".join(f"{_pct(k)}={_pct(all_params[k])}" for k in sorted(all_params.keys()))
    base_str = "&".join([method.upper(), _pct(url), _pct(param_str)])
    signing_key = f"{_pct(cs)}&{_pct(ts)}"
    sig = base64.b64encode(hmac.new(signing_key.encode(), base_str.encode(), hashlib.sha1).digest()).decode()
    header_params = oauth_params.copy()
    header_params["oauth_signature"] = sig
    return "OAuth " + ", ".join([f'{k}="{_pct(header_params[k])}"' for k in sorted(header_params.keys())])

def bl_get(path: str, params: dict) -> dict:
    url = f"{BL_BASE}{path}"
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", oauth_header("GET", f"{BL_BASE}{path}", params or {}))
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

# ---------- Slices / hist ----------
def empty_slice():
    return {
        "num_lots": 0,
        "price_histogram": {},
        "seller_country_hist": {},
        "avg": 0.0,
        "qty_avg": 0.0,
        "max": 0.0,
        "min": 0.0,
        "total_qty": 0,
    }

def _mk_hist(prices, bins=10):
    if not prices:
        return {}
    lo, hi = min(prices), max(prices)
    if hi <= lo:
        label = f"{round(lo,2):.2f}-{round(hi,2):.2f}"
        return {label: len(prices)}
    width = (hi - lo) / bins
    edges = [lo + i*width for i in range(bins)]
    edges.append(hi)  # inclusive end
    counts = [0]*bins
    for p in prices:
        if p >= edges[-1]:  # guard for floating rounding
            idx = bins-1
        else:
            idx = max(0, min(bins-1, int((p - lo) / width)))
        counts[idx] += 1
    hist = {}
    for i in range(bins):
        a = round(edges[i], 2)
        b = round(edges[i+1], 2)
        hist[f"{a:.2f}-{b:.2f}"] = counts[i]
    return hist

def build_slice(resp_json: dict) -> dict:
    d = (resp_json or {}).get("data") or {}
    detail = d.get("price_detail") or []
    prices = []
    qtys = []
    country_codes = []
    for it in detail:
        try:
            price = float(it.get("unit_price") or 0.0)
            qty   = int(it.get("quantity") or 0)
            prices.append(price)
            qtys.append(qty)
            cc = it.get("seller_country_code") or it.get("country_code")
            if cc:
                country_codes.append(cc)
        except Exception:
            pass

    # Fallbacks when summary is empty but detail exists
    num_lots  = int(d.get("total_lots") or (len(detail) if detail else 0))
    total_qty = int(d.get("total_quantity") or (sum(qtys) if qtys else 0))
    avg       = float(d.get("avg_price") or (sum(prices)/len(prices) if prices else 0.0))
    qty_avg   = float(d.get("qty_avg_price") or 0.0)
    max_p     = float(d.get("max_price") or (max(prices) if prices else 0.0))
    min_p     = float(d.get("min_price") or (min(prices) if prices else 0.0))

    price_hist = _mk_hist(prices, bins=10)
    seller_hist = dict(Counter(country_codes)) if country_codes else {}

    return {
        "num_lots": num_lots,
        "price_histogram": price_hist,
        "seller_country_hist": seller_hist,
        "avg": avg,
        "qty_avg": qty_avg,
        "max": max_p,
        "min": min_p,
        "total_qty": total_qty,
    }

def get_price(part_no: str, color_id: int, guide: str, cond: str, currency: str, country: str | None):
    q = {
        "color_id": color_id,
        "guide_type": guide,         # "stock" or "sold"
        "new_or_used": cond,         # "N" or "U"
        "currency_code": currency,
    }
    if country:
        q["country_code"] = country
    return bl_get(f"/items/part/{urllib.parse.quote(part_no)}/price", q)

def choose_color(part_no: str, prefer: list[int] | None) -> int | None:
    try:
        colors = bl_get(f"/items/part/{urllib.parse.quote(part_no)}/colors", {})
        ids = [int(c.get("color_id")) for c in (colors.get("data") or []) if c.get("color_id") is not None]
        if not ids: return None
        for cid in (prefer or []):
            if cid in ids: return cid
        return ids[0]
    except Exception:
        return None

def build_row(part_no: str, color_id: int, currency: str):
    try: s_stock_used_world = build_slice(get_price(part_no, color_id, "stock", "U", currency, None))
    except Exception: s_stock_used_world = empty_slice()
    try: s_sold_new_US     = build_slice(get_price(part_no, color_id, "sold",  "N", currency, "US"))
    except Exception: s_sold_new_US = empty_slice()
    try: s_sold_used_world = build_slice(get_price(part_no, color_id, "sold",  "U", currency, None))
    except Exception: s_sold_used_world = empty_slice()
    try: s_stock_new_world = build_slice(get_price(part_no, color_id, "stock", "N", currency, None))
    except Exception: s_stock_new_world = empty_slice()
    try: s_sold_new_world  = build_slice(get_price(part_no, color_id, "sold",  "N", currency, None))
    except Exception: s_sold_new_world = empty_slice()
    try: s_stock_new_US    = build_slice(get_price(part_no, color_id, "stock", "N", currency, "US"))
    except Exception: s_stock_new_US = empty_slice()

    return {
        "rb_part_num": part_no,
        "bricklink": {
            "part_no": part_no,
            "currency": currency,
            "per_color": {
                str(color_id): {
                    "slices": {
                        "stock_used_world": s_stock_used_world,
                        "sold_new_US":     s_sold_new_US,
                        "sold_used_world": s_sold_used_world,
                        "stock_new_world": s_stock_new_world,
                        "sold_new_world":  s_sold_new_world,
                        "stock_new_US":    s_stock_new_US,
                    },
                    "color_id": color_id,
                    "element_id": None,
                    "color_name": None,
                }
            }
        },
        "links": {"catalog": f"https://www.bricklink.com/v2/catalog/catalogitem.page?P={part_no}"},
        "last_updated": datetime.utcnow().isoformat() + "Z",
    }

def main():
    ap = argparse.ArgumentParser(description="BrickLink marketplace smoke (schema-accurate with histograms).")
    ap.add_argument("--parts", nargs="+", required=True)
    ap.add_argument("--currency", default="USD")
    ap.add_argument("--color", type=int, default=None)
    ap.add_argument("--autocolor", action="store_true")
    args = ap.parse_args()

    print("Env -> CK={0} CS={1} TK={2} TS={3}".format(
        bool(os.environ.get("BRICKLINK_CONSUMER_KEY")),
        bool(os.environ.get("BRICKLINK_CONSUMER_SECRET")),
        bool(os.environ.get("BRICKLINK_TOKEN")),
        bool(os.environ.get("BRICKLINK_TOKEN_SECRET"))
    ), file=sys.stderr)

    for part in args.parts:
        cid = args.color
        if cid is None and args.autocolor:
            cid = choose_color(part, prefer=[86, 0])
        if cid is None:
            cid = 0
        try:
            row = build_row(part, cid, args.currency)
            print(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
        except urllib.error.HTTPError as e:
            try: body = e.read().decode()
            except Exception: body = ""
            print(json.dumps({"rb_part_num": part, "error":{"status": e.code, "reason": e.reason, "body": body}}))
        except Exception as ex:
            print(json.dumps({"rb_part_num": part, "error":{"message": str(ex)}}))

if __name__ == "__main__":
    main()
