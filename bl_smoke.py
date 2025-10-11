# bl_smoke.py
# Purpose: Call BrickLink price guide for one color and emit EXACT marketplace rows (JSONL).
# Usage examples:
#   python bl_smoke.py --parts 6920 6923 69234 --color 86 --currency USD
#   python bl_smoke.py --parts 10247c01 6191pb042 23325 --color 0 --currency USD
#
# Env required (or in .env at repo root):
#   BRICKLINK_CONSUMER_KEY, BRICKLINK_CONSUMER_SECRET, BRICKLINK_TOKEN, BRICKLINK_TOKEN_SECRET

import argparse, base64, hashlib, hmac, json, os, sys, time, urllib.parse, urllib.request
from datetime import datetime, timezone
from collections import OrderedDict

API_BASE = "https://api.bricklink.com/api/store/v1"

def load_dotenv_if_needed():
    """If the 4 BL vars are missing, try loading .env in CWD (KEY=VALUE lines)."""
    need = [
        "BRICKLINK_CONSUMER_KEY",
        "BRICKLINK_CONSUMER_SECRET",
        "BRICKLINK_TOKEN",
        "BRICKLINK_TOKEN_SECRET",
    ]
    missing = [k for k in need if not os.getenv(k)]
    if missing and os.path.exists(".env"):
        with open(".env", "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and v and not os.getenv(k):
                    os.environ[k] = v

def pct_encode(s: str) -> str:
    return urllib.parse.quote(s, safe="~-._")

def oauth_header(method: str, url: str, query: dict) -> str:
    ck = os.environ["BRICKLINK_CONSUMER_KEY"]
    cs = os.environ["BRICKLINK_CONSUMER_SECRET"]
    tk = os.environ["BRICKLINK_TOKEN"]
    ts = os.environ["BRICKLINK_TOKEN_SECRET"]

    nonce = "".join(__import__("random").choice("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") for _ in range(16))
    ts_sec = str(int(time.time()))

    params = OrderedDict()
    params["oauth_consumer_key"] = ck
    params["oauth_token"] = tk
    params["oauth_nonce"] = nonce
    params["oauth_timestamp"] = ts_sec
    params["oauth_signature_method"] = "HMAC-SHA1"
    params["oauth_version"] = "1.0"
    if query:
        for k in sorted(query.keys()):
            params[k] = str(query[k])

    # Normalize
    param_pairs = [f"{pct_encode(k)}={pct_encode(v)}" for k, v in params.items()]
    param_str = "&".join(param_pairs)

    base_elems = [
        method.upper(),
        pct_encode(url.lower()),
        pct_encode(param_str),
    ]
    base_str = "&".join(base_elems)

    key = f"{pct_encode(cs)}&{pct_encode(ts)}"
    digest = hmac.new(key.encode("ascii"), base_str.encode("ascii"), hashlib.sha1).digest()
    sig = base64.b64encode(digest).decode("ascii")

    hdr = (
        'OAuth '
        f'oauth_consumer_key="{ck}",'
        f'oauth_token="{tk}",'
        'oauth_signature_method="HMAC-SHA1",'
        f'oauth_signature="{pct_encode(sig)}",'
        f'oauth_timestamp="{ts_sec}",'
        f'oauth_nonce="{nonce}",'
        'oauth_version="1.0"'
    )
    return hdr

def bl_get(endpoint: str, q: dict):
    url = API_BASE + endpoint
    qs = urllib.parse.urlencode({k: str(v) for k, v in sorted(q.items())}) if q else ""
    full = url + ("?" + qs if qs else "")

    req = urllib.request.Request(full, method="GET")
    req.add_header("Authorization", oauth_header("GET", url, q))
    req.add_header("Accept", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read().decode("utf-8"))
            return data
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", "ignore")
            payload = json.loads(body)
        except Exception:
            payload = {"meta": {"code": e.code, "message": str(e)}}
        return payload
    except Exception as e:
        return {"meta": {"code": -1, "message": str(e)}}

def price_guide(part_no: str, color_id: int, guide_type: str, new_or_used: str, country: str, currency: str):
    q = {
        "new_or_used": new_or_used,   # 'N' or 'U'
        "guide_type": guide_type,     # 'sold' or 'stock'
        "currency_code": currency,
    }
    if color_id >= 0:
        q["color_id"] = color_id
    if country:
        q["country_code"] = country
    else:
        q["region"] = "world"
    return bl_get(f"/items/part/{part_no}/price", q)

def num(v):  # safe numeric cast
    try:
        if v is None: return 0
        return float(v)
    except Exception:
        return 0

def build_slice(resp: dict) -> dict | None:
    if not resp or "meta" not in resp or resp["meta"].get("code") != 200:
        return None
    d = resp.get("data") or {}

    detail = d.get("price_detail") or []
    if isinstance(detail, dict):
        detail = [detail]

    # totals
    num_lots = int(d.get("total_lots") or len(detail) or 0)
    total_qty = d.get("total_quantity")
    if total_qty is None:
        total_qty = sum(int(x.get("quantity") or 0) for x in detail)
    total_qty = int(total_qty or 0)

    avg = num(d.get("avg_price"))
    qty_avg = num(d.get("qty_avg_price"))
    min_p = num(d.get("min_price"))
    max_p = num(d.get("max_price"))

    # histogram + seller countries (tiny, indicative)
    prices = [num(x.get("unit_price")) for x in detail if x.get("unit_price") is not None]
    price_hist = OrderedDict()
    if prices:
        lo, hi = min(prices), max(prices)
        if hi > lo:
            bins = 10
            span = hi - lo
            for p in prices:
                idx = min(bins - 1, int((p - lo) / span * bins))
                lo_i = lo + idx * span / bins
                hi_i = lo + (idx + 1) * span / bins
                key = f"{round(lo_i, 2)}-{round(hi_i, 2)}"
                price_hist[key] = price_hist.get(key, 0) + 1

    seller_hist = OrderedDict()
    for x in detail:
        cc = x.get("seller_country_code")
        if cc:
            seller_hist[cc] = seller_hist.get(cc, 0) + 1

    return OrderedDict([
        ("num_lots", num_lots),
        ("price_histogram", price_hist),
        ("seller_country_hist", seller_hist),
        ("avg", avg),
        ("qty_avg", qty_avg),
        ("max", max_p),
        ("min", min_p),
        ("total_qty", total_qty),
    ])

def build_row(part_no: str, color_id: int, currency: str) -> dict:
    # The 6 slices you store
    sold_used_world  = build_slice(price_guide(part_no, color_id, "sold",  "U", "",   currency))
    sold_new_US      = build_slice(price_guide(part_no, color_id, "sold",  "N", "US", currency))
    stock_new_world  = build_slice(price_guide(part_no, color_id, "stock", "N", "",   currency))
    stock_new_US     = build_slice(price_guide(part_no, color_id, "stock", "N", "US", currency))
    sold_new_world   = build_slice(price_guide(part_no, color_id, "sold",  "N", "",   currency))
    stock_used_world = build_slice(price_guide(part_no, color_id, "stock", "U", "",   currency))

    per_color = {
        str(color_id): OrderedDict([
            ("slices", OrderedDict([
                ("stock_used_world", stock_used_world),
                ("sold_new_US",      sold_new_US),
                ("sold_used_world",  sold_used_world),
                ("stock_new_world",  stock_new_world),
                ("sold_new_world",   sold_new_world),
                ("stock_new_US",     stock_new_US),
            ])),
            ("color_id",  color_id),
            ("element_id", None),
            ("color_name", None),
        ])
    }

    row = OrderedDict([
        ("rb_part_num", part_no),  # for smoke test we reuse part_no; your repair script will set real rb_part_num
        ("bricklink", OrderedDict([
            ("part_no", part_no),
            ("currency", currency),
            ("per_color", per_color),
        ])),
        ("links", {"catalog": f"https://www.bricklink.com/v2/catalog/catalogitem.page?P={part_no}"}),
        ("last_updated", datetime.now(timezone.utc).isoformat()),
    ])
    return row

def main():
    load_dotenv_if_needed()
    for k in ["BRICKLINK_CONSUMER_KEY","BRICKLINK_CONSUMER_SECRET","BRICKLINK_TOKEN","BRICKLINK_TOKEN_SECRET"]:
        if not os.getenv(k):
            print(f"FATAL: missing env {k}", file=sys.stderr)
            sys.exit(2)

    ap = argparse.ArgumentParser(description="BrickLink price-guide smoke test (JSONL rows).")
    ap.add_argument("--parts", nargs="+", required=True, help="BrickLink part numbers (e.g., 69234 6191pb042)")
    ap.add_argument("--color", type=int, default=0, help="color_id to fetch (default 0)")
    ap.add_argument("--currency", default="USD")
    ap.add_argument("--delay-ms", type=int, default=350, help="delay between parts")
    args = ap.parse_args()

    # Emit one JSONL row per part
    for idx, p in enumerate(args.parts, 1):
        try:
            row = build_row(p, args.color, args.currency)
            print(json.dumps(row, separators=(",", ":"), ensure_ascii=False))
        except Exception as e:
            meta = {"rb_part_num": p, "error": str(e)}
            print(json.dumps(meta, separators=(",", ":")))
        if idx < len(args.parts):
            time.sleep(args.delay_ms / 1000.0)

if __name__ == "__main__":
    main()
