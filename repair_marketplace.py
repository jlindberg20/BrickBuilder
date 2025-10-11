# -*- coding: utf-8 -*-
"""
Repair BrickLink marketplace JSONL:
- Copy all RICH rows from input JSONL
- Detect NEED rows (empty per_color {} or missing per_color)
- For each NEED part: fetch ALL available colors, then fetch 6 guides per color:
    stock_used_world, sold_used_world, stock_new_world, sold_new_world, stock_new_US, sold_new_US
- Build exact schema: rb_part_num, bricklink{part_no,currency,per_color{color_id:{slices{...}}}}, links.catalog, last_updated
- Stream to OUT.tmp and rename to OUT at the end.
"""

import os, sys, re, json, time, hmac, base64, random, string, hashlib
from datetime import datetime, timezone
from urllib.parse import quote, urlencode
import argparse
import urllib.request

API_ROOT = "https://api.bricklink.com/api/store/v1"

def _pct(s:str) -> str: return quote(s, safe="~-._")
def _now_iso() -> str:  return datetime.now(timezone.utc).isoformat()

def oauth_header(method:str, url:str, query:dict):
    need = ["BRICKLINK_CONSUMER_KEY","BRICKLINK_CONSUMER_SECRET","BRICKLINK_TOKEN","BRICKLINK_TOKEN_SECRET"]
    missing = [k for k in need if not os.environ.get(k)]
    if missing:
        raise RuntimeError("Missing env: " + ", ".join(missing))
    ck = os.environ["BRICKLINK_CONSUMER_KEY"]
    cs = os.environ["BRICKLINK_CONSUMER_SECRET"]
    tk = os.environ["BRICKLINK_TOKEN"]
    ts = os.environ["BRICKLINK_TOKEN_SECRET"]

    oauth = {
        "oauth_consumer_key": ck,
        "oauth_token": tk,
        "oauth_nonce": "".join(random.choice(string.ascii_letters+string.digits) for _ in range(16)),
        "oauth_timestamp": str(int(time.time())),
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_version": "1.0",
    }
    # signature base string
    params = dict(oauth)
    params.update(query or {})
    items = sorted((k, str(v)) for k,v in params.items())
    param_str = "&".join(f"{_pct(k)}={_pct(v)}" for k,v in items)
    base = "&".join([method.upper(), _pct(url.lower()), _pct(param_str)])
    key = f"{_pct(cs)}&{_pct(ts)}".encode("utf-8")
    sig = hmac.new(key, base.encode("utf-8"), hashlib.sha1).digest()
    oauth["oauth_signature"] = base64.b64encode(sig).decode("ascii")
    hdr = "OAuth " + ", ".join(f'{k}="{_pct(v)}"' for k,v in oauth.items())
    return hdr

def bl_get(path:str, query:dict|None=None, retry:int=5, delay_ms:int=400):
    url = API_ROOT + path
    qs  = "" if not query else "?" + urlencode(query)
    req = urllib.request.Request(url + qs, method="GET")
    req.add_header("Authorization", oauth_header("GET", url, query or {}))
    req.add_header("Content-Type", "application/json")
    last_err = None
    for i in range(retry+1):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.loads(r.read().decode("utf-8"))
                return data
        except Exception as e:
            last_err = e
            time.sleep(max(0.05, delay_ms/1000.0))
    raise last_err

# ---------- price guide helpers ----------

def _histogram(price_details, bins=10):
    # Build string-range histogram like "a-b": count
    if not price_details:
        return {}
    prices = [float(pd.get("unit_price") or 0) for pd in price_details if pd.get("unit_price") is not None]
    if not prices:
        return {}
    mn, mx = min(prices), max(prices)
    if mx <= mn:
        key = f"{mn:.2f}-{mx:.2f}"
        return {key: len(prices)}
    width = (mx - mn) / bins
    # clamp tiny width
    if width <= 1e-9:
        key = f"{mn:.2f}-{mx:.2f}"
        return {key: len(prices)}
    counts = {}
    for p in prices:
        idx = int((p - mn) / width)
        if idx >= bins: idx = bins-1
        lo = mn + idx*width
        hi = lo + width
        key = f"{lo:.2f}-{hi:.2f}"
        counts[key] = counts.get(key, 0) + 1
    return counts

def _avg(v): 
    return float(v) if v is not None else 0.0

def _qty_avg(price_details):
    if not price_details: return 0.0
    s_val = 0.0; s_qty = 0.0
    for pd in price_details:
        q = float(pd.get("quantity") or 0)
        p = float(pd.get("unit_price") or 0)
        s_val += p*q
        s_qty += q
    return (s_val/s_qty) if s_qty > 0 else 0.0

def build_slice(pg_json):
    # pg_json is the raw response from /items/part/{no}/price
    meta = pg_json.get("meta") or {}
    if int(meta.get("code",0)) != 200:   # tolerate empty-ok by returning zeros
        return {
            "num_lots": 0, "price_histogram": {}, "seller_country_hist": {},
            "avg": 0.0, "qty_avg": 0.0, "max": 0.0, "min": 0.0, "total_qty": 0
        }
    d = (pg_json.get("data") or {})
    details = d.get("price_detail") or []
    return {
        "num_lots":   int(d.get("total_lots") or len(details) or 0),
        "price_histogram": _histogram(details, bins=10),
        "seller_country_hist": {},  # can be filled if you need per-country later
        "avg":        _avg(d.get("avg_price")),
        "qty_avg":    _qty_avg(details),
        "max":        _avg(d.get("max_price")),
        "min":        _avg(d.get("min_price")),
        "total_qty":  int(d.get("total_quantity") or 0),
    }

def fetch_color_ids(part_no:str) -> list[int]:
    # Returns ALL BrickLink color IDs for this part; if none, returns [0]
    try:
        j = bl_get(f"/items/part/{part_no}/colors", {})
        if int((j.get("meta") or {}).get("code",0)) == 200:
            arr = j.get("data") or []
            ids = sorted({int(x.get("color_id")) for x in arr if x.get("color_id") is not None})
            return ids if ids else [0]
    except Exception:
        pass
    return [0]

def fetch_slices_for_color(part_no:str, color_id:int, currency:str, delay_ms:int, retries:int):
    # 6 slices
    def _pg(guide, cond, region):
        q = {"color_id": color_id, "guide_type": guide, "new_or_used": cond, "currency_code": currency}
        if region is not None:
            q["country_code"] = region
        return bl_get(f"/items/part/{part_no}/price", q, retry=retries, delay_ms=delay_ms)

    slices = {
        "stock_used_world": build_slice(_pg("stock","U", None)),
        "sold_new_US":      build_slice(_pg("sold","N","US")),
        "sold_used_world":  build_slice(_pg("sold","U", None)),
        "stock_new_world":  build_slice(_pg("stock","N", None)),
        "sold_new_world":   build_slice(_pg("sold","N", None)),
        "stock_new_US":     build_slice(_pg("stock","N","US")),
    }
    return slices

# ---------- main ----------

def is_rich(line:str) -> bool:
    # RICH if it contains a non-empty "price_histogram" (has digits)
    return ('"price_histogram":{"' in line and re.search(r'"price_histogram":\s*\{[^}]*\d', line))

def extract_ids(line:str):
    rb = None; bl = None
    m = re.search(r'"rb_part_num"\s*:\s*"([^"]+)"', line)
    if m: rb = m.group(1)
    m = re.search(r'catalogitem\.page\?P=([^"\\]+)', line)
    if m: bl = m.group(1)
    if not bl:
        # fallback to part_no if present
        m = re.search(r'"part_no"\s*:\s*"([^"]+)"', line)
        if m: bl = m.group(1)
    return rb, bl

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in",  dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--currency", default="USD")
    ap.add_argument("--delay-ms", type=int, default=400)
    ap.add_argument("--max-retries", type=int, default=5)
    args = ap.parse_args()

    in_path  = args.inp
    out_path = args.out
    tmp_path = out_path + ".tmp"

    env_ok = all(os.environ.get(k) for k in
        ["BRICKLINK_CONSUMER_KEY","BRICKLINK_CONSUMER_SECRET","BRICKLINK_TOKEN","BRICKLINK_TOKEN_SECRET"])
    print(f"=== BL repair ===  Env={env_ok}  IN={in_path}  OUT={out_path}")
    sys.stdout.flush()

    if not os.path.exists(in_path):
        print("Input file not found.")
        sys.exit(2)

    # Prepare temp
    if os.path.exists(tmp_path):
        os.remove(tmp_path)

    total = 0; rich = 0; need = 0
    lines = []
    with open(in_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        for ln in f:
            lines.append(ln)
    total = len(lines)

    # First pass: copy RICH, collect NEED
    need_list = []
    with open(tmp_path, "w", encoding="utf-8", newline="\n") as out:
        for ln in lines:
            if is_rich(ln):
                out.write(ln if ln.endswith("\n") else ln+"\n")
                rich += 1
            else:
                need_list.append(ln)
                need += 1

    print(f"Loaded lines={total}  RICH copied={rich}  NEED={need}")
    sys.stdout.flush()

    # Second pass: fetch ONLY NEED
    ok = 0; fail = 0
    with open(tmp_path, "a", encoding="utf-8", newline="\n") as out:
        for idx, ln in enumerate(need_list, 1):
            rb, bl = extract_ids(ln)
            if not bl:
                fail += 1
                continue
            # ALL COLORS:
            try:
                color_ids = fetch_color_ids(bl)
            except Exception as e:
                color_ids = [0]

            per_color = {}
            for cid in color_ids:
                try:
                    slices = fetch_slices_for_color(bl, cid, args.currency, args.delay_ms, args.max_retries)
                    per_color[str(cid)] = {
                        "slices": slices,
                        "color_id": cid,
                        "element_id": None,
                        "color_name": None
                    }
                except Exception:
                    # still write empty slices to keep shape
                    per_color[str(cid)] = {
                        "slices": {
                            "stock_used_world":  {"num_lots":0,"price_histogram":{},"seller_country_hist":{},"avg":0.0,"qty_avg":0.0,"max":0.0,"min":0.0,"total_qty":0},
                            "sold_new_US":      {"num_lots":0,"price_histogram":{},"seller_country_hist":{},"avg":0.0,"qty_avg":0.0,"max":0.0,"min":0.0,"total_qty":0},
                            "sold_used_world":  {"num_lots":0,"price_histogram":{},"seller_country_hist":{},"avg":0.0,"qty_avg":0.0,"max":0.0,"min":0.0,"total_qty":0},
                            "stock_new_world":  {"num_lots":0,"price_histogram":{},"seller_country_hist":{},"avg":0.0,"qty_avg":0.0,"max":0.0,"min":0.0,"total_qty":0},
                            "sold_new_world":   {"num_lots":0,"price_histogram":{},"seller_country_hist":{},"avg":0.0,"qty_avg":0.0,"max":0.0,"min":0.0,"total_qty":0},
                            "stock_new_US":     {"num_lots":0,"price_histogram":{},"seller_country_hist":{},"avg":0.0,"qty_avg":0.0,"max":0.0,"min":0.0,"total_qty":0}
                        },
                        "color_id": cid, "element_id": None, "color_name": None
                    }

            row = {
                "rb_part_num": rb or bl,
                "bricklink": {
                    "part_no": bl,
                    "currency": args.currency,
                    "per_color": per_color
                },
                "links": {
                    "catalog": f"https://www.bricklink.com/v2/catalog/catalogitem.page?P={bl}"
                },
                "last_updated": _now_iso()
            }
            out.write(json.dumps(row, separators=(",",":"), ensure_ascii=False) + "\n")
            ok += 1

            # progress line
            pct = int((idx*100)/max(1,len(need_list)))
            sys.stdout.write(f"\rFetch [{idx}/{len(need_list)}] {pct}%  OK={ok} FAIL={fail}  RB={rb or '<none>'} BL={bl}      ")
            sys.stdout.flush()
            time.sleep(max(0.0, args.delay_ms/1000.0))

    # finalize
    os.replace(tmp_path, out_path)
    print(f"\nDone. Wrote -> {out_path}  (OK={ok} FAIL={fail}, RICH_copied={rich}, TOTAL={total})")

if __name__ == "__main__":
    main()