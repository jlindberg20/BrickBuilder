import os, re, json, time, hmac, base64, hashlib, random, string, argparse
from datetime import datetime, timezone
from urllib.parse import urlencode, quote

API_ROOT = "https://api.bricklink.com/api/store/v1"
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

def pct(s): return quote(s, safe="~-._")
def nowz():  return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def need_env_or_die():
    miss = [k for k in REQUIRED_ENV if not os.environ.get(k)]
    if miss:
        raise SystemExit("Missing BL env: " + ", ".join(miss))

def oauth_header(method, url, query):
    oauth = {
        "oauth_consumer_key": os.environ["BRICKLINK_CONSUMER_KEY"],
        "oauth_token": os.environ["BRICKLINK_TOKEN"],
        "oauth_nonce": "".join(random.choice(string.ascii_letters+string.digits) for _ in range(16)),
        "oauth_timestamp": str(int(time.time())),
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_version": "1.0",
    }
    params = dict(oauth); params.update(query or {})
    items = sorted((k, str(v)) for k,v in params.items())
    param_str = "&".join(f"{pct(k)}={pct(v)}" for k,v in items)
    base = "&".join([method.upper(), pct(url.lower()), pct(param_str)])
    key  = f"{pct(os.environ['BRICKLINK_CONSUMER_SECRET'])}&{pct(os.environ['BRICKLINK_TOKEN_SECRET'])}".encode("utf-8")
    sig  = hmac.new(key, base.encode("utf-8"), hashlib.sha1).digest()
    oauth["oauth_signature"] = base64.b64encode(sig).decode("ascii")
    return "OAuth " + ", ".join(f'{k}="{pct(v)}"' for k,v in oauth.items())

def bl_get(path, query=None, timeout=20, delay_ms=0):
    if delay_ms: time.sleep(delay_ms/1000.0)
    import urllib.request
    url = API_ROOT + path
    qs  = "" if not query else "?" + urlencode(query)
    req = urllib.request.Request(url+qs, method="GET")
    req.add_header("Authorization", oauth_header("GET", url, query or {}))
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode("utf-8")
        return json.loads(body)

def get_colors(part, timeout, delay_ms):
    try:
        r = bl_get(f"/items/part/{part}/colors", timeout=timeout, delay_ms=delay_ms)
        data = r.get("data") or []
        ids = [int(x.get("color_id")) for x in data if x and x.get("color_id") is not None]
        ids = sorted(set(ids))
        return ids if ids else []
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

    prices = [fnum(x.get("unit_price")) for x in detail if x]
    qtys   = [int(x.get("quantity") or 0) for x in detail if x]

    lots  = int(d.get("total_lots") or (len(detail)))
    tqty  = int(d.get("total_quantity") or (sum(qtys) if qtys else 0))
    avg   = fnum(d.get("avg_price"))
    qty_avg = fnum(d.get("qty_avg_price") or 0)
    maxp = max(prices) if prices else fnum(d.get("max_price"))
    minp = min(prices) if prices else fnum(d.get("min_price"))

    # bin to 10 buckets like we validated
    from collections import Counter
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
    else:
        price_hist = {}

    sc = {}
    for x in (detail or []):
        cc = x.get("seller_country_code") or x.get("country_code") or x.get("seller_country")
        if cc:
            sc[cc] = sc.get(cc,0) + 1

    return {
        "num_lots": lots,
        "price_histogram": price_hist,
        "seller_country_hist": sc,
        "avg": avg,
        "qty_avg": qty_avg,
        "max": maxp,
        "min": minp,
        "total_qty": tqty
    }

def build_for_part(part, currency, timeout, delay_ms, max_slices):
    # discover colors
    colors = get_colors(part, timeout, delay_ms)
    if not colors:
        colors = COMMON_COLORS[:]  # fallback
    if 86 in colors:
        colors = [86] + [c for c in colors if c != 86]

    chosen = None
    totals = None
    for cid in colors:
        active, t = probe_color_activity(part, cid, currency, timeout, delay_ms)
        if active:
            chosen = cid
            totals = t
            break
    if chosen is None:
        chosen = colors[0]
        totals = {k:(0,0) for k,_,_,_ in SLICE_SPECS}

    slices = {}
    filled = 0
    for key, guide, cond, country in SLICE_SPECS:
        lots, qty = totals.get(key, (0,0))
        if lots or qty:
            if filled < max_slices:
                try:
                    s = fetch_price_detail(part, chosen, currency, guide, cond, country, timeout, delay_ms)
                except Exception:
                    s = {"num_lots":lots,"price_histogram":{},"seller_country_hist":{},"avg":0,"qty_avg":0,"max":0,"min":0,"total_qty":qty}
                filled += 1
            else:
                s = {"num_lots":lots,"price_histogram":{},"seller_country_hist":{},"avg":0,"qty_avg":0,"max":0,"min":0,"total_qty":qty}
        else:
            s = {"num_lots":0,"price_histogram":{},"seller_country_hist":{},"avg":0,"qty_avg":0,"max":0,"min":0,"total_qty":0}
        slices[key] = s

    row = {
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
    return row

def parse_line_for_partno(line):
    # try link P=xxxx first
    m = re.search(r'catalogitem\.page\?P=([^"\\}]+)', line)
    if m: return m.group(1)
    # try explicit bricklink.part_no
    m = re.search(r'"bricklink"\s*:\s*\{[^}]*"part_no"\s*:\s*"([^"]+)"', line)
    if m: return m.group(1)
    # fallback rb_part_num
    m = re.search(r'"rb_part_num"\s*:\s*"([^"]+)"', line)
    if m: return m.group(1)
    return None

def is_rich(line):
    # consider rich if there is at least one price_histogram with any buckets OR total lots>0 captured in text
    if '"per_color":{}' in line: return False
    if re.search(r'"price_histogram"\s*:\s*\{[^}]+\}', line): return True
    # quick numeric non-zero hints
    if re.search(r'"total_qty"\s*:\s*(?!0)\d', line): return True
    if re.search(r'"num_lots"\s*:\s*(?!0)\d', line): return True
    return False

def main():
    need_env_or_die()
    ap = argparse.ArgumentParser()
    ap.add_argument("--in",  dest="infile",  required=True, help="NORMALIZED jsonl")
    ap.add_argument("--out", dest="outfile", required=True, help="REPAIRED jsonl")
    ap.add_argument("--currency", default="USD")
    ap.add_argument("--timeout",  type=int, default=18)
    ap.add_argument("--delay-ms", type=int, default=150)
    ap.add_argument("--max-slices", type=int, default=6)
    ap.add_argument("--flush-every", type=int, default=25, help="flush temp file every N rows")
    args = ap.parse_args()

    infile  = args.infile
    outfile = args.outfile
    tmpfile = outfile + ".tmp"

    if not os.path.exists(infile):
        raise SystemExit(f"Missing input: {infile}")

    # read all lines once
    with open(infile,'r',encoding='utf-8',errors='replace',newline='') as f:
        lines = f.read().splitlines()

    rich, need = [], []
    for i, line in enumerate(lines):
        if is_rich(line):
            rich.append(line)
        else:
            part = parse_line_for_partno(line)
            if part:
                need.append((i, part, line))

    # start tmp fresh
    if os.path.exists(tmpfile):
        os.remove(tmpfile)
    # copy rich rows verbatim
    with open(tmpfile,'w',encoding='utf-8',newline='\n') as out:
        for ln in rich:
            out.write(ln + "\n")

    print(f"RICH copied: {len(rich)}   NEED fetching: {len(need)}")

    ok = fail = 0
    last_flush = 0
    with open(tmpfile,'a',encoding='utf-8',newline='\n') as out:
        total = len(need)
        for j,(idx, part, raw) in enumerate(need, start=1):
            try:
                row = build_for_part(part, args.currency, args.timeout, args.delay_ms, args.max_slices)
                out.write(json.dumps(row, separators=(",",":")) + "\n")
                ok += 1
            except Exception as e:
                # write a minimal stub so line counts keep pace (or skip; your call)
                stub = {
                  "rb_part_num": part,
                  "bricklink": {"part_no": part, "currency": args.currency, "per_color": {}},
                  "links": {"catalog": f"https://www.bricklink.com/v2/catalog/catalogitem.page?P={part}"},
                  "last_updated": nowz(),
                  "error": str(e)
                }
                out.write(json.dumps(stub, separators=(",",":")) + "\n")
                fail += 1

            if j - last_flush >= args.flush_every:
                out.flush()
                os.fsync(out.fileno())
                last_flush = j

            pct = int(j*100/total) if total else 100
            print(f"Fetch [{j}/{total}] {pct}%  OK={ok} FAIL={fail}  BL={part}", flush=True)

    # atomically move into place
    if os.path.exists(outfile):
        os.remove(outfile)
    os.replace(tmpfile, outfile)
    print(f"Done -> {outfile}   OK={ok} FAIL={fail}")

if __name__ == "__main__":
    main()
