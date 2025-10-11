# repair_marketplace.py
# Rebuild missing BrickLink payloads into a fresh marketplace JSONL.
# - Copies all "rich" rows verbatim.
# - Re-fetches only the missing/null rows and appends them with identical schema.

import os, sys, time, json, math, hmac, base64, uuid, hashlib, urllib.parse
from datetime import datetime, timezone

try:
    import requests
except Exception as e:
    print("ERROR: This script requires the 'requests' package. Install with: pip install requests")
    sys.exit(1)

# Optional progress bar
try:
    from tqdm import tqdm
except Exception:
    tqdm = None


# ---------------------------
# Config / paths
# ---------------------------
REPO = os.path.abspath(os.path.dirname(__file__))
IN_FILE  = os.path.join(REPO, r"data\processed\master\master_parts.marketplace.jsonl")
OUT_FILE = os.path.join(REPO, r"data\processed\master\master_parts.marketplace.NEW.jsonl")
BACKUP   = os.path.join(REPO, r"data\processed\master\master_parts.marketplace.jsonl.bak_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S"))

BL_BASE = "https://api.bricklink.com/api/store/v1"
CURRENCY = "USD"

# slices to build -> (key_name, guide_type, new_or_used, country_code)
SLICE_SPECS = [
    ("stock_used_world", "stock", "U", None),
    ("sold_used_world",  "sold",  "U", None),
    ("stock_new_world",  "stock", "N", None),
    ("sold_new_world",   "sold",  "N", None),
    ("stock_new_US",     "stock", "N", "US"),
    ("sold_new_US",      "sold",  "N", "US"),
]

# ---------------------------
# Helpers
# ---------------------------

def load_env_or_dotenv():
    """Ensure OAuth env vars exist. If missing, attempt to load from .env in repo root."""
    need = [
        "BRICKLINK_CONSUMER_KEY",
        "BRICKLINK_CONSUMER_SECRET",
        "BRICKLINK_TOKEN",
        "BRICKLINK_TOKEN_SECRET",
    ]
    missing = [k for k in need if not os.environ.get(k)]
    if not missing:
        return

    # Try reading .env manually (no third-party dependency)
    dot = os.path.join(REPO, ".env")
    if os.path.exists(dot):
        with open(dot, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and v and k in need and not os.environ.get(k):
                    os.environ[k] = v

    missing = [k for k in need if not os.environ.get(k)]
    if missing:
        raise RuntimeError(f"Missing BrickLink OAuth env vars: {', '.join(missing)}")


def percent_escape(s: str) -> str:
    return urllib.parse.quote(s, safe="~")


def oauth1_header(method: str, url: str, extra_params: dict):
    """Build OAuth1 HMAC-SHA1 Authorization header for BrickLink."""
    ck = os.environ["BRICKLINK_CONSUMER_KEY"]
    cs = os.environ["BRICKLINK_CONSUMER_SECRET"]
    tk = os.environ["BRICKLINK_TOKEN"]
    ts = os.environ["BRICKLINK_TOKEN_SECRET"]

    oauth_params = {
        "oauth_consumer_key": ck,
        "oauth_nonce": uuid.uuid4().hex,
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": str(int(time.time())),
        "oauth_token": tk,
        "oauth_version": "1.0",
    }

    # Collect and normalize params
    qp = dict(extra_params or {})
    for k, v in oauth_params.items():
        qp[k] = v

    # Normalize
    items = []
    for k in sorted(qp.keys()):
        items.append(f"{percent_escape(k)}={percent_escape(str(qp[k]))}")
    param_str = "&".join(items)

    base_elems = [
        method.upper(),
        percent_escape(url.lower()),
        percent_escape(param_str),
    ]
    base_str = "&".join(base_elems)

    signing_key = f"{percent_escape(cs)}&{percent_escape(ts)}"
    digest = hmac.new(signing_key.encode("utf-8"), base_str.encode("utf-8"), hashlib.sha1).digest()
    signature = base64.b64encode(digest).decode("ascii")

    oauth_params["oauth_signature"] = signature

    # Build header
    kv = []
    for k in ["oauth_consumer_key","oauth_nonce","oauth_signature","oauth_signature_method","oauth_timestamp","oauth_token","oauth_version"]:
        kv.append(f'{k}="{percent_escape(oauth_params[k])}"')
    header = "OAuth " + ", ".join(kv)
    return header


def bl_get(path: str, params: dict):
    url = f"{BL_BASE}{path}"
    hdr = {"Authorization": oauth1_header("GET", url, params)}
    r = requests.get(url, headers=hdr, params=params, timeout=30)
    # BrickLink returns JSON with 'meta' and 'data'
    r.raise_for_status()
    return r.json()


def extract_partno_from_line(line: str):
    # Try catalog URL query ?P=xxxxx
    # Fallback to rb_part_num if present
    part = None
    if "?P=" in line:
        try:
            after = line.split("?P=", 1)[1]
            part = after.split('"', 1)[0]
        except Exception:
            part = None
    if not part:
        # Try rb_part_num
        try:
            # naive small parse (not robust for escaped quotes, but fine for our lines)
            before = line.split('"rb_part_num"', 1)[1]
            after_colon = before.split(":", 1)[1].lstrip()
            if after_colon[0] == '"':
                part = after_colon.split('"', 2)[1]
        except Exception:
            part = None
    return part


def is_rich_line(line: str) -> bool:
    """Heuristic: has a 'bricklink' object AND at least one nonzero total or any price_detail present."""
    if '"bricklink":null' in line:
        return False
    if '"bricklink":' not in line:
        return False
    # If it has a nonzero 'num_lots' or 'total_qty' or any 'price_detail', call it rich
    if '"price_detail"' in line:
        return True
    if '"num_lots":' in line or '"total_qty":' in line:
        # Could be zeros though; but acceptable—we trust anything that had been built as "rich"
        return True
    return False


def bin_histogram(values, bins=10):
    if not values:
        return {}, None, None
    lo, hi = min(values), max(values)
    if hi == lo:
        # single bucket
        bucket = f"{round(lo, 2)}-{round(hi, 2)}"
        return {bucket: len(values)}, lo, hi
    step = (hi - lo) / bins
    edges = [lo + i * step for i in range(bins)]
    edges.append(hi)
    counts = {f"{round(edges[i], 2)}-{round(edges[i+1], 2)}": 0 for i in range(bins)}
    # place each value
    for v in values:
        if v >= edges[-1]:
            idx = bins - 1
        else:
            idx = int((v - lo) / step)
            if idx >= bins:
                idx = bins - 1
        key = list(counts.keys())[idx]
        counts[key] += 1
    return counts, lo, hi


def shape_slice(pg_json):
    """Map BrickLink priceguide meta/data into our compact slice schema."""
    # Expect structure: {"meta": {...}, "data": {"price_detail":[...], "avg_price":..., "qty_avg_price":..., "min_price":..., "max_price":..., "total_lots":..., "total_qty":...}}
    data = pg_json.get("data") or {}
    details = data.get("price_detail") or []

    # Prices & qtys for histogram
    unit_prices = []
    seller_country_hist = {}
    total_qty = 0

    for d in details:
        # unit_price, qty, seller_country
        p = d.get("unit_price")
        q = d.get("qty")
        c = d.get("seller_country")
        try:
            if p is not None:
                unit_prices.append(float(p))
        except Exception:
            pass
        try:
            if isinstance(q, (int, float)):
                total_qty += int(q)
        except Exception:
            pass
        if c:
            seller_country_hist[c] = seller_country_hist.get(c, 0) + 1

    price_hist, lo, hi = bin_histogram(unit_prices, 10)

    # Some APIs return strings; normalize to numbers or None
    def num(x):
        try:
            return float(x) if x is not None else None
        except Exception:
            return None

    out = {
        "num_lots":         int(data.get("total_lots") or 0),
        "price_histogram":  price_hist,
        "seller_country_hist": seller_country_hist,
        "avg":              num(data.get("avg_price")),
        "qty_avg":          num(data.get("qty_avg_price")),
        "max":              num(data.get("max_price")),
        "min":              num(data.get("min_price")),
        "total_qty":        int(data.get("total_qty") or total_qty or 0),
    }
    # normalize zeros as 0 (not None)
    for k in ["avg","qty_avg","max","min"]:
        if out[k] is None:
            out[k] = 0
    return out


def fetch_colors(part_no: str):
    """Return list of color_id ints for this part; fall back to [0] if none."""
    try:
        js = bl_get(f"/items/part/{part_no}/colors", {})
        dat = js.get("data") or []
        ids = []
        for row in dat:
            cid = row.get("color_id")
            if isinstance(cid, int):
                ids.append(cid)
        # Deduplicate, limit (safety)
        ids = sorted(set(ids))
        if not ids:
            ids = [0]  # unknown / generic
        return ids
    except Exception:
        return [0]


def fetch_slice(part_no: str, color_id: int, guide_type: str, new_or_used: str, country_code: str | None):
    params = {
        "color_id": color_id,
        "guide_type": guide_type,   # 'stock' or 'sold'
        "new_or_used": new_or_used, # 'N' or 'U'
        "currency_code": CURRENCY,
    }
    if country_code:
        params["country_code"] = country_code
    js = bl_get(f"/items/part/{part_no}/price", params)
    return shape_slice(js)


def build_bricklink_payload(part_no: str):
    payload = {
        "part_no": part_no,
        "currency": CURRENCY,
        "per_color": {}
    }
    color_ids = fetch_colors(part_no)

    for cid in color_ids:
        slices = {}
        for key, gtype, nu, cc in SLICE_SPECS:
            try:
                slices[key] = fetch_slice(part_no, cid, gtype, nu, cc)
            except Exception:
                # leave an empty slice if fetch fails
                slices[key] = {
                    "num_lots": 0,
                    "price_histogram": {},
                    "seller_country_hist": {},
                    "avg": 0, "qty_avg": 0, "max": 0, "min": 0, "total_qty": 0,
                }
        payload["per_color"][str(cid)] = {
            "slices": slices,
            "color_id": cid,
            "element_id": None,
            "color_name": None
        }
    return payload


def classify_lines(lines):
    rich_idxs = []
    need_idxs = []
    for i, line in enumerate(lines):
        if is_rich_line(line):
            rich_idxs.append(i)
        else:
            # treat missing/null/empty per_color as needing
            need_idxs.append(i)
    return rich_idxs, need_idxs


def write_lines(path, lines):
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for ln in lines:
            f.write(ln.rstrip("\r\n") + "\n")


def main():
    print("=== BrickLink marketplace repair (Python) ===")

    # 1) Ensure OAuth env
    try:
        load_env_or_dotenv()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    # 2) Quick env print
    print("Loaded OAuth keys ->",
          "CK:", bool(os.environ.get("BRICKLINK_CONSUMER_KEY")),
          "CS:", bool(os.environ.get("BRICKLINK_CONSUMER_SECRET")),
          "TK:", bool(os.environ.get("BRICKLINK_TOKEN")),
          "TS:", bool(os.environ.get("BRICKLINK_TOKEN_SECRET")))

    # 3) Load input file
    if not os.path.exists(IN_FILE):
        print(f"ERROR: Missing input file: {IN_FILE}")
        sys.exit(1)

    with open(IN_FILE, "r", encoding="utf-8") as f:
        src_lines = [ln.rstrip("\r\n") for ln in f]

    rich_idxs, need_idxs = classify_lines(src_lines)
    print(f"File lines: {len(src_lines)}  |  RICH={len(rich_idxs)}  NEED={len(need_idxs)}")

    # 4) Backup original (safety)
    try:
        if os.path.exists(IN_FILE):
            import shutil
            shutil.copy2(IN_FILE, BACKUP)
            print(f"Backup -> {BACKUP}")
    except Exception:
        pass

    # 5) Start the new output with all RICH rows copied verbatim
    out_lines = [src_lines[i] for i in rich_idxs]
    write_lines(OUT_FILE, out_lines)
    print(f"Copied rich rows -> {len(out_lines)}  into {OUT_FILE}")

    # 6) Build a set of RB part nums already written (to avoid dupes on resume)
    have_rb = set()
    for ln in out_lines:
        try:
            obj = json.loads(ln)
            rb = obj.get("rb_part_num")
            if rb:
                have_rb.add(rb)
        except Exception:
            pass

    # 7) Prepare target list (only missing), but skip those already in OUT_FILE (resume)
    targets = []
    for i in need_idxs:
        ln = src_lines[i]
        rb = None
        try:
            obj = json.loads(ln)
            rb = obj.get("rb_part_num")
        except Exception:
            # not valid JSON? we’ll still try a string extract
            pass
        if rb and rb in have_rb:
            continue
        targets.append(i)

    total = len(targets)
    if total == 0:
        print("Nothing to repair. You're already complete.")
        return

    print(f"Will fetch {total} missing rows…")

    # 8) Iterate missing rows with a progress bar
    iterator = enumerate(targets, start=1)
    if tqdm:
        iterator = tqdm(iterator, total=total, unit="row")

    ok = 0
    fail = 0

    for idx, i in iterator:
        line = src_lines[i]
        # progress label
        rb = None
        try:
            rb = json.loads(line).get("rb_part_num")
        except Exception:
            pass

        part_no = extract_partno_from_line(line)
        if tqdm:
            iterator.set_description(f"RB={rb or '<none>'} BL={part_no or '<none>'}")
        else:
            if idx == 1 or idx % 25 == 0:
                pct = int((idx / total) * 100)
                print(f"[{idx}/{total}] {pct}%  RB={rb} BL={part_no}")

        if not part_no:
            fail += 1
            continue

        try:
            bl_payload = build_bricklink_payload(part_no)

            # keep catalog link from source line
            catalog_url = None
            try:
                src_obj = json.loads(line)
                links = src_obj.get("links") or {}
                catalog_url = links.get("catalog")
            except Exception:
                pass

            row = {
                "rb_part_num": rb,
                "bricklink": bl_payload,
                "links": {"catalog": catalog_url} if catalog_url else {},
                "last_updated": datetime.now(timezone.utc).isoformat()
            }

            # append
            with open(OUT_FILE, "a", encoding="utf-8", newline="\n") as out:
                out.write(json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n")

            # track for resume
            if rb:
                have_rb.add(rb)
            ok += 1

        except requests.HTTPError as e:
            # HTTP error; keep going
            fail += 1
        except Exception:
            fail += 1

    print(f"\nDone. Wrote: {ok} rows  |  Failed: {fail}")
    print(f"Output file: {OUT_FILE}")
    print("Next step: once satisfied, swap NEW.jsonl in place of the old marketplace file, then run your merge-to-master step.")
    print("Tip: you can re-run this script anytime; it will copy rich rows again and only fetch the still-missing ones.")
    

if __name__ == "__main__":
    main()
