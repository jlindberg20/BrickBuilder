import os, json, time, hmac, base64, hashlib, random, string
from urllib.parse import quote, urlencode
from datetime import datetime, timezone
import urllib.request

API_ROOT = "https://api.bricklink.com/api/store/v1"

def pct(s): return quote(s, safe="~-._")

def oauth_header(method, url, query):
    need = ["BRICKLINK_CONSUMER_KEY","BRICKLINK_CONSUMER_SECRET","BRICKLINK_TOKEN","BRICKLINK_TOKEN_SECRET"]
    miss = [k for k in need if not os.environ.get(k)]
    if miss: raise SystemExit("Missing env: " + ", ".join(miss))
    ck = os.environ["BRICKLINK_CONSUMER_KEY"]; cs = os.environ["BRICKLINK_CONSUMER_SECRET"]
    tk = os.environ["BRICKLINK_TOKEN"];        ts = os.environ["BRICKLINK_TOKEN_SECRET"]
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

def bl_get(path, query=None):
    url = API_ROOT + path
    qs  = "" if not query else "?" + urlencode(query)
    req = urllib.request.Request(url+qs, method="GET")
    req.add_header("Authorization", oauth_header("GET", url, query or {}))
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))

part = "11217"
currency = "USD"

# Common BL color IDs to try (white, black, red, blue, DBG, LBG, tan, etc.)
common_colors = [1, 2, 3, 4, 5, 7, 8, 9, 11, 14, 15, 19, 21, 22, 23, 24, 25, 26,
                 27, 28, 33, 34, 36, 40, 41, 42, 85, 86, 88, 95, 99, 102, 110, 112, 115, 120]

def probe(color_id, guide, cond, country=None):
    q = {"color_id": color_id, "guide_type": guide, "new_or_used": cond, "currency_code": currency}
    if country: q["country_code"] = country
    r = bl_get(f"/items/part/{part}/price", q)
    d = r.get("data") or {}
    return int(d.get("total_lots") or 0), int(d.get("total_quantity") or 0)

results = {}
for cid in common_colors:
    row = {}
    for label,guide,cond,country in [
        ("stock_used_world","stock","U",None),
        ("sold_used_world","sold","U",None),
        ("stock_new_world","stock","N",None),
        ("sold_new_world","sold","N",None),
        ("stock_new_US","stock","N","US"),
        ("sold_new_US","sold","N","US"),
    ]:
        lots, qty = probe(cid, guide, cond, country)
        row[label] = {"lots": lots, "qty": qty}
    # keep only if ANY is non-zero
    if any(v["lots"] or v["qty"] for v in row.values()):
        results[cid] = row

print(json.dumps({"part": part, "colors_found": sorted(results.keys()), "details": results}, indent=2))
