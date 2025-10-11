import os, sys, json, time, hmac, base64, hashlib, random, string
from urllib.parse import quote, urlencode
from datetime import datetime, timezone
import argparse, urllib.request

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

def bl_get(path, query=None, retries=3, delay=0.4):
    url = API_ROOT + path
    qs  = "" if not query else "?" + urlencode(query)
    req = urllib.request.Request(url+qs, method="GET")
    req.add_header("Authorization", oauth_header("GET", url, query or {}))
    req.add_header("Content-Type", "application/json")
    last = None
    for _ in range(retries+1):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            last = e; time.sleep(delay)
    raise last

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--part", required=True, nargs="+", help="BrickLink part_no(s) e.g. 14718pb074")
    ap.add_argument("--currency", default="USD")
    args = ap.parse_args()
    print(f"Env -> CK={bool(os.environ.get('BRICKLINK_CONSUMER_KEY'))} "
          f"CS={bool(os.environ.get('BRICKLINK_CONSUMER_SECRET'))} "
          f"TK={bool(os.environ.get('BRICKLINK_TOKEN'))} "
          f"TS={bool(os.environ.get('BRICKLINK_TOKEN_SECRET'))}")

    for p in args.part:
        try:
            colors = bl_get(f"/items/part/{p}/colors", {})
            meta   = colors.get("meta", {})
            if int(meta.get("code",0)) != 200:
                print(f"❌ {p}  colors code={meta.get('code')} msg={meta.get('message')} desc={meta.get('description')}")
                continue
            data = colors.get("data") or []
            cids = [int(x["color_id"]) for x in data if x.get("color_id") is not None]
            if not cids: cids = [0]
            print(f"\n=== {p} colors === {cids}")

            for cid in cids:
                print(f"  -- color_id={cid} --")
                for label,guide,cond,country in [
                    ("stock_used_world","stock","U",None),
                    ("sold_used_world","sold","U",None),
                    ("stock_new_world", "stock","N",None),
                    ("sold_new_world",  "sold","N",None),
                    ("stock_new_US",    "stock","N","US"),
                    ("sold_new_US",     "sold","N","US"),
                ]:
                    q = {"color_id": cid, "guide_type": guide, "new_or_used": cond, "currency_code": args.currency}
                    if country: q["country_code"] = country
                    resp = bl_get(f"/items/part/{p}/price", q)
                    m = resp.get("meta",{})
                    d = resp.get("data",{}) or {}
                    lots = int(d.get("total_lots") or 0)
                    qty  = int(d.get("total_quantity") or 0)
                    code = int(m.get("code",0))
                    print(f"    {label}: code={code} lots={lots} qty={qty}")
        except Exception as e:
            print(f"❌ {p} ERROR: {e}")

if __name__ == "__main__":
    main()
