# bl_probe.py — print the RAW BrickLink price-guide response (meta + data) for a single call.
# Usage:
#   python bl_probe.py --part 69234 --color 86 --guide sold --cond N --currency USD
#   python bl_probe.py --part 69234 --color 86 --guide stock --cond U --region world
#
# Env vars required (or present in .env at repo root):
#   BRICKLINK_CONSUMER_KEY, BRICKLINK_CONSUMER_SECRET, BRICKLINK_TOKEN, BRICKLINK_TOKEN_SECRET

import os, sys, time, json, base64, hmac, hashlib
import urllib.parse, urllib.request
from collections import OrderedDict
import argparse

API_BASE = "https://api.bricklink.com/api/store/v1"

def pct(s): return urllib.parse.quote(s, safe="~-._")
def need_env():
    keys = ["BRICKLINK_CONSUMER_KEY","BRICKLINK_CONSUMER_SECRET","BRICKLINK_TOKEN","BRICKLINK_TOKEN_SECRET"]
    miss = [k for k in keys if not os.getenv(k)]
    if miss and os.path.exists(".env"):
        for line in open(".env", "r", encoding="utf-8", errors="ignore"):
            line=line.strip()
            if not line or line.startswith("#") or "=" not in line: continue
            k,v = line.split("=",1); k=k.strip(); v=v.strip().strip('"').strip("'")
            if k and v and not os.getenv(k): os.environ[k]=v
        miss = [k for k in keys if not os.getenv(k)]
    if miss:
        print("Missing env: " + ", ".join(miss), file=sys.stderr); sys.exit(2)

def oauth_header(method, url, query):
    ck,cs,tk,ts = (os.environ[x] for x in ["BRICKLINK_CONSUMER_KEY","BRICKLINK_CONSUMER_SECRET","BRICKLINK_TOKEN","BRICKLINK_TOKEN_SECRET"])
    nonce = "".join(__import__("random").choice("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(16))
    ts_sec = str(int(time.time()))
    params = OrderedDict([
        ("oauth_consumer_key", ck),
        ("oauth_token", tk),
        ("oauth_nonce", nonce),
        ("oauth_timestamp", ts_sec),
        ("oauth_signature_method", "HMAC-SHA1"),
        ("oauth_version", "1.0"),
    ])
    for k in sorted(query.keys()):
        params[k] = str(query[k])
    param_str = "&".join(f"{pct(k)}={pct(v)}" for k,v in params.items())
    base_str  = "&".join([method.upper(), pct(url.lower()), pct(param_str)])
    key       = f"{pct(cs)}&{pct(ts)}"
    sig       = base64.b64encode(hmac.new(key.encode(), base_str.encode(), hashlib.sha1).digest()).decode()
    return ('OAuth ' +
            f'oauth_consumer_key="{ck}",' +
            f'oauth_token="{tk}",' +
            'oauth_signature_method="HMAC-SHA1",' +
            f'oauth_signature="{pct(sig)}",' +
            f'oauth_timestamp="{ts_sec}",' +
            f'oauth_nonce="{nonce}",' +
            'oauth_version="1.0"')

def bl_get(endpoint, q):
    url = API_BASE + endpoint
    qs  = urllib.parse.urlencode({k:str(v) for k,v in sorted(q.items())}) if q else ""
    req = urllib.request.Request(url + ("?"+qs if qs else ""), method="GET")
    req.add_header("Authorization", oauth_header("GET", url, q))
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode("utf-8", "ignore"))
        except Exception:
            return {"meta":{"code":e.code,"message":str(e)}}
    except Exception as e:
        return {"meta":{"code":-1,"message":str(e)}}

def main():
    need_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("--part", required=True, help="BrickLink part_no (e.g., 69234, 6191pb042)")
    ap.add_argument("--color", type=int, required=True, help="color_id (try 0 if unknown)")
    ap.add_argument("--guide", choices=["sold","stock"], required=True, help="sold | stock")
    ap.add_argument("--cond", choices=["N","U"], required=True, help="N=new, U=used")
    ap.add_argument("--currency", default="USD")
    group = ap.add_mutually_exclusive_group()
    group.add_argument("--country", help="country code like US")
    group.add_argument("--region", default="world", help="region=world (default)")
    args = ap.parse_args()

    q = {
        "new_or_used": args.cond,
        "guide_type": args.guide,
        "currency_code": args.currency,
        "color_id": args.color,
    }
    if args.country: q["country_code"] = args.country
    else: q["region"] = args.region

    resp = bl_get(f"/items/part/{args.part}/price", q)
    print(json.dumps(resp, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
