import os, time, hmac, base64, argparse, json, urllib.parse, urllib.request, hashlib, random, string

API_BASE = "https://api.bricklink.com/api/store/v1"

def pct(s:str)->str:  # RFC3986 percent-encode
    return urllib.parse.quote(s, safe="~-._")

def oauth_header(method:str, url:str, extra_query=None)->str:
    ck = os.environ["BRICKLINK_CONSUMER_KEY"]
    cs = os.environ["BRICKLINK_CONSUMER_SECRET"]
    tk = os.environ["BRICKLINK_TOKEN"]
    ts = os.environ["BRICKLINK_TOKEN_SECRET"]

    oauth = {
        "oauth_consumer_key": ck,
        "oauth_token": tk,
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": str(int(time.time())),
        "oauth_nonce": "".join(random.choices(string.ascii_letters+string.digits, k=16)),
        "oauth_version": "1.0",
    }

    # Collect params for base string (query + oauth, excluding oauth_signature)
    params = {}
    if extra_query:
        params.update(extra_query)
    params.update(oauth)

    # Normalize
    norm = "&".join(f"{pct(k)}={pct(str(params[k]))}" for k in sorted(params.keys()))
    base = "&".join([method.upper(), pct(url), pct(norm)])
    key  = "&".join([pct(cs), pct(ts)])

    sig = base64.b64encode(hmac.new(key.encode("utf-8"),
                                    base.encode("utf-8"),
                                    hashlib.sha1).digest()).decode("ascii")
    oauth["oauth_signature"] = sig

    # Build header
    kv = ", ".join([f'{k}="{pct(v)}"' for k,v in oauth.items()])
    return "OAuth " + kv

def get_part(part_no:str):
    url = f"{API_BASE}/items/part/{pct(part_no)}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", oauth_header("GET", url))
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", "replace")
            return resp.status, json.loads(body)
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", "replace")
            return e.code, json.loads(body)
        except Exception:
            return e.code, {"error": str(e)}
    except Exception as e:
        return 0, {"error": str(e)}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parts", nargs="+", required=True, help="BrickLink part numbers")
    args = ap.parse_args()

    # Quick env presence check
    need = ["BRICKLINK_CONSUMER_KEY","BRICKLINK_CONSUMER_SECRET","BRICKLINK_TOKEN","BRICKLINK_TOKEN_SECRET"]
    miss = [k for k in need if not os.environ.get(k)]
    if miss:
        print("ERROR: Missing env vars:", ", ".join(miss))
        return

    for p in args.parts:
        code, data = get_part(p)
        if code == 200 and isinstance(data, dict) and "data" in data:
            d = data["data"]
            name = d.get("name")
            cat  = d.get("category_id")
            print(f"✅ {p} {code} OK  name={json.dumps(name)}  category_id={cat}")
        else:
            # Show meta/message if present to diagnose (401 etc.)
            meta = data.get("meta") if isinstance(data, dict) else None
            if meta:
                msg = f"Code={meta.get('code')} msg={meta.get('message')} desc={meta.get('description')}"
            else:
                msg = json.dumps(data)[:200]
            print(f"❌ {p} ERROR: {msg}")

if __name__ == "__main__":
    main()
