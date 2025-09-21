import os, sys, json, time, argparse, urllib.request, urllib.error
from datetime import datetime

API_BASE = "https://rebrickable.com/api/v3/lego/sets/{set_num}/"

def fetch_set(set_num: str, api_key: str, retries=3, backoff=0.7):
    url = API_BASE.format(set_num=set_num)
    req = urllib.request.Request(url, headers={"Authorization": f"key {api_key}"})
    for attempt in range(1, retries+1):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(backoff * attempt)
                continue
            raise
        except Exception:
            if attempt < retries:
                time.sleep(backoff * attempt)
                continue
            raise

def choose_instruction_urls(api_obj: dict):
    # Rebrickable returns "instructions": [{"url": "...", "description": "..."}] sometimes missing.
    instr = api_obj.get("instructions") or []
    urls = []
    for item in instr:
        u = (item or {}).get("url")
        if not u: 
            continue
        desc = (item or {}).get("description") or ""
        urls.append({"url": u, "desc": desc})
    # Prefer a PDF for main "source"
    pdf = next((u for u in urls if u["url"].lower().endswith(".pdf")), None)
    return urls, (pdf["url"] if pdf else None)

def extract_set_num(rec: dict) -> str:
    # Prefer source_ids.rb.set_num; fallback to id prefix "model:rb:<set_num>"
    sn = ((rec.get("source_ids") or {}).get("rb") or {}).get("set_num")
    if sn:
        return sn
    rid = rec.get("id") or ""
    if rid.startswith("model:rb:"):
        return rid[len("model:rb:"):]
    return ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--set-num", required=True)
    ap.add_argument("--inn", required=True, help="input JSONL")
    ap.add_argument("--out", required=True, help="output JSONL")
    args = ap.parse_args()

    api_key = os.environ.get("REBRICKABLE_API_KEY")
    if not api_key:
        print("ERROR: REBRICKABLE_API_KEY not set in environment", file=sys.stderr)
        sys.exit(2)

    # Fetch instruction metadata once
    try:
        api_obj = fetch_set(args.set_num, api_key)
    except Exception as e:
        print(f"ERROR: API fetch failed for {args.set_num}: {e}", file=sys.stderr)
        sys.exit(3)

    all_urls, pdf_url = choose_instruction_urls(api_obj)
    now = datetime.utcnow().isoformat() + "Z"

    total = 0
    touched = 0

    with open(args.inn, "r", encoding="utf-8") as fin, \
         open(args.out, "w", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            total += 1
            obj = json.loads(line)

            sn = extract_set_num(obj)
            if sn == args.set_num:
                # Ensure links dict exists
                links = obj.get("links") or {}
                links["instruction_sources"] = all_urls
                links["instructions_api_enriched"] = True
                links["instructions_last_checked"] = now
                obj["links"] = links

                # Instructions field: prefer pdf; else fallback to reference
                if pdf_url:
                    obj["instructions"] = {"kind": "pdf", "source": pdf_url}
                else:
                    # Keep existing if it's already reference; otherwise set a sane reference
                    if not obj.get("instructions"):
                        obj["instructions"] = {
                            "kind": "reference",
                            "source": f"https://rebrickable.com/sets/{sn}/"
                        }
                touched += 1

            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

    print(f"Done. Records processed: {total} | Enriched: {touched} | set_num={args.set_num}")
    if touched == 0:
        print("WARNING: Target set not found in input.", file=sys.stderr)

if __name__ == "__main__":
    main()
