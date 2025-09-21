import os, sys, json, time, argparse, urllib.parse, urllib.request
from datetime import datetime, timezone
from phase0_backend.scripts.instructions_html_fallback import fetch_pdf_links_from_rebrickable_html

API_BASE = "https://rebrickable.com/api/v3/lego/building_instructions/"

def fetch_brickset_instructions(set_num: str, api_key: str, timeout: float = 15.0):
    """
    Use Brickset API to find instruction PDFs for a given set number.
    Returns: list[{"url": str, "format": "PDF", "source": "brickset_api"}]
             or [] if none.
    """
    import urllib.request, urllib.parse, json

    base = "https://brickset.com/api/v3.asmx"
    # 1) getSets: find setID
    qs = urllib.parse.urlencode({"apiKey": api_key, "setNumber": set_num})
    req1 = urllib.request.Request(f"{base}/getSets?{qs}")
    with urllib.request.urlopen(req1, timeout=timeout) as r:
        data = json.loads(r.read().decode("utf-8"))
    # Brickset returns {"status":"success","matches":N,"sets":[...]}
    if not isinstance(data, dict) or data.get("status") != "success":
        return []
    sets = data.get("sets") or []
    if not sets:
        return []
    set_id = sets[0].get("setID")
    if not set_id:
        return []

    # 2) getInstructions for that setID
    qs2 = urllib.parse.urlencode({"apiKey": api_key, "setID": set_id})
    req2 = urllib.request.Request(f"{base}/getInstructions?{qs2}")
    with urllib.request.urlopen(req2, timeout=timeout) as r2:
        d2 = json.loads(r2.read().decode("utf-8"))

    # Brickset returns {"status":"success","instructions":[{"URL": "...", ...}, ...]}
    if not isinstance(d2, dict) or d2.get("status") != "success":
        return []

    out = []
    for item in (d2.get("instructions") or []):
        url = item.get("URL") or item.get("url")
        if not url:
            continue
        # Brickset often points directly to lego.com PDFs
        fmt = "PDF" if url.lower().endswith(".pdf") else "HTML"
        out.append({"url": url, "format": fmt, "source": "brickset_api"})
    return out


def fetch_rebrickable_instructions(set_num: str, api_key: str, timeout: float = 15.0):
    params = urllib.parse.urlencode({"set_num": set_num})
    url = f"{API_BASE}?{params}"
    req = urllib.request.Request(url, headers={"Authorization": f"key {api_key}"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        if resp.status != 200:
            return None
        return json.loads(resp.read().decode("utf-8"))

def choose_primary_pdf(results):
    if not results:
        return None, []
    norm, pdf_candidates = [], []
    for r in results:
        url = r.get("url") or ""
        fmt = (r.get("format") or "").strip().upper()
        norm.append({"url": url, "format": r.get("format") or "", "source": "rebrickable"})
        if fmt == "PDF" or url.lower().endswith(".pdf"):
            pdf_candidates.append(url)
    primary = pdf_candidates[0] if pdf_candidates else None
    return primary, norm

def process(in_path, out_path, limit=None, only_missing=False, sleep_s=0.25, verbose=False):
    key = os.environ.get("REBRICKABLE_API_KEY", "").strip()
    if not key:
        print("ERROR: REBRICKABLE_API_KEY not set in environment.", file=sys.stderr)
        sys.exit(2)

    # Pre-count how many we intend to scan (for nicer progress)
    intended = 0
    with open(in_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            intended += 1
            if limit is not None and intended >= limit:
                break

    processed = 0
    upgraded = 0

    with open(in_path, "r", encoding="utf-8-sig") as fin, \
         open(out_path, "w", encoding="utf-8", newline="\n") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)

            # Respect limit
            if limit is not None and processed >= limit:
                fout.write(json.dumps(obj, ensure_ascii=False) + "")
                continue

            sid = (((obj.get("source_ids") or {}).get("rb") or {}).get("set_num") or "")
            if not sid:
                fout.write(json.dumps(obj, ensure_ascii=False) + "")
                continue

            instr = obj.get("instructions") or {}
            if only_missing and (instr.get("kind") == "pdf" and (instr.get("source") or "")):
                fout.write(json.dumps(obj, ensure_ascii=False) + "")
                processed += 1
                if verbose:
                    print(f"[{processed}/{intended}] {sid}: already pdf → skip")
                continue

            # Call API
            primary_pdf = None
            sources = []
            try:
                data = fetch_rebrickable_instructions(sid, key)
                if data and isinstance(data.get("results"), list):
                    primary_pdf, sources = choose_primary_pdf(data["results"])
            except Exception as ex:
                if verbose:
                    print(f"[{processed+1}/{intended}] {sid}: API error: {ex}")
                sources = []

            # Upgrade if possible
            if primary_pdf is None:
                # HTML fallback: scan the public set page for direct .pdf links
                html_pdfs = fetch_pdf_links_from_rebrickable_html(sid, timeout=15.0)
                if html_pdfs:
                    primary_pdf = html_pdfs[0]
                    # merge into sources list for traceability
                    for p in html_pdfs:
                        sources.append({"url": p, "format": "PDF", "source": "rebrickable_html"})
            if primary_pdf is None:
                # HTML fallback: scan the public set page for direct .pdf links
                html_pdfs = fetch_pdf_links_from_rebrickable_html(sid, timeout=15.0)
                if html_pdfs:
                    primary_pdf = html_pdfs[0]
                    # merge into sources list for traceability
                    for p in html_pdfs:
                        sources.append({"url": p, "format": "PDF", "source": "rebrickable_html"})
            if primary_pdf:
                obj.setdefault("instructions", {})
                obj["instructions"]["kind"] = "pdf"
                obj["instructions"]["source"] = primary_pdf
                upgraded += 1
                if verbose:
                    print(f"[{processed+1}/{intended}] {sid}: ✅ PDF")
            else:
                if verbose:
                    print(f"[{processed+1}/{intended}] {sid}: no pdf (kept reference)")

            # Always stash enrichment metadata
            obj.setdefault("links", {})
            obj["links"]["instruction_sources"] = sources
            obj["links"]["instructions_api_enriched"] = True
            obj["links"]["instructions_last_checked"] = datetime.now(timezone.utc).isoformat()

            fout.write(json.dumps(obj, ensure_ascii=False) + "")
            processed += 1
            time.sleep(sleep_s)

    print(f"Done. Records scanned: {processed} | Upgraded to PDF: {upgraded} | out={out_path}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inn", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--only-missing", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    process(args.inn, args.out, limit=args.limit, only_missing=args.only_missing, sleep_s=args.sleep, verbose=args.verbose)

if __name__ == "__main__":
    main()




