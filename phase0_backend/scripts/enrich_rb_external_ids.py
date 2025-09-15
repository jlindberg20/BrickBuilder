from __future__ import annotations
import os, json, time, random, argparse
from pathlib import Path
from typing import Dict, Optional, Iterator
import requests
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

API_KEY = os.getenv("REBRICKABLE_API_KEY")
BASE = "https://rebrickable.com/api/v3/lego"
HEADERS = {"Authorization": f"key {API_KEY}"} if API_KEY else {}

def rb_search_part_by_name(name: str, page_size: int = 5) -> Optional[Dict]:
    r = requests.get(
        f"{BASE}/parts/",
        params={"search": name, "page_size": page_size},
        headers=HEADERS,
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    results = data.get("results") or []
    return results[0] if results else None

def has_mesh(rec: Dict) -> bool:
    g = rec.get("geometry") or {}
    if g.get("mesh"):  # canonical single mesh
        return True
    assets = g.get("assets") or []
    return any(a.get("format") == "obj" for a in assets)

def iter_eligible(path: Path, only_with_mesh: bool) -> Iterator[Dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)
            if rec.get("type") != "part":
                continue
            if only_with_mesh and not has_mesh(rec):
                continue
            yield rec

def precount(path: Path, only_with_mesh: bool, hard_limit: Optional[int]) -> int:
    if hard_limit is not None:
        return hard_limit
    n = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("type") != "part":
                continue
            if only_with_mesh and not has_mesh(rec):
                continue
            n += 1
    return n

def main():
    ap = argparse.ArgumentParser(description="Enrich RB parts with external_ids via Rebrickable name search, with progress bar.")
    ap.add_argument("--rb-jsonl", required=True, help="data/processed/rebrickable/parts_with_mesh_mm.jsonl")
    ap.add_argument("--out-jsonl", required=True, help="data/processed/rebrickable/parts_with_ext.jsonl")
    ap.add_argument("--limit", type=int, default=None, help="Max eligible parts to process (default all eligible)")
    ap.add_argument("--rpm", type=float, default=180.0, help="Requests per minute cap")
    ap.add_argument("--retry", type=int, default=3, help="Max retries on HTTP 5xx/429")
    ap.add_argument("--only_missing", action="store_true", help="Skip records that already have external_ids")
    ap.add_argument("--only-with-mesh", action="store_true", help="Process only parts that have geometry.mesh/assets")
    args = ap.parse_args()

    if not API_KEY:
        raise SystemExit("Missing REBRICKABLE_API_KEY in environment (.env).")

    src = Path(args.rb_jsonl)
    dst = Path(args.out_jsonl)
    dst.parent.mkdir(parents=True, exist_ok=True)

    # Pre-count eligible for a correct ETA
    total_target = precount(src, args.only_with_mesh, args.limit)

    min_interval = 60.0 / args.rpm if args.rpm > 0 else 0.0
    last_call_ts = 0.0

    processed = 0
    enriched = 0
    errors = 0

    # We stream source and write only the processed eligible records to out.
    # (We’re building a new file with just the enriched structural scope.)
    with dst.open("w", encoding="utf-8") as fout:
        pbar = tqdm(total=total_target, unit="part", desc="Enrich RB external_ids", dynamic_ncols=True)

        for rec in iter_eligible(src, only_with_mesh=args.only_with_mesh):
            out = dict(rec)
            do_enrich = True
            if args.only_missing:
                if (out.get("rebrickable_enrichment") or {}).get("external_ids"):
                    do_enrich = False
                if out.get("external_ids"):
                    do_enrich = False

            if do_enrich:
                # Pace by RPM
                now = time.time()
                wait = min_interval - (now - last_call_ts)
                if wait > 0:
                    time.sleep(wait)
                last_call_ts = time.time()

                ok = False
                err_msg = None
                for attempt in range(1, args.retry + 1):
                    try:
                        hit = rb_search_part_by_name(out.get("name") or "")
                        ok = True
                        break
                    except requests.HTTPError as e:
                        code = getattr(e.response, "status_code", 0)
                        if code in (429, 500, 502, 503, 504):
                            time.sleep(min(10.0, 2 ** (attempt - 1)) + random.random())
                            continue
                        err_msg = f"http {code}"
                        break
                    except Exception as e:
                        err_msg = f"unexpected {type(e).__name__}: {e}"
                        break

                if ok:
                    subset = {}
                    if hit:
                        subset = {
                            "rb_search_part_num": hit.get("part_num"),
                            "external_ids": hit.get("external_ids") or {},
                        }
                        if subset["external_ids"]:
                            enriched += 1
                    out.setdefault("rebrickable_enrichment", {}).update(subset)
                else:
                    out.setdefault("rebrickable_enrichment", {})["error"] = err_msg or "unknown_error"
                    errors += 1

            fout.write(json.dumps(out) + "\n")
            processed += 1
            pbar.set_postfix_str(f"enriched={enriched} errors={errors}")
            pbar.update(1)

            if args.limit is not None and processed >= args.limit:
                break

        pbar.close()

    print(json.dumps({"processed": processed, "enriched_with_external_ids": enriched, "errors": errors, "rpm": args.rpm, "mesh_scope": args.only_with_mesh}, indent=2))

if __name__ == "__main__":
    main()
