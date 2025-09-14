from __future__ import annotations
import argparse, json, csv
from pathlib import Path
from typing import Dict, Iterable, Optional

from dotenv import load_dotenv
load_dotenv()

from phase0_backend.marketplace.brickowl_client import BrickOwlClient

def iter_rb_parts(jsonl_path: Path, limit: Optional[int]) -> Iterable[Dict]:
    n = 0
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            rec = json.loads(line)
            if rec.get("type") != "part":
                continue
            part_num = rec.get("id") or rec.get("source_ids", {}).get("rb", {}).get("part_num")
            name = rec.get("name")
            if not part_num: continue
            yield {"rb_part_num": str(part_num), "rb_name": name}
            n += 1
            if limit and n >= limit:
                break

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rb-jsonl", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--mode", choices=["live","dry"], default="live")
    args = ap.parse_args()

    rb_path = Path(args.rb_jsonl)
    out_csv = Path(args.out_csv); out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_json = Path(args.out_json); out_json.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    total = 0
    mapped = 0

    if args.mode == "dry":
        # Minimal dry-run for structure only
        fixture = {"3001":"771344","3023":"44980","6558":"99899"}
        for rec in iter_rb_parts(rb_path, args.limit):
            total += 1
            rb = rec["rb_part_num"]
            boid = fixture.get(rb)
            if boid: mapped += 1
            rows.append({
                "rb_part_num": rb,
                "rb_name": rec.get("rb_name") or "",
                "bo_part_num": boid,
                "source": "fixture" if boid else None,
                "confidence": 1.0 if boid else None,
            })
    else:
        bo = BrickOwlClient()
        for rec in iter_rb_parts(rb_path, args.limit):
            total += 1
            rb = rec["rb_part_num"]
            boid = bo.resolve_boid(rb)  # uses catalog/id_lookup only
            if boid: mapped += 1
            rows.append({
                "rb_part_num": rb,
                "rb_name": rec.get("rb_name") or "",
                "bo_part_num": boid,
                "source": "api" if boid else None,
                "confidence": 1.0 if boid else None,
            })

    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["rb_part_num","rb_name","bo_part_num","source","confidence"])
        w.writeheader(); w.writerows(rows)

    stats = {"total": total, "mapped": mapped, "coverage_pct": (100.0 * mapped / total if total else 0.0)}
    with out_json.open("w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)
    print(json.dumps(stats, indent=2))

if __name__ == "__main__":
    main()
