from __future__ import annotations
import argparse, json, csv
from pathlib import Path
from typing import Dict

from dotenv import load_dotenv
load_dotenv()

def load_crosswalk(csv_path: Path) -> Dict[str, Dict]:
    out = {}
    with csv_path.open("r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rb = (row.get("rb_part_num") or "").strip()
            if rb:
                out[rb] = row
    return out

def enrich_record(rec: Dict, cw: Dict[str, Dict]) -> Dict:
    rb_num = rec.get("id") or rec.get("source_ids", {}).get("rb", {}).get("part_num")
    rb_num = str(rb_num) if rb_num is not None else None
    if not rb_num:
        return rec

    m = cw.get(rb_num)
    if not m or not m.get("bo_part_num"):
        return rec

    rec.setdefault("source_ids", {}).setdefault("rb", {})
    rec["source_ids"].setdefault("bo", {})
    rec.setdefault("external_links", {})  # left empty for BO until lookup is permitted
    rec.setdefault("metadata", {}).setdefault("checks", [])
    rec.setdefault("provenance", {})

    rec["source_ids"]["rb"]["part_num"] = rb_num
    rec["source_ids"]["bo"]["part_num"] = m["bo_part_num"]

    checks = rec["metadata"]["checks"]
    if "marketplace_enriched:v1" not in checks:
        checks.append("marketplace_enriched:v1")
    rec["provenance"]["source_ids.bo"] = {
        "source": f"brickowl:{m.get('source')}",
        "confidence": float(m.get("confidence") or 0.0),
    }
    return rec

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rb-jsonl", required=True)
    ap.add_argument("--crosswalk", required=True)
    ap.add_argument("--out-master", required=True)
    args = ap.parse_args()

    cw = load_crosswalk(Path(args.crosswalk))
    outp = Path(args.out_master)
    outp.parent.mkdir(parents=True, exist_ok=True)

    n_total = n_enriched = 0
    with open(args.rb_jsonl, "r", encoding="utf-8") as inp, outp.open("w", encoding="utf-8") as out:
        for line in inp:
            if not line.strip(): continue
            rec = json.loads(line)
            n_total += 1
            newrec = enrich_record(rec, cw)
            if newrec is not rec:
                n_enriched += 1
            out.write(json.dumps(newrec, ensure_ascii=False) + "\n")

    print(json.dumps({"total": n_total, "enriched": n_enriched}, indent=2))

if __name__ == "__main__":
    main()
