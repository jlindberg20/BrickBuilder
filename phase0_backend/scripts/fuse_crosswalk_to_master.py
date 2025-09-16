# phase0_backend/scripts/fuse_crosswalk_to_master.py
from __future__ import annotations

import argparse, csv, json, sys
from pathlib import Path
from typing import Dict, Any, Optional

def load_crosswalk(csv_path: Path) -> Dict[str, Dict[str, Any]]:
    xmap: Dict[str, Dict[str, Any]] = {}
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rb_num = (row.get("rb_part_num") or "").strip()
            if not rb_num:
                continue
            # Normalize “list-like” columns if present
            def clean_list(val: Optional[str]) -> list[str]:
                if not val: return []
                s = val.strip()
                if not s: return []
                # CSV may store python-like lists or comma-separated; handle both
                if s.startswith("[") and s.endswith("]"):
                    try:
                        parsed = json.loads(s)
                        return [str(x) for x in parsed if str(x).strip()]
                    except Exception:
                        pass
                return [v.strip() for v in s.split(",") if v.strip()]
            xmap[rb_num] = {
                "rb_num": rb_num,
                "rb_name": row.get("rb_name"),
                "rb_category": row.get("rb_category"),
                "rb_url": row.get("rb_url"),
                "bo_boid": (row.get("bo_boid") or "").strip(),
                "bl_id": (row.get("bl_id") or "").strip(),
                "ldraw_primary": (row.get("ldraw_primary") or "").strip(),
                "lego_primary": (row.get("lego_primary") or "").strip(),
                "bl_url": row.get("bl_url"),
                "bo_url": row.get("bo_url"),
                "ldraw_url": row.get("ldraw_url"),
                "rb_exact_match": (row.get("rb_exact_match") or "").strip().lower() in ("1","true","yes"),
                "has_mesh": (row.get("has_mesh") or "").strip().lower() in ("1","true","yes"),
                "bl_ids": clean_list(row.get("bl_ids")),
                "bo_ids": clean_list(row.get("bo_ids")),
                "ldraw_ids": clean_list(row.get("ldraw_ids")),
                "lego_ids": clean_list(row.get("lego_ids")),
                "brickset_ids": clean_list(row.get("brickset_ids")),
            }
    return xmap

def fuse_record(rec: Dict[str, Any], x: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(rec)  # shallow copy

    # Ensure expected containers exist
    out.setdefault("external_ids", {})
    out.setdefault("external_links", {})
    out.setdefault("marketplaces", {})
    out["marketplaces"].setdefault("brickowl", {})
    out["marketplaces"].setdefault("bricklink", {})

    # Carry forward prior enrichment if present
    # (We do not delete; we union below to avoid regression.)
    existing_ids = out.get("external_ids") or {}
    existing_links = out.get("external_links") or {}

    if x:
        # BrickOwl identifiers/URL
        bo_all = [id for id in x.get("bo_ids", []) if id]  # all known BO IDs
        if x.get("bo_boid"):
            # prefer canonical BOID as first item
            if x["bo_boid"] not in bo_all:
                bo_all = [x["bo_boid"]] + bo_all
            out["marketplaces"]["brickowl"]["boid"] = x["bo_boid"]
        if bo_all:
            existing_ids["BrickOwl"] = sorted(set(bo_all), key=str)
        if x.get("bo_url"):
            existing_links["brickowl"] = x["bo_url"]

        # BrickLink identifiers/URL
        bl_all = [id for id in x.get("bl_ids", []) if id]
        if x.get("bl_id"):
            if x["bl_id"] not in bl_all:
                bl_all = [x["bl_id"]] + bl_all
            out["marketplaces"]["bricklink"]["part_id"] = x["bl_id"]
        if bl_all:
            existing_ids["BrickLink"] = sorted(set(bl_all), key=str)
        if x.get("bl_url"):
            existing_links["bricklink"] = x["bl_url"]

        # Optional extras if the crosswalk provided them
        if x.get("ldraw_primary"):
            existing_ids.setdefault("LDraw", [])
            if x["ldraw_primary"] not in existing_ids["LDraw"]:
                existing_ids["LDraw"] = [x["ldraw_primary"]] + existing_ids["LDraw"]
        if x.get("ldraw_url"):
            existing_links["ldraw"] = x["ldraw_url"]

    out["external_ids"] = existing_ids
    out["external_links"] = existing_links
    return out

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-master", required=True, help="parts_with_ext.jsonl")
    ap.add_argument("--crosswalk", required=True, help="unified_crosswalk_mesh.csv")
    ap.add_argument("--out", required=True, help="master_parts.jsonl")
    ap.add_argument("--stats", required=True, help="master_parts.stats.json")
    args = ap.parse_args(argv)

    in_master = Path(args.in_master)
    crosswalk = Path(args.crosswalk)
    out_path = Path(args.out)
    stats_path = Path(args.stats)

    xmap = load_crosswalk(crosswalk)

    total = 0
    wrote = 0
    ids_any = 0
    with out_path.open("w", encoding="utf-8") as w, in_master.open("r", encoding="utf-8") as r:
        for line in r:
            if not line.strip():
                continue
            total += 1
            rec = json.loads(line)
            rb_num = rec.get("source_ids", {}).get("rb", {}).get("part_num")
            x = xmap.get(str(rb_num)) if rb_num else None
            fused = fuse_record(rec, x)
            if fused.get("external_ids"):
                ids_any += 1
            w.write(json.dumps(fused, ensure_ascii=False) + "\n")
            wrote += 1

    stats = {
        "input_records": total,
        "written_records": wrote,
        "with_any_external_ids_count": ids_any,
        "with_any_external_ids_pct": round(100.0 * ids_any / wrote, 2) if wrote else 0.0,
        "sources": {
            "master_in": str(in_master),
            "crosswalk_csv": str(crosswalk),
        },
        "outputs": {
            "master_out": str(out_path),
            "stats_out": str(stats_path),
        },
    }
    with stats_path.open("w", encoding="utf-8") as s:
        json.dump(stats, s, indent=2)
    print(json.dumps(stats, indent=2))

if __name__ == "__main__":
    sys.exit(main())
