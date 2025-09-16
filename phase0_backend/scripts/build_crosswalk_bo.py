# phase0_backend/scripts/build_crosswalk_bo.py
"""
Build a unified crosswalk from the enriched Rebrickable parts file.

Inputs
------
- data/processed/rebrickable/parts_with_ext.jsonl  (produced by enrich_rb_external_ids.py)

Outputs
-------
- data/processed/crosswalk/unified_crosswalk.csv
- data/processed/crosswalk/unified_crosswalk.jsonl
- data/processed/crosswalk/unified_crosswalk.stats.json
- data/processed/crosswalk/unified_crosswalk.log  (run log with errors/warnings)

Notes
-----
- Default behavior filters to parts that have a mesh (--only-with-mesh on by default).
- Progress is shown via tqdm.
- No external APIs are called.
"""

import argparse
import csv
import json
import logging
from pathlib import Path
from typing import Dict, Any, Iterable, List, Tuple, Optional
from tqdm import tqdm

# ---------- Utilities ----------

def read_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception as e:
                logging.exception("Failed to parse JSON line")
                continue

def to_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def norm_str(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s or None

def best_str(items: List[str]) -> Optional[str]:
    items = [i for i in items if i]
    if not items:
        return None
    # Prefer the shortest token; for prints/variants this tends to pick the base id
    items_sorted = sorted(items, key=lambda s: (len(s), s))
    return items_sorted[0]

def build_row(rec: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    # Core RB fields
    rb_num = norm_str(((rec.get("source_ids") or {}).get("rb") or {}).get("part_num"))
    name = norm_str(rec.get("name"))
    ptype = norm_str(rec.get("type"))
    category = (rec.get("category") or {}).get("name")
    ext_links = rec.get("external_links") or {}
    rb_url = ext_links.get("rebrickable") if isinstance(ext_links, dict) else None

    enr = rec.get("rebrickable_enrichment") or {}
    rb_hit = norm_str(enr.get("rb_search_part_num"))
    ext = enr.get("external_ids") or {}

    # External IDs (lists)
    bl_ids      = [norm_str(x) for x in to_list(ext.get("BrickLink")) if norm_str(x)]
    bo_ids      = [norm_str(x) for x in to_list(ext.get("BrickOwl")) if norm_str(x)]
    ldraw_ids   = [norm_str(x) for x in to_list(ext.get("LDraw")) if norm_str(x)]
    lego_ids    = [norm_str(x) for x in to_list(ext.get("LEGO")) if norm_str(x)]
    brickset_ids= [norm_str(x) for x in to_list(ext.get("Brickset")) if norm_str(x)]

    exact_rb_match = (rb_num is not None and rb_hit is not None and rb_num.lower() == rb_hit.lower())

    # Representative singletons for CSV readability
    bl_primary   = best_str(bl_ids)
    bo_primary   = best_str(bo_ids)
    ldraw_primary= best_str(ldraw_ids)
    lego_primary = best_str(lego_ids)

    # Friendly URLs where possible (skip if missing)
    bl_url = f"https://www.bricklink.com/v2/catalog/catalogitem.page?P={bl_primary}" if bl_primary else None
    bo_url = f"https://www.brickowl.com/catalog/{bo_primary}" if bo_primary else None
    ldraw_url = f"https://library.ldraw.org/parts/{ldraw_primary}" if ldraw_primary and ldraw_primary.endswith(".dat") else None

    geometry = rec.get("geometry") or {}
    has_mesh = bool(geometry.get("mesh"))

    csv_row = {
        "rb_part_num": rb_num,
        "rb_name": name,
        "rb_type": ptype,
        "rb_category": category,
        "rb_url": rb_url or "",

        "bl_ids": ";".join(bl_ids) if bl_ids else "",
        "bo_ids": ";".join(bo_ids) if bo_ids else "",
        "ldraw_ids": ";".join(ldraw_ids) if ldraw_ids else "",
        "lego_ids": ";".join(lego_ids) if lego_ids else "",
        "brickset_ids": ";".join(brickset_ids) if brickset_ids else "",

        "bl_primary": bl_primary or "",
        "bo_primary": bo_primary or "",
        "ldraw_primary": ldraw_primary or "",
        "lego_primary": lego_primary or "",

        "bl_url": bl_url or "",
        "bo_url": bo_url or "",
        "ldraw_url": ldraw_url or "",

        "rb_exact_match": "1" if exact_rb_match else "0",
        "has_mesh": "1" if has_mesh else "0",
    }

    jsonl_row = {
        "rb": {
            "part_num": rb_num,
            "name": name,
            "type": ptype,
            "category": category,
            "url": rb_url,
        },
        "external_ids": {
            "BrickLink": bl_ids or None,
            "BrickOwl": bo_ids or None,
            "LDraw": ldraw_ids or None,
            "LEGO": lego_ids or None,
            "Brickset": brickset_ids or None,
        },
        "primary": {
            "bl": bl_primary,
            "bo": bo_primary,
            "ldraw": ldraw_primary,
            "lego": lego_primary,
        },
        "urls": {
            "bricklink": bl_url,
            "brickowl": bo_url,
            "ldraw": ldraw_url,
            "rebrickable": rb_url,
        },
        "quality": {
            "rb_exact_match": exact_rb_match,
        },
        "flags": {
            "has_mesh": has_mesh,
        }
    }
    return csv_row, jsonl_row

# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser(description="Build unified crosswalk from enriched RB parts.")
    ap.add_argument("--in-jsonl", default="data/processed/rebrickable/parts_with_ext.jsonl",
                    help="Input enriched JSONL (default: data/processed/rebrickable/parts_with_ext.jsonl)")
    ap.add_argument("--out-csv", default="data/processed/crosswalk/unified_crosswalk.csv",
                    help="Output CSV path")
    ap.add_argument("--out-jsonl", default="data/processed/crosswalk/unified_crosswalk.jsonl",
                    help="Output JSONL path")
    ap.add_argument("--out-stats", default="data/processed/crosswalk/unified_crosswalk.stats.json",
                    help="Stats JSON path")
    ap.add_argument("--log-file", default="data/processed/crosswalk/unified_crosswalk.log",
                    help="Run log file path")
    ap.add_argument("--only-with-mesh", action="store_true", default=True,
                    help="Process only parts that have a mesh (DEFAULT: on)")
    ap.add_argument("--include-non-mesh", action="store_true",
                    help="Include parts without mesh (overrides --only-with-mesh)")
    args = ap.parse_args()

    # Effective mesh filter flag
    only_with_mesh = False if args.include_non_mesh else True

    in_path   = Path(args.in_jsonl)
    out_csv   = Path(args.out_csv)
    out_jsonl = Path(args.out_jsonl)
    out_stats = Path(args.out_stats)
    log_file  = Path(args.log_file)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    out_stats.parent.mkdir(parents=True, exist_ok=True)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()]
    )
    logging.info("Starting unified crosswalk build")
    logging.info("Input: %s", in_path)

    # Count for progress bar
    try:
        total_records = sum(1 for _ in read_jsonl(in_path))
    except FileNotFoundError:
        logging.error("Input not found: %s", in_path)
        print(json.dumps({"ok": False, "error": f"Input not found: {in_path}"}))
        return

    written = 0
    with_ids = 0
    exact = 0
    with_mesh_count = 0
    sample_mismatches = []

    fieldnames = [
        "rb_part_num","rb_name","rb_type","rb_category","rb_url",
        "bl_ids","bo_ids","ldraw_ids","lego_ids","brickset_ids",
        "bl_primary","bo_primary","ldraw_primary","lego_primary",
        "bl_url","bo_url","ldraw_url",
        "rb_exact_match","has_mesh"
    ]

    # Streaming pass with progress bar
    with out_csv.open("w", newline="", encoding="utf-8") as fcsv, out_jsonl.open("w", encoding="utf-8") as fj:
        writer = csv.DictWriter(fcsv, fieldnames=fieldnames)
        writer.writeheader()

        pbar = tqdm(read_jsonl(in_path), total=total_records, unit="part", desc="Unified crosswalk")
        for rec in pbar:
            try:
                geometry = rec.get("geometry") or {}
                has_mesh = bool(geometry.get("mesh"))
                if only_with_mesh and not has_mesh:
                    continue

                csv_row, jsonl_row = build_row(rec)

                any_ids = any([
                    csv_row["bl_ids"],
                    csv_row["bo_ids"],
                    csv_row["ldraw_ids"],
                    csv_row["lego_ids"],
                    csv_row["brickset_ids"]
                ])
                if any_ids:
                    with_ids += 1
                if csv_row["rb_exact_match"] == "1":
                    exact += 1
                if csv_row["has_mesh"] == "1":
                    with_mesh_count += 1

                rb_num = csv_row.get("rb_part_num")
                rb_hit = ((rec.get("rebrickable_enrichment") or {}).get("rb_search_part_num"))
                if (rb_num and rb_hit and str(rb_num).lower() != str(rb_hit).lower()) and len(sample_mismatches) < 20:
                    ext = ((rec.get("rebrickable_enrichment") or {}).get("external_ids") or {})
                    sample_mismatches.append({
                        "name": rec.get("name"),
                        "rb_num": rb_num,
                        "hit_num": rb_hit,
                        "ext_keys": sorted([k for k, v in ext.items() if v])
                    })

                writer.writerow(csv_row)
                fj.write(json.dumps(jsonl_row, ensure_ascii=False) + "\n")
                written += 1

                pbar.set_postfix(
                    written=written,
                    with_ids=with_ids,
                    exact=exact,
                    mesh=with_mesh_count
                )
            except Exception as e:
                logging.exception("Failed to process record")
                continue

    stats = {
        "input_records": total_records,
        "written_records": written,
        "with_any_external_ids": with_ids,
        "with_any_external_ids_pct": round(100.0 * with_ids / written, 2) if written else 0.0,
        "rb_exact_match_count": exact,
        "rb_exact_match_pct": round(100.0 * exact / written, 2) if written else 0.0,
        "with_mesh_count": with_mesh_count,
        "with_mesh_pct": round(100.0 * with_mesh_count / written, 2) if written else 0.0,
        "sample_mismatches": sample_mismatches,
        "only_with_mesh": only_with_mesh,
        "log_file": str(log_file)
    }
    with out_stats.open("w", encoding="utf-8") as fs:
        json.dump(stats, fs, indent=2)

    logging.info("Finished. Wrote CSV: %s", out_csv)
    logging.info("Finished. Wrote JSONL: %s", out_jsonl)
    logging.info("Stats: %s", out_stats)
    print(json.dumps({
        "ok": True,
        "input": str(in_path),
        "out_csv": str(out_csv),
        "out_jsonl": str(out_jsonl),
        "out_stats": str(out_stats),
        "log_file": str(log_file)
    }, indent=2))

if __name__ == "__main__":
    main()
