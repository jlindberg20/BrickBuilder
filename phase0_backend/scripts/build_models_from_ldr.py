import argparse
import json
from pathlib import Path
from datetime import datetime, timezone

from phase0_backend.parsers.ldraw_parser import parse_ldraw_steps, infer_ldraw_stem
from phase0_backend.parsers.ldraw_resolver import load_ldraw_to_rb_map, resolve_rb_part_num
from phase0_backend.parsers.ldraw_colors import load_ldraw_colors

LDU_TO_MM = 0.4

def _drop_none(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}

def _enrich_with_rb_and_colors(res, rbmap, ldraw_colors):
    steps = res["steps"] or []

    # Resolve rb_part_num on placements
    for st in steps:
        for pl in st["placements"]:
            if not pl.get("rb_part_num"):
                rb = resolve_rb_part_num(pl.get("subfile",""), rbmap)
                if rb:
                    pl["rb_part_num"] = rb

    # Aggregate BOM
    bom_counts = {}
    for st in steps:
        for pl in st["placements"]:
            rb = pl.get("rb_part_num") or infer_ldraw_stem(pl.get("subfile",""))
            pl["rb_part_num"] = rb
            lcd = pl.get("ldraw_color")
            key = (rb, lcd)
            bom_counts[key] = bom_counts.get(key, 0) + pl.get("qty", 1)

    # Build BOM rows with hex color
    bom = []
    for (rb, lcd), qty in sorted(bom_counts.items(), key=lambda x: (-x[1], x[0])):
        item = {"rb_part_num": rb, "qty": qty}
        if isinstance(lcd, int):
            hexv = (ldraw_colors.get(lcd) or {}).get("hex")
            if hexv:
                item["color_rgb_hex"] = hexv
        bom.append(item)

    res["bom"] = bom
    return res

def _compute_bbox_mm_or_none(ldr_root_file: str):
    """
    Compute full-assembly bbox in mm using ldraw_expand.LDrawExpander triangles.
    Assumes triangles are returned in LDU as iterable of 3 points (x,y,z).
    """
    try:
        from phase0_backend.scripts.ldraw_expand import LDrawIndex, LDrawExpander
    except Exception as e:
        print(f"[bbox] Skipping (ldraw_expand import failed: {e})")
        return None

    try:
        # locate LDraw root by walking up to a folder containing 'parts'
        p = Path(ldr_root_file).resolve()
        ldraw_root = None
        for parent in p.parents:
            if (parent / "parts").exists():
                ldraw_root = parent
                break
        if ldraw_root is None:
            print("[bbox] Could not locate LDraw root (no 'parts' folder found upward).")
            return None

        index = LDrawIndex(str(ldraw_root))
        expander = LDrawExpander(index)

        # Expect triangles in LDU; adjust if your expander uses a different method name/signature
        tris = expander.expand_to_triangles(ldr_root_file)
        if not tris:
            print("[bbox] No triangles produced; skipping bbox.")
            return None

        # Compute bbox in LDU
        minx=miny=minz= float("inf")
        maxx=maxy=maxz= float("-inf")
        for tri in tris:
            # tri is expected to be an iterable of 3 points
            for (x,y,z) in tri:
                if x<minx: minx=x
                if y<miny: miny=y
                if z<minz: minz=z
                if x>maxx: maxx=x
                if y>maxy: maxy=y
                if z>maxz: maxz=z

        min_mm = [round(minx*LDU_TO_MM,3), round(miny*LDU_TO_MM,3), round(minz*LDU_TO_MM,3)]
        max_mm = [round(maxx*LDU_TO_MM,3), round(maxy*LDU_TO_MM,3), round(maxz*LDU_TO_MM,3)]
        return {"min": min_mm, "max": max_mm}
    except Exception as e:
        print(f"[bbox] Skipping (expand error: {e})")
        return None

def build_record(ldr_path: str, parsed, master_parts_version: str | None = None, final_bbox_mm=None):
    stem = infer_ldraw_stem(ldr_path)

    # Placements → schema-compliant
    steps = parsed["steps"]
    if steps is not None:
        out_steps = []
        for st in steps:
            out_pls = []
            for pl in st["placements"]:
                base = {
                    "rb_part_num": pl.get("rb_part_num"),
                    "rb_color_id": pl.get("rb_color_id"),
                    "qty": pl.get("qty", 1),
                    "transform": pl.get("transform")
                }
                out_pls.append(_drop_none(base))
            out_steps.append({ "step_num": st["step_num"], "placements": out_pls })
    else:
        out_steps = None

    piece_count = sum([row["qty"] for row in parsed["bom"]]) if parsed["bom"] else 0

    record = {
        "id": f"model:ldraw:{stem}",
        "type": "model",
        "source_ids": { "ldraw": {"root_file": ldr_path, "file_stem": stem} },
        "name": stem,
        "metadata": {
            "piece_count": piece_count,
            "last_updated": datetime.now(timezone.utc).isoformat()
        },
        "geometry": {
            "ldraw": { "root_file": ldr_path, "scale": {"units": "LDU", "to_mm": LDU_TO_MM} }
        },
        "bom": parsed["bom"],
        "steps": out_steps,
        "instructions": { "kind": "ldraw", "source": ldr_path, "parsed_confidence": 1.0 if out_steps else 0.5 },
        "links": { "master_parts_version": master_parts_version or "", "parser_version": "ldraw_parser_v0.5", "build_tools": "python" }
    }

    if final_bbox_mm:
        record["geometry"]["final_bbox_mm"] = final_bbox_mm

    return record

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", nargs="+", required=True, help="List of .ldr/.mpd model files (repo-relative).")
    ap.add_argument("--crosswalk", default="data/processed/crosswalk/unified_crosswalk_mesh.jsonl")
    ap.add_argument("--ldraw-root", default="data/raw/ldraw")
    ap.add_argument("--bbox", action="store_true", help="Compute full-assembly bbox in mm (best-effort).")
    ap.add_argument("--out", default="data/processed/models/master_models.jsonl")
    args = ap.parse_args()

    rbmap = load_ldraw_to_rb_map(args.crosswalk)
    ldraw_colors = load_ldraw_colors(args.ldraw_root)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    written = 0
    with out_path.open("w", encoding="utf-8") as w:
        for f in args.files:
            parsed = parse_ldraw_steps(f)
            parsed = _enrich_with_rb_and_colors(parsed, rbmap, ldraw_colors)

            bbox_mm = None
            if args.bbox:
                bbox_mm = _compute_bbox_mm_or_none(f)

            rec = build_record(f, parsed, master_parts_version="", final_bbox_mm=bbox_mm)
            w.write(json.dumps(rec, ensure_ascii=False) + "\n")
            written += 1
            print(f"Wrote model record for {f}")

    print(f"Done. Records written: {written} → {out_path}")

if __name__ == "__main__":
    main()
