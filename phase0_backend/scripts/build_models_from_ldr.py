import argparse
import json
from pathlib import Path
from datetime import datetime, timezone

from phase0_backend.parsers.ldraw_parser import parse_ldraw_steps, infer_ldraw_stem
from phase0_backend.parsers.ldraw_resolver import load_ldraw_to_rb_map, resolve_rb_part_num
from phase0_backend.parsers.ldraw_colors import load_ldraw_colors
from phase0_backend.parsers.rebrickable_colors import load_rebrickable_colors

LDU_TO_MM = 0.4

def _drop_none(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}

def _enrich_with_rb_and_colors(res, rbmap, ldraw_colors, rbcolors):
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
    color_lookup = {}
    for st in steps:
        for pl in st["placements"]:
            rb = pl.get("rb_part_num") or infer_ldraw_stem(pl.get("subfile",""))
            pl["rb_part_num"] = rb
            lcd = pl.get("ldraw_color")
            key = (rb, lcd)
            bom_counts[key] = bom_counts.get(key, 0) + pl.get("qty", 1)
            color_lookup[key] = lcd

    # Build BOM rows with hex + rb_color_id
    bom = []
    for (rb, lcd), qty in sorted(bom_counts.items(), key=lambda x: (-x[1], x[0])):
        item = {"rb_part_num": rb, "qty": qty}
        if isinstance(lcd, int):
            hexv = (ldraw_colors.get(lcd) or {}).get("hex")
            if hexv:
                item["color_rgb_hex"] = hexv
                rbcolor = rbcolors.get(hexv)
                if rbcolor:
                    item["rb_color_id"] = rbcolor["rb_color_id"]
        bom.append(item)

    res["bom"] = bom
    return res

def build_record(ldr_path: str, parsed, master_parts_version: str | None = None):
    stem = infer_ldraw_stem(ldr_path)

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
        "links": { "master_parts_version": master_parts_version or "", "parser_version": "ldraw_parser_v0.6", "build_tools": "python" }
    }

    return record

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", nargs="+", required=True)
    ap.add_argument("--crosswalk", default="data/processed/crosswalk/unified_crosswalk_mesh.jsonl")
    ap.add_argument("--ldraw-root", default="data/raw/ldraw")
    ap.add_argument("--out", default="data/processed/models/master_models.jsonl")
    args = ap.parse_args()

    rbmap = load_ldraw_to_rb_map(args.crosswalk)
    ldraw_colors = load_ldraw_colors(args.ldraw_root)
    rbcolors = load_rebrickable_colors()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    written = 0
    with out_path.open("w", encoding="utf-8") as w:
        for f in args.files:
            parsed = parse_ldraw_steps(f)
            parsed = _enrich_with_rb_and_colors(parsed, rbmap, ldraw_colors, rbcolors)
            rec = build_record(f, parsed, master_parts_version="")
            w.write(json.dumps(rec, ensure_ascii=False) + "\n")
            written += 1
            print(f"Wrote model record for {f}")

    print(f"Done. Records written: {written} → {out_path}")

if __name__ == "__main__":
    main()
