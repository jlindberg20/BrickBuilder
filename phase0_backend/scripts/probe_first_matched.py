from __future__ import annotations
from pathlib import Path
import json

from .ldraw_expand import LDrawIndex, LDrawExpander, triangle_bounds

RB_IN = Path("data/processed/rebrickable/parts_with_ldraw.jsonl")
LDRAW_INDEX_JSON = Path("data/raw/ldraw/_index/ldraw_index.json")

def iter_jsonl(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def main():
    # Find the first matched record
    first = None
    for obj in iter_jsonl(RB_IN):
        ld = obj.get("geometry", {}).get("ldraw", {})
        if ld and ld.get("status") == "matched" and ld.get("file"):
            first = (obj, ld["file"])
            break
    if not first:
        raise SystemExit("No matched parts found in parts_with_ldraw.jsonl")

    obj, dat_path = first
    print(f"Probing part: {obj.get('id')}  RB:{obj.get('source_ids',{}).get('rb',{}).get('part_num')}")
    print(f"LDraw file:  {dat_path}")

    index = LDrawIndex(LDRAW_INDEX_JSON)
    expander = LDrawExpander(Path("data/raw/ldraw"), index)
    tris = expander.expand_to_triangles(dat_path)

    print(f"Triangles:   {len(tris)}")
    mn, mx = triangle_bounds(tris)
    print(f"BBox (LDU):  min={mn}  max={mx}")
    # also print in mm for sanity
    print(f"BBox (mm):   min={mn*0.4}  max={mx*0.4}")

if __name__ == "__main__":
    main()



