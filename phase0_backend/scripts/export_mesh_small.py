from __future__ import annotations
from pathlib import Path
import json
import csv
from typing import Iterable, Tuple

from .ldraw_expand import LDrawIndex, LDrawExpander, triangle_bounds, LDU_TO_MM

RB_IN = Path("data/processed/rebrickable/parts_with_ldraw.jsonl")
LDRAW_INDEX_JSON = Path("data/raw/ldraw/_index/ldraw_index.json")

OUT_DIR = Path("data/mesh/obj")
OUT_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST = Path("data/mesh/obj_manifest_small.csv")

BATCH_LIMIT = 5   # keep small and safe for the first pass

def iter_jsonl(path: Path) -> Iterable[dict]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def find_first_n_matched(n: int) -> Iterable[Tuple[dict, str]]:
    count = 0
    for obj in iter_jsonl(RB_IN):
        ld = obj.get("geometry", {}).get("ldraw", {})
        if ld and ld.get("status") == "matched" and ld.get("file"):
            yield obj, ld["file"]
            count += 1
            if count >= n:
                break

def write_obj_triangle_soup(tris_ldu, out_path: Path):
    """
    Minimal OBJ writer (triangle soup). We do not weld or dedupe for this first pass.
    Coordinates are converted to mm.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("# BrickBuilder mesh export (triangle soup)\n")
        f.write("# units: millimeters\n")
        # Write vertices
        verts_mm = (tris_ldu.reshape(-1, 3) * LDU_TO_MM)
        for v in verts_mm:
            f.write(f"v {v[0]:.6f} {v[1]:.6f} {v[2]:.6f}\n")
        # Faces: 1-based indices, each tri uses 3 consecutive vertices
        num_tris = len(tris_ldu)
        for i in range(num_tris):
            i0 = 3*i + 1
            f.write(f"f {i0} {i0+1} {i0+2}\n")

def main():
    if not RB_IN.exists():
        raise SystemExit(f"Missing input: {RB_IN}")
    if not LDRAW_INDEX_JSON.exists():
        raise SystemExit(f"Missing index: {LDRAW_INDEX_JSON}")

    index = LDrawIndex(LDRAW_INDEX_JSON)
    expander = LDrawExpander(Path("data/raw/ldraw"), index)

    rows = []
    exported = 0
    for obj, dat_path in find_first_n_matched(BATCH_LIMIT):
        rb_id = obj.get("id", "")
        rb_num = obj.get("source_ids", {}).get("rb", {}).get("part_num", "")
        out_fp = OUT_DIR / f"{rb_num.lower() or rb_id.replace(':','_')}.obj"

        tris = expander.expand_to_triangles(dat_path)
        if tris.size == 0:
            # skip empty geometry
            continue

        # Write OBJ
        write_obj_triangle_soup(tris, out_fp)

        # Stats
        mn_ldu, mx_ldu = triangle_bounds(tris)
        mn_mm = mn_ldu * LDU_TO_MM
        mx_mm = mx_ldu * LDU_TO_MM

        rows.append({
            "id": rb_id,
            "rb_part_num": rb_num,
            "obj_path": out_fp.as_posix(),
            "triangles": len(tris),
            "bbox_min_mm": f"{mn_mm[0]:.3f},{mn_mm[1]:.3f},{mn_mm[2]:.3f}",
            "bbox_max_mm": f"{mx_mm[0]:.3f},{mx_mm[1]:.3f},{mx_mm[2]:.3f}",
        })
        exported += 1

    # Write manifest (append-safe overwrite)
    with open(MANIFEST, "w", newline="", encoding="utf-8") as csvf:
        w = csv.DictWriter(csvf, fieldnames=[
            "id","rb_part_num","obj_path","triangles","bbox_min_mm","bbox_max_mm"
        ])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"Exported {exported} OBJ meshes into {OUT_DIR.as_posix()}")
    print(f"Manifest: {MANIFEST.as_posix()}")

if __name__ == "__main__":
    main()
