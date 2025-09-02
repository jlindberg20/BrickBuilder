from __future__ import annotations
from pathlib import Path
import json
import csv
from typing import Dict, Iterable

RB_IN  = Path("data/processed/rebrickable/parts_with_ldraw.jsonl")
MAN    = Path("data/mesh/obj_manifest_small.csv")
RB_OUT = Path("data/processed/rebrickable/parts_with_mesh_small.jsonl")

def iter_jsonl(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def jsonl_writer(path: Path):
    class _W:
        def __init__(self, p: Path):
            p.parent.mkdir(parents=True, exist_ok=True)
            self.f = open(p, "w", encoding="utf-8")
        def write(self, obj: dict):
            self.f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        def close(self):
            self.f.close()
    return _W(path)

def load_manifest(mpath: Path) -> Dict[str, dict]:
    """
    Returns a mapping from RB part number -> row, using 'rb_part_num' from the manifest.
    """
    m: Dict[str, dict] = {}
    with open(mpath, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            key = (row.get("rb_part_num") or "").strip().lower()
            if key:
                m[key] = row
    return m

def main():
    if not RB_IN.exists():
        raise SystemExit(f"Missing input: {RB_IN}")
    if not MAN.exists():
        raise SystemExit(f"Missing manifest: {MAN}")

    man = load_manifest(MAN)
    writer = jsonl_writer(RB_OUT)

    total = 0
    attached = 0

    for obj in iter_jsonl(RB_IN):
        total += 1
        rb_num = (obj.get("source_ids", {}).get("rb", {}).get("part_num") or "").lower()

        # Ensure geometry container is a dict
        geom = obj.get("geometry")
        if not isinstance(geom, dict):
            geom = {}
            obj["geometry"] = geom

        # Ensure mesh container is a dict
        mesh = geom.get("mesh")
        if not isinstance(mesh, dict):
            mesh = {}
            geom["mesh"] = mesh

        if rb_num in man:
            row = man[rb_num]
            mesh.update({
                "format": "obj",
                "path": row["obj_path"],
                "triangles": int(row["triangles"]),
                "bbox_mm": {
                    "min": [float(x) for x in row["bbox_min_mm"].split(",")],
                    "max": [float(x) for x in row["bbox_max_mm"].split(",")],
                },
                "units": "mm"
            })
            attached += 1

        writer.write(obj)

    writer.close()
    print(f"Scanned {total} records.")
    print(f"Attached mesh to {attached} records (from manifest).")
    print(f"Output: {RB_OUT.as_posix()}")

if __name__ == "__main__":
    main()



