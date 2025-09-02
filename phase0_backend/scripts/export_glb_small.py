from __future__ import annotations
from pathlib import Path
import csv
import json
import hashlib
import numpy as np

from .ldraw_expand import LDrawIndex, LDrawExpander, triangle_bounds, LDU_TO_MM

# ---- Inputs/outputs ----
RB_IN  = Path("data/processed/rebrickable/parts_with_ldraw.jsonl")
IDX    = Path("data/raw/ldraw/_index/ldraw_index.json")
OUTDIR = Path("data/mesh/glb")
OUTDIR.mkdir(parents=True, exist_ok=True)
MAN    = Path("data/mesh/glb_manifest_small.csv")

BATCH_LIMIT = 200  # small and safe pilot

# ---- Utilities ----
def iter_jsonl(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def find_first_n_matched(n: int):
    count = 0
    for obj in iter_jsonl(RB_IN):
        ld = obj.get("geometry", {}).get("ldraw", {})
        if ld and ld.get("status") == "matched" and ld.get("file"):
            yield obj, ld["file"]
            count += 1
            if count >= n:
                break

def sha256_of_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()

# ---- Core export using trimesh ----
def triangles_to_trimesh(tris_ldu: np.ndarray):
    import trimesh
    import numpy as np

    # Convert triangle soup to mm
    verts = (tris_ldu.reshape(-1, 3) * LDU_TO_MM)
    if verts.size == 0:
        return trimesh.Trimesh(vertices=np.zeros((0, 3)),
                               faces=np.zeros((0, 3), dtype=np.int64),
                               process=False)

    faces = np.arange(len(verts), dtype=np.int64).reshape(-1, 3)
    mesh = trimesh.Trimesh(vertices=verts, faces=faces, process=False)

    # --- 1) Remove degenerate faces (handle mask vs indices across versions) ---
    try:
        nd = mesh.nondegenerate_faces()              # may return mask or indices
    except TypeError:
        nd = mesh.nondegenerate_faces(None)          # some versions require kw
    if nd is not None:
        if isinstance(nd, np.ndarray) and nd.dtype == bool:
            mesh.update_faces(nd)
        else:
            mesh.update_faces(nd)                    # indices also OK
        mesh.remove_unreferenced_vertices()

    # --- 2) Remove duplicate faces (handle mask vs indices across versions) ---
    try:
        uniq = mesh.unique_faces()
        if uniq is not None:
            if isinstance(uniq, np.ndarray) and uniq.dtype == bool:
                mesh.update_faces(uniq)
            else:
                mesh.update_faces(uniq)
            mesh.remove_unreferenced_vertices()
    except Exception:
        # Fallback: dedupe by sorted face-vertex indices
        f_sorted = np.sort(mesh.faces, axis=1)
        _, unique_idx = np.unique(f_sorted, axis=0, return_index=True)
        mask = np.zeros(len(mesh.faces), dtype=bool)
        mask[unique_idx] = True
        mesh.update_faces(mask)
        mesh.remove_unreferenced_vertices()

    # --- 3) Clean bad values and dangling verts ---
    try:
        mesh.remove_infinite_values()
    except Exception:
        pass
    mesh.remove_unreferenced_vertices()

    # --- 4) Weld vertices (tolerant to old signatures) ---
    try:
        mesh.merge_vertices()   # older versions: no args; newer accept defaults
    except TypeError:
        # As a fallback, round coordinates then drop dup verts
        mesh.vertices = np.round(mesh.vertices, 6)
        try:
            mesh.merge_vertices()
        except Exception:
            pass
    mesh.remove_unreferenced_vertices()

    return mesh

def save_glb(mesh, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # trimesh will use pygltflib under the hood; keep default settings
    mesh.export(out_path.as_posix())

def main():
    # Dependency check
    try:
        import trimesh  # noqa: F401
    except ImportError:
        raise SystemExit(
            "Missing dependency: trimesh\n"
            "Install with: pip install trimesh pygltflib"
        )

    if not RB_IN.exists():
        raise SystemExit(f"Missing input: {RB_IN}")
    if not IDX.exists():
        raise SystemExit(f"Missing index: {IDX}")

    index   = LDrawIndex(IDX)
    expander = LDrawExpander(Path("data/raw/ldraw"), index)

    rows = []
    exported = 0
    attempted = 0

    for obj, dat_path in find_first_n_matched(BATCH_LIMIT):
        attempted += 1
        rb_num = (obj.get("source_ids", {}).get("rb", {}).get("part_num") or "").lower()
        out_fp = OUTDIR / f"{rb_num or obj.get('id','unknown').replace(':','_')}.glb"

        tris = expander.expand_to_triangles(dat_path)
        if tris.size == 0:
            # Skip parts with no triangles
            continue

        mesh = triangles_to_trimesh(tris)
        if mesh.faces.shape[0] == 0 or mesh.vertices.shape[0] == 0:
            # Very degenerate after cleanup
            continue

        save_glb(mesh, out_fp)

        mn_ldu, mx_ldu = triangle_bounds(tris)
        mn_mm = mn_ldu * LDU_TO_MM
        mx_mm = mx_ldu * LDU_TO_MM

        rows.append({
            "id": obj.get("id",""),
            "rb_part_num": rb_num,
            "glb_path": out_fp.as_posix(),
            "triangles": int(mesh.faces.shape[0]),
            "bbox_min_mm": f"{mn_mm[0]:.3f},{mn_mm[1]:.3f},{mn_mm[2]:.3f}",
            "bbox_max_mm": f"{mx_mm[0]:.3f},{mx_mm[1]:.3f},{mx_mm[2]:.3f}",
            "hash": sha256_of_file(out_fp),
        })
        exported += 1

    # Write manifest
    with open(MAN, "w", newline="", encoding="utf-8") as csvf:
        w = csv.DictWriter(csvf, fieldnames=[
            "id","rb_part_num","glb_path","triangles","bbox_min_mm","bbox_max_mm","hash"
        ])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"Attempted {attempted} matched parts; exported {exported} GLBs into {OUTDIR.as_posix()}")
    print(f"Manifest: {MAN.as_posix()}")

if __name__ == "__main__":
    main()



