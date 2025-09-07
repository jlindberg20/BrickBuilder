import argparse
import csv
import hashlib
import os
import re
import sys
from pathlib import Path

def sha1_of_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def parse_obj_metrics(obj_path: Path):
    """
    Returns:
      triangles: int
      bbox_min: (x_min, y_min, z_min)
      bbox_max: (x_max, y_max, z_max)
    Notes:
      - Faces (f) may have N vertices (N>=3). We count triangles as (N-2) per face.
      - OBJ indices can be like 'f 1/1/1 2/2/2 3/3/3' — we only need the count.
      - We do not load groups/materials; this is a fast pass.
    """
    vmin = [float("inf"), float("inf"), float("inf")]
    vmax = [float("-inf"), float("-inf"), float("-inf")]
    triangles = 0

    with obj_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if not line:
                continue
            c0 = line[0]
            # Fast path by first char
            if c0 == "v" and line.startswith("v "):
                # vertex line: v x y z
                parts = line.strip().split()
                if len(parts) >= 4:
                    try:
                        x = float(parts[1]); y = float(parts[2]); z = float(parts[3])
                        if x < vmin[0]: vmin[0] = x
                        if y < vmin[1]: vmin[1] = y
                        if z < vmin[2]: vmin[2] = z
                        if x > vmax[0]: vmax[0] = x
                        if y > vmax[1]: vmax[1] = y
                        if z > vmax[2]: vmax[2] = z
                    except ValueError:
                        # Ignore malformed vertex lines
                        pass
            elif c0 == "f" and line.startswith("f "):
                # face line: f v1 v2 v3 [v4 ...]
                # triangle count contribution = (num_vertices - 2)
                # Count tokens that look like vertex refs (split by space, ignore leading 'f')
                tokens = line.strip().split()[1:]
                n = len(tokens)
                if n >= 3:
                    triangles += (n - 2)

    # If no vertices, set bbox to zeros
    if vmin[0] == float("inf"):
        vmin = [0.0, 0.0, 0.0]
        vmax = [0.0, 0.0, 0.0]

    return triangles, vmin, vmax

def infer_mtl_path(obj_path: Path) -> Path | None:
    candidate = obj_path.with_suffix(".mtl")
    return candidate if candidate.exists() else None

def to_rel_forward_slash(path: Path, repo_root: Path) -> str:
    return str(path.relative_to(repo_root)).replace("\\", "/")

def main():
    ap = argparse.ArgumentParser(description="Build manifest CSV for mm-scaled OBJ meshes.")
    ap.add_argument("--mesh-dir", type=str, default="data/mesh/obj_mm", help="Directory containing *.obj")
    ap.add_argument("--out", type=str, default="data/mesh/obj_mm_manifest.csv", help="Output CSV path")
    ap.add_argument("--repo-root", type=str, default=".", help="Repository root for relative paths")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    mesh_dir = (repo_root / args.mesh_dir).resolve()
    out_csv = (repo_root / args.out).resolve()

    if not mesh_dir.exists():
        print(f"[ERROR] Mesh directory not found: {mesh_dir}", file=sys.stderr)
        sys.exit(2)

    obj_files = sorted(mesh_dir.glob("*.obj"))
    if not obj_files:
        print(f"[WARN] No .obj files found in {mesh_dir}")
    
    # Prepare output dir
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    # Open CSV and write header
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "rb_part_num",
            "obj_path",
            "mtl_path",
            "triangles",
            "bbox_min_x_mm", "bbox_min_y_mm", "bbox_min_z_mm",
            "bbox_max_x_mm", "bbox_max_y_mm", "bbox_max_z_mm",
            "hash_sha1"
        ])

        for i, obj_path in enumerate(obj_files, 1):
            stem = obj_path.stem  # e.g., "3001", "3001a", "3622", "3062b"
            # Use the stem as the Rebrickable part number directly.
            # (If we later need normalization rules, we can inject a mapping layer.)
            rb_part_num = stem

            try:
                tri, vmin, vmax = parse_obj_metrics(obj_path)
                mtl = infer_mtl_path(obj_path)
                h = sha1_of_file(obj_path)

                writer.writerow([
                    rb_part_num,
                    to_rel_forward_slash(obj_path, repo_root),
                    to_rel_forward_slash(mtl, repo_root) if mtl else "",
                    int(tri),
                    f"{vmin[0]:.6f}", f"{vmin[1]:.6f}", f"{vmin[2]:.6f}",
                    f"{vmax[0]:.6f}", f"{vmax[1]:.6f}", f"{vmax[2]:.6f}",
                    h
                ])
            except Exception as e:
                print(f"[WARN] Failed {obj_path.name}: {e}", file=sys.stderr)

            if i % 1000 == 0:
                print(f"[INFO] Processed {i} files...")

    print(f"[OK] Wrote manifest: {out_csv}")

if __name__ == "__main__":
    main()
