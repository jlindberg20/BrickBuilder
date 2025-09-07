import argparse
import csv
import hashlib
import os
import random
import sys
from pathlib import Path

# -----------------------------
# Utility functions
# -----------------------------

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
    Fast metrics pass over an OBJ:
      - triangles: sum over faces (f): (N_vertices - 2)
      - bbox: min/max over all vertex (v) lines
    Returns: (triangles:int, vmin:[x,y,z], vmax:[x,y,z])
    """
    vmin = [float("inf"), float("inf"), float("inf")]
    vmax = [float("-inf"), float("-inf"), float("-inf")]
    triangles = 0

    with obj_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if not line:
                continue
            if line.startswith("v "):
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
            elif line.startswith("f "):
                # faces can have N>=3 vertices; count as (N-2) triangles
                tokens = line.strip().split()[1:]
                n = len(tokens)
                if n >= 3:
                    triangles += (n - 2)

    if vmin[0] == float("inf"):
        # No vertices encountered
        vmin = [0.0, 0.0, 0.0]
        vmax = [0.0, 0.0, 0.0]

    return triangles, vmin, vmax

def infer_mtl_path(obj_path: Path) -> Path | None:
    candidate = obj_path.with_suffix(".mtl")
    return candidate if candidate.exists() else None

def to_rel_forward_slash(path: Path, repo_root: Path) -> str:
    return str(path.relative_to(repo_root)).replace("\\", "/")

# -----------------------------
# Main
# -----------------------------

def main():
    ap = argparse.ArgumentParser(description="Build manifest CSV for mm-scaled OBJ meshes.")
    ap.add_argument("--mesh-dir", type=str, default="data/mesh/obj_mm", help="Directory containing *.obj")
    ap.add_argument("--out", type=str, default="data/mesh/obj_mm_manifest.csv", help="Output CSV path")
    ap.add_argument("--repo-root", type=str, default=".", help="Repository root for relative paths")
    # SAMPLE MODE CONTROLS (remove or ignore once validated)
    ap.add_argument("--limit", type=int, default=None, help="Process only the first N files (deterministic order)")
    ap.add_argument("--sample-random", type=int, default=None, help="Process N randomly sampled files (ignores --limit)")
    ap.add_argument("--log-every", type=int, default=1000, help="Progress log frequency")
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
    else:
        print(f"[INFO] Found {len(obj_files)} OBJ files in {mesh_dir}")

    # SAMPLE MODE: random sample takes precedence over deterministic limit
    if args.sample_random is not None:
        if args.sample_random <= 0:
            print("[ERROR] --sample-random must be > 0", file=sys.stderr)
            sys.exit(2)
        if args.sample_random > len(obj_files):
            print(f"[WARN] sample size {args.sample_random} > file count {len(obj_files)}; using all files.")
            selected = obj_files
        else:
            selected = random.sample(obj_files, args.sample_random)
        print(f"[INFO] Random sample enabled: processing {len(selected)} files")
    elif args.limit is not None:
        if args.limit <= 0:
            print("[ERROR] --limit must be > 0", file=sys.stderr)
            sys.exit(2)
        selected = obj_files[:args.limit]
        print(f"[INFO] Limit enabled: processing first {len(selected)} files")
    else:
        selected = obj_files

    # Prepare output dir
    out_csv.parent.mkdir(parents=True, exist_ok=True)

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

        for i, obj_path in enumerate(selected, 1):
            rb_part_num = obj_path.stem  # e.g., "3001", "3062b", etc.

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

            if args.log_every and i % args.log_every == 0:
                print(f"[INFO] Processed {i} files...")

    print(f"[OK] Wrote manifest: {out_csv}")
    print("[HINT] Row count should equal processed files + 1 (header).")

if __name__ == "__main__":
    main()
