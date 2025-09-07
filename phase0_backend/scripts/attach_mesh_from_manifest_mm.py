import argparse, csv, json, re, sys, time
from pathlib import Path
from collections import Counter

NON_ALNUM = re.compile(r"[^0-9a-zA-Z]+")

def norm_key(s: str) -> str:
    """Lowercase, strip non-alphanumerics, strip leading zeros; return '0' if empty."""
    if s is None:
        return "0"
    t = NON_ALNUM.sub("", str(s)).lower()
    t = t.lstrip("0")
    return t or "0"

def extract_ldraw_stem(ldraw_path: str | None) -> str | None:
    """Take something like 'data/raw/ldraw/parts/62792.dat' -> '62792'."""
    if not ldraw_path:
        return None
    try:
        return Path(ldraw_path).stem
    except Exception:
        return None

def build_manifest_index(manifest_path: Path):
    """
    Build indices keyed by:
      - 'exact': raw stem as found in manifest (filename stem)
      - 'norm': normalized version of that stem
    NOTE: The manifest column is named 'rb_part_num' but it actually holds the OBJ filename stem.
    """
    by_exact = {}
    by_norm = {}
    count = 0

    with manifest_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stem = (row["rb_part_num"] or "").strip()
            if not stem:
                continue
            rec = {
                "stem": stem,
                "obj_path": row["obj_path"],
                "mtl_path": row.get("mtl_path", "") or "",
                "triangles": int(row["triangles"]),
                "bbox_min": [float(row["bbox_min_x_mm"]), float(row["bbox_min_y_mm"]), float(row["bbox_min_z_mm"])],
                "bbox_max": [float(row["bbox_max_x_mm"]), float(row["bbox_max_y_mm"]), float(row["bbox_max_z_mm"])],
                "hash": row.get("hash_sha1", "") or "",
            }
            count += 1
            if stem not in by_exact:
                by_exact[stem] = rec
            nk = norm_key(stem)
            if nk not in by_norm:
                by_norm[nk] = rec

    return {"exact": by_exact, "norm": by_norm, "count": count}

def ensure_path(obj, path, default_factory=dict):
    cur = obj
    for i, key in enumerate(path):
        if key not in cur or not isinstance(cur[key], dict):
            cur[key] = default_factory() if i == len(path)-1 else {}
        cur = cur[key]
    return cur

def try_match(idx, rb_num: str | None, ldraw_file: str | None, hit_mode_counter: Counter):
    """
    Try multiple keys in order:
      1) rb_exact
      2) rb_norm
      3) ldraw_exact (basename)
      4) ldraw_norm
    Returns: (record or None, mode or None)
    """
    # 1) rb_exact
    if rb_num is not None and rb_num in idx["exact"]:
        hit_mode_counter["rb_exact"] += 1
        return idx["exact"][rb_num], "rb_exact"

    # 2) rb_norm
    if rb_num is not None:
        rk = norm_key(rb_num)
        if rk in idx["norm"]:
            hit_mode_counter["rb_norm"] += 1
            return idx["norm"][rk], "rb_norm"

    # 3) ldraw_exact
    stem = extract_ldraw_stem(ldraw_file)
    if stem and stem in idx["exact"]:
        hit_mode_counter["ldraw_exact"] += 1
        return idx["exact"][stem], "ldraw_exact"

    # 4) ldraw_norm
    if stem:
        nk = norm_key(stem)
        if nk in idx["norm"]:
            hit_mode_counter["ldraw_norm"] += 1
            return idx["norm"][nk], "ldraw_norm"

    return None, None

def main():
    ap = argparse.ArgumentParser(description="Attach mm-scaled OBJ meshes to parts JSONL via RB + LDraw keys.")
    ap.add_argument("--in-jsonl",  type=str, default="data/processed/rebrickable/parts_with_ldraw.jsonl")
    ap.add_argument("--manifest",  type=str, default="data/mesh/obj_mm_manifest.csv")
    ap.add_argument("--out-jsonl", type=str, default="data/processed/rebrickable/parts_with_mesh_mm.jsonl")
    ap.add_argument("--limit", type=int, default=None, help="Process only first N records (debug)")
    ap.add_argument("--log-every", type=int, default=5000, help="Log progress every N records")
    ap.add_argument("--dump-misses", type=int, default=25, help="Print N example misses at the end")
    args = ap.parse_args()

    in_path  = Path(args.in_jsonl)
    man_path = Path(args.manifest)
    out_path = Path(args.out_jsonl)

    print(f"[INFO] Input JSONL: {in_path}  exists={in_path.exists()}", flush=True)
    print(f"[INFO] Manifest CSV: {man_path}  exists={man_path.exists()}", flush=True)
    print(f"[INFO] Output JSONL: {out_path}", flush=True)

    if not in_path.exists():
        print(f"[ERROR] Input JSONL not found: {in_path}", file=sys.stderr, flush=True); sys.exit(2)
    if not man_path.exists():
        print(f"[ERROR] Manifest CSV not found: {man_path}", file=sys.stderr, flush=True); sys.exit(2)

    t0 = time.time()
    print("[INFO] Loading manifest and building indices...", flush=True)
    idx = build_manifest_index(man_path)
    print(f"[INFO] Manifest entries: {idx['count']}", flush=True)

    attached = 0
    passed = 0
    had_legacy_mesh = 0
    total = 0
    hit_modes = Counter()
    misses = []

    out_path.parent.mkdir(parents=True, exist_ok=True)

    with in_path.open("r", encoding="utf-8") as fin, out_path.open("w", encoding="utf-8") as fout:
        for line in fin:
            if not line:
                continue
            rec = json.loads(line)
            total += 1

            rb_num = (rec.get("source_ids", {}).get("rb", {}) or {}).get("part_num")
            ldraw_file = (rec.get("geometry", {}).get("ldraw", {}) or {}).get("file")

            got, mode = try_match(idx, rb_num, ldraw_file, hit_modes)

            if got:
                mesh_asset = {
                    "format": "obj",
                    "path": got["obj_path"],
                    "units": "mm",
                    "triangles": got["triangles"],
                    "bbox_mm": {"min": got["bbox_min"], "max": got["bbox_max"]},
                    "hash": got["hash"],
                    "lod": 0
                }
                geom = rec.setdefault("geometry", {})
                if isinstance(geom.get("mesh"), dict):
                    had_legacy_mesh += 1
                # Legacy single
                geom["mesh"] = {
                    "format": mesh_asset["format"],
                    "path": mesh_asset["path"],
                    "units": mesh_asset["units"],
                    "triangles": mesh_asset["triangles"],
                    "bbox_mm": mesh_asset["bbox_mm"],
                    "hash": mesh_asset["hash"]
                }
                # Preferred multi-asset
                geom["assets"] = [mesh_asset]
                geom["default_lod"] = 0

                # Metrics (extents)
                metrics = ensure_path(rec, ["geometry", "metrics"])
                mn, mx = mesh_asset["bbox_mm"]["min"], mesh_asset["bbox_mm"]["max"]
                metrics["extents_mm"] = {
                    "x": float(mx[0] - mn[0]),
                    "y": float(mx[1] - mn[1]),
                    "z": float(mx[2] - mn[2]),
                }
                attached += 1
            else:
                passed += 1
                if len(misses) < max(0, int(args.dump_misses)):
                    misses.append({"rb": rb_num, "ldraw": extract_ldraw_stem(ldraw_file)})

            fout.write(json.dumps(rec, separators=(",", ":")) + "\n")

            if args.log_every and (total % args.log_every == 0):
                dt = time.time() - t0
                print(f"[PROGRESS] processed={total} attached={attached} passed={passed} hits={dict(hit_modes)} elapsed={dt:.1f}s", flush=True)

            if args.limit and total >= args.limit:
                print(f"[INFO] Hit limit={args.limit}; stopping early.", flush=True)
                break

    dt = time.time() - t0
    print(f"[OK] Wrote: {out_path}", flush=True)
    print(f"[STATS] Total processed: {total}", flush=True)
    print(f"[STATS] Attached meshes: {attached}", flush=True)
    print(f"[STATS] Passed-through: {passed}", flush=True)
    print(f"[STATS] Hit modes: {dict(hit_modes)}", flush=True)
    if misses:
        print(f"[MISS-SAMPLES] First {len(misses)} misses: {misses}", flush=True)
    print(f"[TIME] {dt:.2f}s", flush=True)

if __name__ == "__main__":
    main()
