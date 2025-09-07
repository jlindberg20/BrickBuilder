import argparse, json, csv
from pathlib import Path

def safe_get(d, *ks, default=None):
    cur = d
    for k in ks:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def to_text(rec):
    name = rec.get("name") or ""
    cat = safe_get(rec, "category", "name", default="")
    aliases = rec.get("aliases") or []
    s_alias = ""
    if aliases:
        s_alias = " Aliases: " + ", ".join(a for a in aliases[:8])
    return f"{name}. Category: {cat}.{s_alias}"

def main():
    ap = argparse.ArgumentParser(description="Flatten parts JSONL into embedding docs + numeric features.")
    ap.add_argument("--in-jsonl", type=str, default="data/processed/rebrickable/parts_with_mesh_mm.jsonl")
    ap.add_argument("--out-docs", type=str, default="data/index/parts_docs.jsonl")
    ap.add_argument("--out-features", type=str, default="data/index/parts_features.csv")
    ap.add_argument("--include-type", type=str, default="part", help="Only include this type ('' = include all)")
    ap.add_argument("--require-mesh", action="store_true", help="Emit only records with geometry.mesh.path")
    ap.add_argument("--minify", action="store_true", help="Minify JSON output")
    args = ap.parse_args()

    in_path = Path(args.in_jsonl)
    out_docs = Path(args.out_docs)
    out_feats = Path(args.out_features)

    if not in_path.exists():
        raise SystemExit(f"Input not found: {in_path}")

    out_docs.parent.mkdir(parents=True, exist_ok=True)
    out_feats.parent.mkdir(parents=True, exist_ok=True)

    docs_written = 0
    feats_written = 0
    kept = 0
    scanned = 0

    with in_path.open("r", encoding="utf-8") as fin, \
         out_docs.open("w", encoding="utf-8") as fdocs, \
         out_feats.open("w", newline="", encoding="utf-8") as fcsv:

        w = csv.writer(fcsv)
        w.writerow(["id","rb_part_num","category_id","category_name",
                    "extent_x_mm","extent_y_mm","extent_z_mm","triangles","mesh_path"])

        for line in fin:
            if not line.strip():
                continue
            scanned += 1
            rec = json.loads(line)

            # FIXED: use args.include_type (underscore), not include-type
            if args.include_type and rec.get("type") != args.include_type:
                continue

            mesh = safe_get(rec, "geometry", "mesh", default={}) or {}
            mesh_path = mesh.get("path") or ""
            if args.require_mesh and not mesh_path:
                continue

            cid = rec.get("id")
            rb_num = safe_get(rec, "source_ids", "rb", "part_num", default="")
            category_id = safe_get(rec, "category", "id", default="")
            category_name = safe_get(rec, "category", "name", default="")
            triangles = mesh.get("triangles") or 0
            ext = safe_get(rec, "geometry", "metrics", "extents_mm", default={}) or {}
            ex = float(ext.get("x", 0.0) or 0.0)
            ey = float(ext.get("y", 0.0) or 0.0)
            ez = float(ext.get("z", 0.0) or 0.0)

            # 1) Embedding doc
            doc = {
                "id": cid,
                "text": to_text(rec),
                "metadata": {
                    "rb_part_num": rb_num,
                    "category_id": category_id,
                    "category_name": category_name,
                    "triangles": int(triangles) if isinstance(triangles, int) else 0,
                    "extents_mm": {"x": ex, "y": ey, "z": ez},
                    "mesh_path": mesh_path
                }
            }
            fdocs.write((json.dumps(doc, separators=(",", ":")) if args.minify else json.dumps(doc)) + "\n")
            docs_written += 1

            # 2) Numeric features
            w.writerow([cid, rb_num, category_id, category_name,
                        f"{ex:.3f}", f"{ey:.3f}", f"{ez:.3f}",
                        int(triangles) if isinstance(triangles, int) else 0,
                        mesh_path])
            feats_written += 1
            kept += 1

    print(f"[OK] Scanned {scanned} records; kept {kept} (type={args.include_type or 'ALL'}; require_mesh={args.require_mesh})")
    print(f"[OK] Wrote docs: {out_docs} ({docs_written} rows)")
    print(f"[OK] Wrote features: {out_feats} ({feats_written} rows)")

if __name__ == "__main__":
    main()
