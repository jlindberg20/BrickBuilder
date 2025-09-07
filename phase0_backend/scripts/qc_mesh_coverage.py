import argparse, csv, json
from pathlib import Path
from collections import Counter, defaultdict

def load_manifest_stems(path: Path) -> set:
    stems = set()
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stem = (row["rb_part_num"] or "").strip()
            if stem:
                stems.add(stem.lower())
    return stems

def ldraw_stem(rec) -> str | None:
    try:
        f = rec.get("geometry", {}).get("ldraw", {}).get("file")
        if not f:
            return None
        return Path(f).stem
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser(description="QC coverage: which parts have meshes, which don't, and why.")
    ap.add_argument("--in-jsonl", type=str, default="data/processed/rebrickable/parts_with_mesh_mm.jsonl")
    ap.add_argument("--manifest", type=str, default="data/mesh/obj_mm_manifest.csv")
    ap.add_argument("--out-summary", type=str, default="data/processed/rebrickable/mesh_coverage_summary.json")
    ap.add_argument("--out-missing-csv", type=str, default="data/processed/rebrickable/mesh_missing_reasonB.csv")
    ap.add_argument("--topk", type=int, default=200)
    args = ap.parse_args()

    in_path = Path(args.in_jsonl)
    man_path = Path(args.manifest)
    out_sum = Path(args.out_summary)
    out_csv = Path(args.out_missing_csv)

    if not in_path.exists():
        raise SystemExit(f"Input not found: {in_path}")
    if not man_path.exists():
        raise SystemExit(f"Manifest not found: {man_path}")

    stems = load_manifest_stems(man_path)

    total = 0
    attached = 0
    missing_reasonA = 0  # no ldraw file
    missing_reasonB = 0  # ldraw file present but no matching OBJ stem
    by_category_total = Counter()
    by_category_missing = Counter()
    reasonB_rows = []

    with in_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)
            total += 1

            cat = (rec.get("category", {}) or {}).get("name") or "Unknown"
            by_category_total[cat] += 1

            geom = rec.get("geometry", {})
            mesh = geom.get("mesh")
            if isinstance(mesh, dict) and mesh.get("path"):
                attached += 1
                continue

            # missing
            st = ldraw_stem(rec)
            if not st:
                missing_reasonA += 1
                by_category_missing[cat] += 1
            else:
                if st.lower() in stems:
                    # unexpected: stem exists in manifest yet mesh is missing
                    # (shouldn't happen in our current attach flow, but keep record)
                    by_category_missing[cat] += 1
                else:
                    missing_reasonB += 1
                    by_category_missing[cat] += 1
                    if len(reasonB_rows) < args.topk:
                        rb = (rec.get("source_ids", {}).get("rb", {}) or {}).get("part_num")
                        reasonB_rows.append({
                            "rb_part_num": rb,
                            "ldraw_stem": st,
                            "category": cat,
                            "name": rec.get("name", "")
                        })

    coverage = attached / total if total else 0.0

    # Write CSV of top Reason-B misses
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as fcsv:
        writer = csv.DictWriter(fcsv, fieldnames=["rb_part_num","ldraw_stem","category","name"])
        writer.writeheader()
        writer.writerows(reasonB_rows)

    # Build per-category coverage
    cats = sorted(by_category_total.items(), key=lambda x: -x[1])
    per_cat = []
    for cat, tot in cats:
        miss = by_category_missing.get(cat, 0)
        att = tot - miss
        per_cat.append({
            "category": cat,
            "total": tot,
            "attached": att,
            "missing": miss,
            "coverage_pct": round((att / tot) * 100, 2) if tot else 0.0
        })

    summary = {
        "total": total,
        "attached": attached,
        "coverage_pct": round(coverage * 100, 2),
        "missing_reasonA_no_ldraw_file": missing_reasonA,
        "missing_reasonB_ldraw_but_no_obj": missing_reasonB,
        "per_category": per_cat
    }

    out_sum.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print("[OK] QC summary written:", out_sum)
    print("[OK] Top Reason-B misses written:", out_csv)
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    main()
