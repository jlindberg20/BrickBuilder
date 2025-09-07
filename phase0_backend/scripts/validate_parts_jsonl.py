import argparse, json, sys, time, csv
from pathlib import Path

def try_import_jsonschema():
    try:
        import jsonschema
        return jsonschema
    except Exception:
        return None

# Minimal checks for parts
ESSENTIAL_PATHS_PART = [
    ["id"],
    ["type"],
    ["name"],
    ["source_ids","rb","part_num"],
    ["category","id"],
    ["category","name"],
    ["geometry"],  # container must exist
    ["metadata","created_at"],
    ["metadata","updated_at"],
]

def has_path(obj, path):
    cur = obj
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return False
        cur = cur[k]
    return True

def lightweight_checks_part(rec, require_mesh: bool):
    errs = []
    for p in ESSENTIAL_PATHS_PART:
        if not has_path(rec, p):
            errs.append(f"missing: {'/'.join(p)}")
    if require_mesh:
        mesh = (rec.get("geometry") or {}).get("mesh")
        if not isinstance(mesh, dict) or not mesh.get("path"):
            errs.append("missing: geometry/mesh/path (require_mesh)")
    return errs

def main():
    ap = argparse.ArgumentParser(description="Validate JSONL against schema or fallback checks, focusing on type=='part'.")
    ap.add_argument("--schema", type=str, default="BrickBuilder/phase0_backend/part_record.schema.json")
    ap.add_argument("--in-jsonl", type=str, default="data/processed/rebrickable/parts_with_mesh_mm.jsonl")
    ap.add_argument("--out-dir", type=str, default="data/processed/rebrickable/validation")
    ap.add_argument("--sample-pass", type=int, default=50)
    ap.add_argument("--only-type", type=str, default="part", help="Validate only records with given 'type' (use '' to validate all)")
    ap.add_argument("--require-mesh", action="store_true", help="Require geometry.mesh.path for records under validation")
    args = ap.parse_args()

    schema_path = Path(args.schema)
    in_path = Path(args.in_jsonl)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        print(f"[ERROR] Input JSONL not found: {in_path}", file=sys.stderr); sys.exit(2)
    if not schema_path.exists():
        print(f"[WARN] Schema not found at {schema_path}. Will run lightweight checks only.")

    js = try_import_jsonschema()
    schema = None
    validator = None
    if js and schema_path.exists():
        with schema_path.open("r", encoding="utf-8") as sf:
            schema = json.load(sf)
        validator = js.Draft7Validator(schema)

    summary = {
        "input_file": str(in_path),
        "schema_file": str(schema_path) if schema_path.exists() else None,
        "using_jsonschema": bool(validator),
        "filtered_on_type": args.only_type or None,
        "require_mesh": bool(args.require_mesh),
        "total_in_file": 0,
        "total_checked": 0,
        "valid": 0,
        "invalid": 0,
        "start_time": None,
        "end_time": None,
        "elapsed_seconds": None,
    }

    failures_csv = out_dir / (in_path.stem + ".failures.csv")
    sample_pass_jsonl = out_dir / (in_path.stem + ".sample_pass.jsonl")
    summary_json = out_dir / (in_path.stem + ".validation_summary.json")

    t0 = time.time()
    summary["start_time"] = int(t0)

    with failures_csv.open("w", newline="", encoding="utf-8") as fcsv, \
         sample_pass_jsonl.open("w", encoding="utf-8") as fpass, \
         in_path.open("r", encoding="utf-8") as fin:
        writer = csv.writer(fcsv)
        writer.writerow(["line_index","id","problem_summary","record_type"])

        pass_kept = 0

        for i, line in enumerate(fin, 1):
            line = line.strip()
            if not line:
                continue
            summary["total_in_file"] += 1
            rec = json.loads(line)
            rtype = rec.get("type")

            # Filter by type if requested
            if args.only_type and rtype != args.only_type:
                continue

            summary["total_checked"] += 1
            errs = []
            if validator:
                # Run schema validation only for parts, but our schema is for parts; this is fine.
                errs = [e.message for e in validator.iter_errors(rec)]
                # Also run minimal checks
                errs.extend(lightweight_checks_part(rec, args.require_mesh))
            else:
                errs = lightweight_checks_part(rec, args.require_mesh)

            if errs:
                summary["invalid"] += 1
                rid = rec.get("id","<none>")
                writer.writerow([i, rid, " | ".join(sorted(set(errs))), rtype])
            else:
                summary["valid"] += 1
                if pass_kept < args.sample_pass:
                    fpass.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    pass_kept += 1

    t1 = time.time()
    summary["end_time"] = int(t1)
    summary["elapsed_seconds"] = round(t1 - t0, 3)

    with summary_json.open("w", encoding="utf-8") as fsum:
        json.dump(summary, fsum, indent=2)

    print("[OK] Validation summary:", summary_json)
    print("[OK] Failure rows:", failures_csv)
    print("[OK] Sample valid records:", sample_pass_jsonl)
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    main()
