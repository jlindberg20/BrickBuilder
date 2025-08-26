# scripts/reb_build_parts.py
from __future__ import annotations
import os, json, hashlib, time
from pathlib import Path
from typing import Dict, Any, Iterable, List, Set
import pandas as pd

ENV = {
    "DATA_RAW": os.getenv("DATA_RAW", "./data/raw"),
    "DATA_RAW_REB": os.getenv("DATA_RAW_REB", "./data/raw/rebrickable"),
    "DATA_PROCESSED": os.getenv("DATA_PROCESSED", "./data/processed"),
    "CHUNK_ROWS": int(os.getenv("CHUNK_ROWS", "500000")),
}

REB = Path(ENV["DATA_RAW_REB"])
PROCESSED = Path(ENV["DATA_PROCESSED"]) / "rebrickable"
OUT_JSONL = PROCESSED / "parts_unified.jsonl"
OUT_CSV = PROCESSED / "parts_preview.csv"
MANIFEST = PROCESSED / "_manifest.json"

SRC_PARTS = REB / "parts.csv"
SRC_CATS = REB / "part_categories.csv"
SRC_ELEMS = REB / "elements.csv"

REQUIRED = [SRC_PARTS, SRC_CATS, SRC_ELEMS]

def file_fingerprint(p: Path) -> Dict[str, Any]:
    stat = p.stat()
    return {"size": stat.st_size, "mtime": int(stat.st_mtime)}

def load_manifest() -> Dict[str, Any]:
    if MANIFEST.exists():
        with open(MANIFEST, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_manifest(d: Dict[str, Any]):
    PROCESSED.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST, "w", encoding="utf-8") as f:
        json.dump(d, f, indent=2, sort_keys=True)

def inputs_signature() -> Dict[str, Any]:
    return {str(p): file_fingerprint(p) for p in REQUIRED}

def manifest_matches(current: Dict[str, Any], manifest: Dict[str, Any]) -> bool:
    prev = manifest.get("inputs", {})
    return prev == current

def compute_color_variants() -> Dict[str, List[int]]:
    """
    Stream elements.csv to build a set of colors per part_num.
    elements.csv columns (Rebrickable): element_id, part_num, color_id, design_id (varies by version)
    """
    color_map: Dict[str, Set[int]] = {}
    usecols = ["part_num", "color_id"]
    dtype = {"part_num": "string", "color_id": "Int64"}

    for chunk in pd.read_csv(SRC_ELEMS, usecols=usecols, dtype=dtype, chunksize=ENV["CHUNK_ROWS"]):
        chunk = chunk.dropna(subset=["part_num", "color_id"])
        for part_num, grp in chunk.groupby("part_num"):
            colors = grp["color_id"].dropna().astype("int").unique().tolist()
            if part_num not in color_map:
                color_map[part_num] = set(colors)
            else:
                color_map[part_num].update(colors)
    # Convert sets to sorted lists
    return {k: sorted(list(v)) for k, v in color_map.items()}

def build_unified_records():
    PROCESSED.mkdir(parents=True, exist_ok=True)

    # Load parts + categories
    parts = pd.read_csv(SRC_PARTS, dtype={"part_num": "string", "name": "string", "part_cat_id": "Int64"})
    cats = pd.read_csv(SRC_CATS, dtype={"id": "Int64", "name": "string"}).rename(columns={"id":"part_cat_id", "name":"category_name"})
    parts = parts.merge(cats, how="left", on="part_cat_id")

    # Compute color variants
    color_variants = compute_color_variants()

    # Prepare outputs
    if OUT_JSONL.exists():
        OUT_JSONL.unlink()
    preview_rows = []

    # Emit universal JSONL per part
    with open(OUT_JSONL, "w", encoding="utf-8") as f:
        for _, row in parts.iterrows():
            part_num = row["part_num"]
            name = (row["name"] or "").strip() if pd.notna(row["name"]) else ""
            cat = (row["category_name"] or "").strip() if pd.notna(row["category_name"]) else ""

            record = {
                "id": f"part:rb:{part_num}",
                "type": "part",  # minifigs handled separately later
                "name": name,
                "source_ids": {"rb": {"part_num": part_num}},
                "external_links": {"rebrickable": f"https://rebrickable.com/parts/{part_num}/"},
                "category": {
                    "id": f"rb:{int(row['part_cat_id'])}" if pd.notna(row["part_cat_id"]) else None,
                    "name": cat or None,
                },
                "geometry": {
                    # We do not have LDraw link mapping yet at this step.
                    "ldraw": {"file": None, "dimensions_lu": {"x": None, "y": None, "z": None}},
                    "mesh": {"glb_path": None, "checksum": None},
                    "voxel": {"vox_path": None, "grid_size": None},
                },
                "color_compatibility": {
                    "valid_color_ids": color_variants.get(part_num, []),
                    "color_substitution_rules": [],
                },
                "marketplace": {
                    "bricklink": {"part_id": None, "avg_price_usd": None, "stock": None},
                    "brickowl": {"part_id": None, "avg_price_usd": None, "stock": None},
                    "rebrickable": {"element_count": None},  # placeholder, can be filled later if needed
                },
                "tags": [],
                "metadata": {
                    "created_at": int(time.time()),
                    "updated_at": int(time.time()),
                    "deprecation": {"is_deprecated": False, "note": None},
                },
                "search": {
                    "aliases": [],
                    "keywords": [name, cat, part_num],
                    "embedding_id": None,
                },
                "bom_rules": {"preferred_substitutes": [], "prohibited_in_builds": False},
                "version": {"schema": "1.0.0", "source": "rebrickable_csv:parts+elements"},
            }

            f.write(json.dumps(record, ensure_ascii=False) + "\n")

            # Keep a small preview subset for quick viewing
            preview_rows.append({
                "id": record["id"],
                "name": name,
                "category": cat,
                "rb_part_num": part_num,
                "valid_color_ct": len(record["color_compatibility"]["valid_color_ids"])
            })

    # Write CSV preview (top 10k rows to keep it light)
    preview_df = pd.DataFrame(preview_rows[:10000])
    preview_df.to_csv(OUT_CSV, index=False)

def main():
    # Guard: inputs present
    missing = [str(p) for p in REQUIRED if not p.exists()]
    if missing:
        raise SystemExit(f"Missing required source files:\n" + "\n".join(missing))

    current_inputs = inputs_signature()
    manifest = load_manifest()

    if manifest_matches(current_inputs, manifest):
        print("No changes in inputs; skipping rebuild.")
        print(f"Existing outputs:\n  {OUT_JSONL}\n  {OUT_CSV}")
        return

    print("Building universal parts from Rebrickable…")
    build_unified_records()
    save_manifest({"inputs": current_inputs, "generated": int(time.time())})
    print("Done.")
    print(f"Wrote:\n  {OUT_JSONL}\n  {OUT_CSV}\n  {MANIFEST}")

if __name__ == "__main__":
    main()
