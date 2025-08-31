import os, json, time, hashlib
from pathlib import Path
from typing import Dict, Any
import pandas as pd

# ------------------ Config ------------------
ENV = {
    "DATA_RAW_REB": os.getenv("DATA_RAW_REB", "./data/raw/rebrickable"),
    "DATA_PROCESSED": os.getenv("DATA_PROCESSED", "./data/processed"),
}

REB = Path(ENV["DATA_RAW_REB"])
PROCESSED = Path(ENV["DATA_PROCESSED"]) / "rebrickable"
OUT_JSONL = PROCESSED / "parts_unified.jsonl"
OUT_CSV = PROCESSED / "parts_preview.csv"
MANIFEST = PROCESSED / "_manifest.json"

CSV_FILES = {
    "parts": "parts.csv",
    "categories": "part_categories.csv",
    "colors": "colors.csv",
    "elements": "elements.csv",
    "relationships": "part_relationships.csv",
    "minifigs": "minifigs.csv",
    "inventories": "inventories.csv",
    "inventory_parts": "inventory_parts.csv",
    "inventory_minifigs": "inventory_minifigs.csv",
}

# ------------------ Utilities ------------------
def file_fingerprint(p: Path) -> Dict[str, Any]:
    stat = p.stat()
    return {"size": stat.st_size, "mtime": int(stat.st_mtime)}

def load_manifest() -> Dict[str, Any]:
    if MANIFEST.exists():
        return json.loads(MANIFEST.read_text())
    return {}

def save_manifest(d: Dict[str, Any]):
    PROCESSED.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(d, indent=2, sort_keys=True))

def inputs_signature() -> Dict[str, Any]:
    return {k: file_fingerprint(REB / fn) for k, fn in CSV_FILES.items()}

def manifest_matches(current: Dict[str, Any], manifest: Dict[str, Any]) -> bool:
    return manifest.get("inputs", {}) == current

def now_ts():
    return int(time.time())

# ------------------ Ingestion ------------------
def load_all() -> Dict[str, pd.DataFrame]:
    dfs = {}
    for key, fn in CSV_FILES.items():
        path = REB / fn
        if not path.exists():
            raise FileNotFoundError(f"Missing required file: {path}")
        dfs[key] = pd.read_csv(path)
    return dfs

# ------------------ Enrichment logic ------------------
def build_records(dfs: Dict[str, pd.DataFrame]):
    ts = now_ts()

    parts = dfs["parts"].merge(
        dfs["categories"].rename(columns={"id": "part_cat_id", "name": "category_name"}),
        how="left", on="part_cat_id"
    )

    # --- Colors
    colors = dfs["colors"].rename(columns={"id": "rb_color_id"})

    color_lookup = {
        row["rb_color_id"]: {
            "id": f"rb:{row['rb_color_id']}",
            "name": row["name"],
            "rgb": row["rgb"],
            "is_trans": bool(row["is_trans"])
        }
        for _, row in colors.iterrows()
    }

    # --- Elements: part+color combos
    elem_df = dfs["elements"][["part_num", "color_id"]].dropna()
    part_to_colors = (
        elem_df.groupby("part_num")["color_id"]
        .apply(lambda s: sorted(set(int(x) for x in s)))
        .to_dict()
    )

    # --- Inventory usage counts
    ip = dfs["inventory_parts"][["inventory_id", "part_num"]]
    su = ip.groupby("part_num")["inventory_id"].nunique().to_dict()

    # --- Relationships
    rels = dfs["relationships"]

    # --- Minifigs
    minifigs = dfs["minifigs"]
    im = dfs["inventory_minifigs"][["inventory_id", "fig_num"]]
    su_minifigs = im.groupby("fig_num")["inventory_id"].nunique().to_dict()

    # ---------------- Build records ----------------
    preview_rows = []

    if OUT_JSONL.exists():
        OUT_JSONL.unlink()
    with open(OUT_JSONL, "w", encoding="utf-8") as f:

        # Parts
        for _, row in parts.iterrows():
            part_num = row["part_num"]
            color_ids = part_to_colors.get(part_num, [])
            color_variants = [color_lookup[cid] for cid in color_ids if cid in color_lookup]

            rel_sub = rels[(rels["parent_part_num"] == part_num) | (rels["child_part_num"] == part_num)]
            rel_list = []
            for _, rr in rel_sub.iterrows():
                rel_list.append({
                    "rel_type": rr["rel_type"],
                    "parent": rr["parent_part_num"],
                    "child": rr["child_part_num"],
                })

            record = {
                "id": f"part:rb:{part_num}",
                "type": "part",
                "name": row["name"],
                "source_ids": {"rb": {"part_num": part_num}},
                "external_links": {"rebrickable": f"https://rebrickable.com/parts/{part_num}/"},
                "category": {"id": f"rb:{row['part_cat_id']}", "name": row["category_name"]},
                "geometry": {"ldraw": {"file": None}, "mesh": None, "voxel": None},
                "color_compatibility": {
                    "variants": color_variants,
                    "count": len(color_variants)
                },
                "relationships": rel_list,
                "stats": {
                    "set_usage_count": int(su.get(part_num, 0)),
                    "color_count": len(color_variants),
                },
                "metadata": {"created_at": ts, "updated_at": ts},
            }

            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            preview_rows.append({
                "id": record["id"],
                "name": record["name"],
                "category": record["category"]["name"],
                "colors": record["stats"]["color_count"],
                "set_usage": record["stats"]["set_usage_count"]
            })

        # Minifigs
        for _, row in minifigs.iterrows():
            fig_num = row["fig_num"]
            record = {
                "id": f"minifig:rb:{fig_num}",
                "type": "minifig",
                "name": row["name"],
                "source_ids": {"rb": {"fig_num": fig_num}},
                "external_links": {"rebrickable": f"https://rebrickable.com/minifigs/{fig_num}/"},
                "stats": {
                    "set_usage_count": int(su_minifigs.get(fig_num, 0))
                },
                "metadata": {"created_at": ts, "updated_at": ts},
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            preview_rows.append({
                "id": record["id"],
                "name": record["name"],
                "category": "minifig",
                "colors": None,
                "set_usage": record["stats"]["set_usage_count"]
            })

    pd.DataFrame(preview_rows[:10000]).to_csv(OUT_CSV, index=False)

# ------------------ Main ------------------
def main():
    PROCESSED.mkdir(parents=True, exist_ok=True)

    current_inputs = inputs_signature()
    manifest = load_manifest()
    if manifest_matches(current_inputs, manifest):
        print("No changes; skipping rebuild.")
        print(f"Existing: {OUT_JSONL}")
        return

    print("Building enriched universal parts dataset (2a–2d)…")
    dfs = load_all()
    build_records(dfs)
    save_manifest({"inputs": current_inputs, "generated": now_ts()})
    print("Done. Wrote:", OUT_JSONL, OUT_CSV)

if __name__ == "__main__":
    main()

# Adding note for end
