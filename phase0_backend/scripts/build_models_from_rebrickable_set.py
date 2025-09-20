import argparse, csv, json
from pathlib import Path
from datetime import datetime, timezone

def load_sets(path):
    d={}
    with open(path, newline="", encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r: d[row["set_num"]]=row
    return d

def load_themes(path):
    d={}
    with open(path, newline="", encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r: d[int(row["id"])]=row["name"]
    return d

def load_inventories(path):
    d={}
    with open(path, newline="", encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r: d[int(row["id"])]={"id":int(row["id"]), "set_num":row["set_num"], "version":int(row["version"]) if row.get("version") else None}
    return d

def load_inventory_sets(path):
    from collections import defaultdict
    d=defaultdict(list)
    with open(path, newline="", encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r:
            d[row["set_num"]].append({"inventory_id":int(row["inventory_id"]), "quantity":int(row["quantity"])})
    return d

def load_inventory_parts(path):
    from collections import defaultdict
    d=defaultdict(list)
    with open(path, newline="", encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r:
            d[int(row["inventory_id"])].append({
                "part_num": row["part_num"],
                "color_id": row["color_id"],
                "quantity": int(row["quantity"]),
                "is_spare": (row.get("is_spare") == "t")
            })
    return d

def load_colors_rb(path):
    # rb_color_id -> '#RRGGBB'
    m={}
    with open(path, newline="", encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r:
            rgb=row.get("rgb")
            if rgb:
                m[str(row["id"])] = "#"+rgb.strip().upper()
    return m

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--set-num", required=True)
    ap.add_argument("--rb-root", default="data/raw/rebrickable")
    ap.add_argument("--out", default="data/processed/models/master_models_from_sets.jsonl")
    args=ap.parse_args()

    root = Path(args.rb_root)
    sets = load_sets(str(root/"sets.csv"))
    themes = load_themes(str(root/"themes.csv"))
    inventories = load_inventories(str(root/"inventories.csv"))
    inv_sets = load_inventory_sets(str(root/"inventory_sets.csv"))
    inv_parts = load_inventory_parts(str(root/"inventory_parts.csv"))
    rb_colors = load_colors_rb(str(root/"colors.csv"))

    s = sets.get(args.set_num)
    if not s:
        raise SystemExit(f"Set not found: {args.set_num}")

    theme_name = themes.get(int(s["theme_id"])) if s.get("theme_id") else None

    # choose the latest inventory id for this set (max id)
    rel = inv_sets.get(args.set_num, [])
    bom=[]
    if rel:
        inv_ids = sorted([x["inventory_id"] for x in rel])
        iid = inv_ids[-1]
        rows = inv_parts.get(iid, [])
        # aggregate by (part_num,color_id) excluding spares
        from collections import defaultdict
        agg=defaultdict(int)
        for r in rows:
            if r.get("is_spare"):
                continue
            key=(r["part_num"], r["color_id"])
            agg[key]+=int(r["quantity"])
        for (pnum,cid), qty in sorted(agg.items(), key=lambda kv: (-kv[1], kv[0])):
            item={"rb_part_num": pnum, "qty": qty, "rb_color_id": str(cid)}
            hexv = rb_colors.get(str(cid))
            if hexv: item["color_rgb_hex"]=hexv
            bom.append(item)

    piece_count = int(s["num_parts"]) if s.get("num_parts") else sum([r["qty"] for r in bom])

    rec = {
        "id": f"model:rb:{args.set_num}",
        "type": "model",
        "source_ids": { "rb": {"set_num": args.set_num } },
        "name": s["name"],
        "metadata": {
            "piece_count": piece_count,
            "year": int(s["year"]) if s.get("year") else None,
            "theme": theme_name,
            "last_updated": datetime.now(timezone.utc).isoformat()
        },
        "geometry": {
            "ldraw": { "root_file": "", "scale": { "units": "LDU", "to_mm": 0.4 } }
        },
        "bom": bom,
        "steps": None,
        "instructions": {
            "kind": "reference",
            "source": f"https://rebrickable.com/sets/{args.set_num}/"
        },
        "links": {
            "parser_version": "rb_set_builder_v0.1",
            "build_tools": "python"
        }
    }

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "a", encoding="utf-8") as w:
        w.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"Wrote set-model record for {args.set_num} → {args.out}")

if __name__ == "__main__":
    main()


