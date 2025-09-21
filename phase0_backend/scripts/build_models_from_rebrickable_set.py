import argparse, csv, json
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

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

def load_inventories_by_set(path):
    # set_num -> list of (id, version)
    m=defaultdict(list)
    with open(path, newline="", encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r:
            try:
                inv_id = int(row["id"])
                ver    = int(row["version"]) if row.get("version") else 0
                m[row["set_num"]].append((inv_id, ver))
            except Exception:
                continue
    return m

def load_inventory_sets(path):
    # set_num -> list of inventory_id (fallback only)
    m=defaultdict(list)
    with open(path, newline="", encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r:
            try:
                m[row["set_num"]].append(int(row["inventory_id"]))
            except Exception:
                continue
    return m

def load_inventory_parts_grouped(path):
    # inventory_id -> {(part_num,color_id) -> qty}, excluding spares
    m=defaultdict(lambda: defaultdict(int))
    with open(path, newline="", encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r:
            try:
                if row.get("is_spare") == "t":
                    continue
                iid = int(row["inventory_id"])
                key = (row["part_num"], row["color_id"])
                m[iid][key] += int(row["quantity"])
            except Exception:
                continue
    return m

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
    sets         = load_sets(str(root/"sets.csv"))
    themes       = load_themes(str(root/"themes.csv"))
    inv_by_set   = load_inventories_by_set(str(root/"inventories.csv"))
    inv_sets_fb  = load_inventory_sets(str(root/"inventory_sets.csv"))  # fallback
    inv_parts_g  = load_inventory_parts_grouped(str(root/"inventory_parts.csv"))
    rb_colors    = load_colors_rb(str(root/"colors.csv"))

    s = sets.get(args.set_num)
    if not s:
        raise SystemExit(f"Set not found: {args.set_num}")

    theme_name = themes.get(int(s["theme_id"])) if s.get("theme_id") else None

    # Choose inventory_id:
    # 1) prefer inventories.csv (max version for set_num)
    # 2) fallback to inventory_sets.csv (max id)
    iid = None
    lst = inv_by_set.get(args.set_num)
    if lst:
        lst.sort(key=lambda t: (t[1], t[0]))  # sort by (version, id)
        iid = lst[-1][0]
    else:
        fb = inv_sets_fb.get(args.set_num, [])
        if fb:
            iid = max(fb)

    # Build BOM from inventory_parts.csv for this iid
    bom=[]
    if iid is not None:
        agg = inv_parts_g.get(iid, {})
        for (pnum, cid), qty in sorted(agg.items(), key=lambda kv: (-kv[1], kv[0])):
            item = {"rb_part_num": pnum, "qty": int(qty), "rb_color_id": str(cid)}
            hexv = rb_colors.get(str(cid))
            if hexv: item["color_rgb_hex"]=hexv
            bom.append(item)

    piece_count = int(s["num_parts"]) if s.get("num_parts") else sum([r["qty"] for r in bom])

    rec = {
        "id": f"model:rb:{args.set_num}",
        "type": "model",
        "source_ids": { "rb": { "set_num": args.set_num } },
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
            "parser_version": "rb_set_builder_v0.2",
            "build_tools": "python"
        }
    }

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "a", encoding="utf-8") as w:
        w.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"Wrote set-model record for {args.set_num} → {args.out} (inventory_id={iid})")

if __name__ == "__main__":
    main()
