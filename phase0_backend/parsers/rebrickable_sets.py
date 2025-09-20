import csv
from collections import defaultdict
from pathlib import Path

def load_sets(path: str):
    """Load sets.csv into dict keyed by set_num."""
    sets = {}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sets[row["set_num"]] = {
                "set_num": row["set_num"],
                "name": row["name"],
                "year": int(row["year"]) if row["year"] else None,
                "theme_id": int(row["theme_id"]) if row["theme_id"] else None,
                "num_parts": int(row["num_parts"]) if row["num_parts"] else None,
                "img_url": row["img_url"] or None,
            }
    return sets


def load_themes(path: str):
    """Load themes.csv into dict keyed by theme_id."""
    themes = {}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            tid = int(row["id"])
            themes[tid] = {
                "id": tid,
                "name": row["name"],
                "parent_id": int(row["parent_id"]) if row["parent_id"] else None,
            }
    return themes


def load_colors(path: str):
    """Load colors.csv into dict keyed by color_id."""
    colors = {}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = int(row["id"])
            colors[cid] = {
                "id": cid,
                "name": row["name"],
                "rgb": row["rgb"],
                "is_trans": row.get("is_trans"),
            }
    return colors


def load_inventories(path: str):
    """Load inventories.csv into dict keyed by inventory_id."""
    inventories = {}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            iid = int(row["id"])
            inventories[iid] = {
                "id": iid,
                "version": row.get("version"),
                "set_num": row.get("set_num"),
            }
    return inventories


def load_inventory_sets(path: str):
    """Load inventory_sets.csv into dict of set_num -> [inventory_id, quantity]."""
    inv_sets = defaultdict(list)
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            inv_sets[row["set_num"]].append({
                "inventory_id": int(row["inventory_id"]),
                "quantity": int(row["quantity"]),
            })
    return inv_sets


def load_inventory_parts(path: str):
    """Load inventory_parts.csv into dict keyed by inventory_id, listing parts."""
    inv_parts = defaultdict(list)
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            inv_parts[int(row["inventory_id"])].append({
                "part_num": row["part_num"],
                "color_id": int(row["color_id"]),
                "quantity": int(row["quantity"]),
                "is_spare": row.get("is_spare") == "t",
            })
    return inv_parts
