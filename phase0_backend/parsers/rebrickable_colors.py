import csv

def load_rebrickable_colors(path="data/raw/rebrickable/colors.csv"):
    """
    Load Rebrickable colors CSV.
    Returns dict { rgb_hex: {rb_color_id, name} }
    """
    mapping = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rb_id = row.get("id")
            name = row.get("name")
            rgb = row.get("rgb")
            if not rgb:
                continue
            # Normalize to #RRGGBB
            hexv = "#" + rgb.strip().upper()
            mapping[hexv] = {"rb_color_id": rb_id, "name": name}
    return mapping
