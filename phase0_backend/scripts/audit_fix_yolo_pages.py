# file: phase0_backend/scripts/audit_fix_yolo_pages.py
from __future__ import annotations
import json, os, random, csv
from pathlib import Path
from collections import defaultdict
from PIL import Image, ImageDraw

# --- CONFIG ---
REPO = Path(__file__).resolve().parents[2]  # repo root
JSONL = REPO / "data/processed/panels/annotations.step_panels.FILTERED.jsonl"
DS    = REPO / "data/processed/panels/yolo_pages_UNIQUE"          # images/labels live here
OVERL = REPO / "data/reviews/yolo_pages_UNIQUE_val_overlays_big"   # we'll (re)write 100 here
CSV   = REPO / "data/reviews/yolo_pages_UNIQUE_label_audit.csv"

# --- helpers ---
def iter_jsonl(path):
    # tolerate UTF-8 BOM and blank lines
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            yield json.loads(ln)

def abs_path(p: str | Path) -> Path:
    p = Path(p)
    return p if p.is_absolute() else (REPO / p)

def yolo_line(x0,y0,x1,y1,w,h,cls=0) -> str:
    # clamp and normalize
    x0 = max(0, min(w-1, float(x0))); x1 = max(0, min(w-1, float(x1)))
    y0 = max(0, min(h-1, float(y0))); y1 = max(0, min(h-1, float(y1)))
    if x1 < x0: x0, x1 = x1, x0
    if y1 < y0: y0, y1 = y1, y0
    cx = ((x0 + x1) / 2.0) / w
    cy = ((y0 + y1) / 2.0) / h
    bw = (x1 - x0) / w
    bh = (y1 - y0) / h
    return f"{cls} {cx:.6f} {cy:.6f} {bw:.6f} {bh:.6f}"

# --- load annotations grouped by image ---
rows_by_img: dict[Path, list[dict]] = defaultdict(list)
bad_paths = 0
with open(JSONL, "r", encoding="utf-8") as f:
    for ln in f:
        ln = ln.strip()
        if not ln: continue
        r = json.loads(ln)
        src = abs_path(r.get("source_path",""))
        if not src.exists():
            bad_paths += 1
            continue
        rows_by_img[src].append(r)

# --- verify and (re)write labels using REAL sizes ---
mismatch_records = []
label_counts = {"train":0,"val":0}
img_counts   = {"train":0,"val":0}

# build a map from DS images to split so we know where to drop labels
split_by_img: dict[Path,str] = {}
for split in ("train","val"):
    for img in (DS / "images" / split).glob("*.*"):
        split_by_img[img.resolve()] = split

# map source -> actual dataset image path
# (filenames in UNIQUE are like "{pdf_id}__page-XXXX.png")
def dataset_img_for(src: Path) -> Path | None:
    pdf_id = src.parts[-2]   # e.g., "70615-1"
    page   = src.stem        # e.g., "page-0132"
    name   = f"{pdf_id}__{page}{src.suffix.lower()}"
    for split in ("train","val"):
        cand = (DS / "images" / split / name)
        if cand.exists():
            return cand
    return None

# wipe labels to rebuild from scratch (keep negatives)
for split in ("train","val"):
    (DS / "labels" / split).mkdir(parents=True, exist_ok=True)
    for txt in (DS / "labels" / split).glob("*.txt"):
        txt.unlink()

for src, rows in rows_by_img.items():
    ds_img = dataset_img_for(src)
    if ds_img is None:
        continue  # this page isn’t in the UNIQUE dataset split
    split = split_by_img[ds_img.resolve()]
    w_real, h_real = Image.open(ds_img).size

    # audit consistency of recorded img_w/img_h
    for r in rows:
        w_rec = int(r.get("img_w", w_real))
        h_rec = int(r.get("img_h", h_real))
        if (w_rec, h_rec) != (w_real, h_real):
            mismatch_records.append({
                "pdf_id": r.get("pdf_id",""),
                "page": r.get("page",""),
                "source_path": str(src),
                "img_in_ds": str(ds_img),
                "rec_w": w_rec, "rec_h": h_rec,
                "real_w": w_real, "real_h": h_real
            })

    # write label lines using REAL w/h
    lines = []
    for r in rows:
        x0,y0,x1,y1 = r["x0"], r["y0"], r["x1"], r["y1"]
        lines.append(yolo_line(x0,y0,x1,y1,w_real,h_real,cls=0))
    lbl = (DS / "labels" / split / (ds_img.stem + ".txt"))
    lbl.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="ascii")
    label_counts[split] += 1
    img_counts[split]   += 1

# keep negatives: ensure each negative still has an empty label file
for split in ("train","val"):
    for img in (DS / "images" / split).glob("*.png"):
        lbl = (DS / "labels" / split / (img.stem + ".txt"))
        if not lbl.exists():
            lbl.write_text("", encoding="ascii")

# --- write audit CSV ---
CSV.parent.mkdir(parents=True, exist_ok=True)
with open(CSV, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["pdf_id","page","source_path","img_in_ds","rec_w","rec_h","real_w","real_h"])
    w.writeheader()
    for m in mismatch_records:
        w.writerow(m)

print(f"Rebuilt labels with REAL sizes. Mismatch rows: {len(mismatch_records)}  Bad/missing source paths: {bad_paths}")
print(f"Labels written -> train:{label_counts['train']}  val:{label_counts['val']}")
print(f"Audit CSV -> {CSV}")

# --- overlays for 100 random val images ---
OVERL.mkdir(parents=True, exist_ok=True)
val_imgs = list((DS/"images"/"val").glob("*.png"))
random.shuffle(val_imgs)
sample = val_imgs[:100]

def draw_overlay(img_path: Path, lbl_path: Path, out_path: Path):
    im = Image.open(img_path).convert("RGB")
    W,H = im.size
    dr = ImageDraw.Draw(im, "RGBA")
    if lbl_path.exists():
        for ln in lbl_path.read_text().splitlines():
            ln = ln.strip()
            if not ln:  # negative: no boxes
                continue
            parts = ln.split()
            if len(parts) < 5: continue
            cx,cy,bw,bh = map(float, parts[1:5])
            cx,cy,bw,bh = cx*W, cy*H, bw*W, bh*H
            x0 = int(cx - bw/2); y0 = int(cy - bh/2)
            x1 = int(cx + bw/2); y1 = int(cy + bh/2)
            dr.rectangle([x0,y0,x1,y1], outline=(0,255,0,255), width=max(2, W//600))
    im.save(out_path)

written = 0
for img in sample:
    lbl = DS / "labels" / "val" / (img.stem + ".txt")
    out = OVERL / img.name
    draw_overlay(img, lbl, out)
    written += 1

print(f"Overlay previews (val): wrote {written} to {OVERL}")
