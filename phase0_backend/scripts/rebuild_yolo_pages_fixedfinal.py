#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rebuild YOLO dataset for full-page step_panel detection with globally-unique filenames.

Inputs (relative to repo root):
- data/processed/panels/page_annot_sample.list              (400 pages you reviewed)
- data/processed/panels/annotations.step_panels.jsonl       (your drawn boxes)

Outputs:
- data/processed/panels/yolo_pages_FIXEDFINAL/
    data.yaml
    images/{train,val}/*.png
    labels/{train,val}/*.txt
- data/reviews/yolo_pages_FIXEDFINAL_val_overlays/*.png     (100 random overlays)

Design:
- Every copied page is named "{pdf_id}__page-{page:04d}.png" to guarantee uniqueness.
- Each label file is built only from JSONL entries with the SAME (pdf_id,page) -> same file.
- Pages in your list that have no matching JSONL entries become **negatives** (empty .txt).
- A ~90/10 split is used (train/val), but you can tweak SPLIT_VAL if you like.

"""

import json, os, random, re, shutil
from pathlib import Path
from PIL import Image, ImageDraw

REPO = Path(__file__).resolve().parents[2]  # .../BrickBuilder
LIST_FILE = REPO / "data/processed/panels/page_annot_sample.list"
ANN_JSONL = REPO / "data/processed/panels/annotations.step_panels.jsonl"
OUT_ROOT  = REPO / "data/processed/panels/yolo_pages_FIXEDFINAL"
REVIEW_DIR = REPO / "data/reviews/yolo_pages_FIXEDFINAL_val_overlays"
SPLIT_VAL = 0.10  # 10% val

def err(msg): print(f"[ERROR] {msg}")
def info(msg): print(f"[info]  {msg}")

def parse_pdf_page_from_path(p: Path):
    """
    Expect source like: data/instructions/<pdf_id>/page-0031.png
    Returns (pdf_id:str, page:int) or (None,None) if not matched.
    """
    m = re.search(r"instructions[/\\]([^/\\]+)[/\\]page-(\d+)\.png$", str(p))
    if not m: return None, None
    return m.group(1), int(m.group(2))

def to_unique_name(pdf_id: str, page: int) -> str:
    return f"{pdf_id}__page-{page:04d}.png"

def load_list():
    if not LIST_FILE.exists():
        raise FileNotFoundError(f"Missing list file: {LIST_FILE}")
    paths = []
    for line in LIST_FILE.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s: continue
        q = (REPO / s) if not os.path.isabs(s) else Path(s)
        q = q.resolve()
        if q.exists():
            paths.append(q)
    return paths

def load_annotations():
    if not ANN_JSONL.exists():
        raise FileNotFoundError(f"Missing annotations JSONL: {ANN_JSONL}")
    ann_map = {}  # (pdf_id,page) -> list of boxes absolute (x0,y0,x1,y1,img_w,img_h)
    with ANN_JSONL.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try:
                r = json.loads(line)
            except Exception:
                continue
            pdf_id = r.get("pdf_id")
            page   = r.get("page")
            x0,y0,x1,y1 = r.get("x0"), r.get("y0"), r.get("x1"), r.get("y1")
            W,H = r.get("img_w"), r.get("img_h")
            if not (pdf_id and isinstance(page,int) and None not in (x0,y0,x1,y1,W,H)):
                continue
            key = (pdf_id, int(page))
            ann_map.setdefault(key, []).append((float(x0),float(y0),float(x1),float(y1),float(W),float(H)))
    return ann_map

def write_yolo_label(out_txt: Path, boxes):
    """
    boxes: list of (x0,y0,x1,y1,W,H) absolute pixel coordinates
    Write YOLO lines: cls cx cy w h   (normalized)
    """
    out_txt.parent.mkdir(parents=True, exist_ok=True)
    with out_txt.open("w", encoding="ascii") as f:
        for (x0,y0,x1,y1,W,H) in boxes:
            x0,x1 = max(0,min(W-1,x0)), max(0,min(W-1,x1))
            y0,y1 = max(0,min(H-1,y0)), max(0,min(H-1,y1))
            if x1<=x0 or y1<=y0: continue
            cx = ((x0+x1)/2.0)/W
            cy = ((y0+y1)/2.0)/H
            bw = (x1-x0)/W
            bh = (y1-y0)/H
            # single-class 'step_panel' -> 0
            f.write(f"0 {cx:.6f} {cy:.6f} {bw:.6f} {bh:.6f}\n")

def build():
    pages = load_list()
    anns  = load_annotations()

    # Wipe and recreate out structure
    if OUT_ROOT.exists():
        shutil.rmtree(OUT_ROOT)
    (OUT_ROOT / "images" / "train").mkdir(parents=True, exist_ok=True)
    (OUT_ROOT / "images" / "val").mkdir(parents=True, exist_ok=True)
    (OUT_ROOT / "labels" / "train").mkdir(parents=True, exist_ok=True)
    (OUT_ROOT / "labels" / "val").mkdir(parents=True, exist_ok=True)

    # Split ~90/10
    random.seed(42)
    pages_shuffled = pages[:]
    random.shuffle(pages_shuffled)
    cut = max(1, int(round(len(pages_shuffled) * (1.0 - SPLIT_VAL))))
    train_pages = set(pages_shuffled[:cut])
    val_pages   = set(pages_shuffled[cut:])

    pos = neg = 0
    for src_path in pages:
        pdf_id, page = parse_pdf_page_from_path(src_path)
        if not pdf_id:
            # keep but skip labels (treat as negative), still copy uniquely
            pdf_id, page = "UNKNOWN", 0

        split = "train" if src_path in train_pages else "val"
        uniq  = to_unique_name(pdf_id, page)

        dst_img = OUT_ROOT / "images" / split / uniq
        dst_lbl = OUT_ROOT / "labels" / split / (Path(uniq).with_suffix(".txt").name)

        # copy image
        dst_img.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, dst_img)

        # labels
        key = (pdf_id, page)
        if key in anns:
            write_yolo_label(dst_lbl, anns[key])
            pos += 1
        else:
            # explicit negative
            dst_lbl.parent.mkdir(parents=True, exist_ok=True)
            with dst_lbl.open("w", encoding="ascii") as f:
                f.write("")  # empty label file
            neg += 1

    # data.yaml
    (OUT_ROOT / "data.yaml").write_text(
        f"path: {OUT_ROOT.as_posix()}\n"
        f"train: images/train\n"
        f"val: images/val\n"
        f"nc: 1\n"
        f"names: [step_panel]\n", encoding="utf-8"
    )

    # stats
    def count_files(p): return len(list(Path(p).glob("*")))
    tr_i = count_files(OUT_ROOT / "images" / "train")
    tr_l = count_files(OUT_ROOT / "labels" / "train")
    va_i = count_files(OUT_ROOT / "images" / "val")
    va_l = count_files(OUT_ROOT / "labels" / "val")

    print()
    print(f"✅ NEW DATASET: {OUT_ROOT}")
    print(f"train imgs/labels: {tr_i} / {tr_l}")
    print(f"val   imgs/labels: {va_i} / {va_l}")
    print(f"positives: {pos}   negatives: {neg}")

def draw_overlays(n=100):
    if REVIEW_DIR.exists():
        shutil.rmtree(REVIEW_DIR)
    REVIEW_DIR.mkdir(parents=True, exist_ok=True)

    val_imgs = list((OUT_ROOT / "images" / "val").glob("*.png")) + \
               list((OUT_ROOT / "images" / "val").glob("*.jpg"))
    random.seed(123)
    sample = random.sample(val_imgs, min(n, len(val_imgs)))

    for im_path in sample:
        lbl = OUT_ROOT / "labels" / "val" / (im_path.stem + ".txt")
        im = Image.open(im_path).convert("RGB")
        W, H = im.size
        draw = ImageDraw.Draw(im, "RGBA")
        if lbl.exists() and lbl.stat().st_size > 0:
            for line in lbl.read_text(encoding="ascii").splitlines():
                parts = line.strip().split()
                if len(parts) < 5: continue
                _, cx, cy, bw, bh = map(float, parts[:5])
                x0 = (cx - bw/2.0) * W
                y0 = (cy - bh/2.0) * H
                x1 = (cx + bw/2.0) * W
                y1 = (cy + bh/2.0) * H
                draw.rectangle([x0,y0,x1,y1], outline=(0,255,0,255), width=max(2,int(W/600)))
        out = REVIEW_DIR / im_path.name
        im.save(out, "PNG")

    print(f"Overlays -> {REVIEW_DIR}  (wrote {len(sample)})")

if __name__ == "__main__":
    build()
    draw_overlays(100)
