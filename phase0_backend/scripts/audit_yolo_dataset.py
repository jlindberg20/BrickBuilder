# audit_yolo_dataset.py
# Standalone overlay+QC tool for a YOLO detection dataset (pages + step_panel labels).
# - Reads images/val + labels/val from a dataset root.
# - Randomly samples N images and draws green boxes (if any) onto copies.
# - Writes a CSV with counts/areas and prints dataset stats.
# - Zero dependencies beyond Pillow (pip install pillow).

import argparse, csv, os, random, sys
from pathlib import Path
from typing import List, Tuple
from PIL import Image, ImageDraw

def yolo_to_xyxy(line: str, W: int, H: int) -> Tuple[int, int, int, int]:
    """
    YOLO txt line: 'cls cx cy w h' (all normalized).
    Returns integer pixel xyxy (clamped).
    """
    parts = line.strip().split()
    if len(parts) < 5:
        return None
    _, cx, cy, bw, bh = parts[:5]
    cx = float(cx) * W
    cy = float(cy) * H
    bw = float(bw) * W
    bh = float(bh) * H
    x0 = int(round(cx - bw / 2.0))
    y0 = int(round(cy - bh / 2.0))
    x1 = int(round(cx + bw / 2.0))
    y1 = int(round(cy + bh / 2.0))
    # clamp
    x0 = max(0, min(W - 1, x0))
    y0 = max(0, min(H - 1, y0))
    x1 = max(0, min(W - 1, x1))
    y1 = max(0, min(H - 1, y1))
    if x1 <= x0 or y1 <= y0:
        return None
    return (x0, y0, x1, y1)

def collect_split(ds_root: Path, split: str) -> List[Tuple[Path, Path]]:
    imgs_dir  = ds_root / "images" / split
    labels_dir= ds_root / "labels" / split
    pairs = []
    if not imgs_dir.exists():
        return pairs
    for p in imgs_dir.iterdir():
        if not p.is_file(): 
            continue
        if p.suffix.lower() not in {".png", ".jpg", ".jpeg", ".bmp", ".webp", ".tif", ".tiff"}:
            continue
        lbl = labels_dir / (p.stem + ".txt")
        pairs.append((p, lbl))
    return pairs

def draw_overlay(img_path: Path, lbl_path: Path, dst_path: Path) -> Tuple[int, float, float]:
    """
    Draw boxes (if any) and save PNG to dst_path.
    Returns (#boxes, mean_area_pct, mean_ar)
    """
    im = Image.open(img_path).convert("RGB")
    W, H = im.size
    draw = ImageDraw.Draw(im)
    n = 0
    area_pcts = []
    ars = []
    if lbl_path.exists():
        txt = lbl_path.read_text(encoding="utf-8", errors="ignore")
        for ln in txt.splitlines():
            if not ln.strip():
                continue
            xyxy = yolo_to_xyxy(ln, W, H)
            if not xyxy:
                continue
            x0, y0, x1, y1 = xyxy
            # box + label bar
            draw.rectangle([x0, y0, x1, y1], outline=(0, 255, 0), width=max(2, W // 600))
            n += 1
            area = (x1 - x0) * (y1 - y0)
            area_pcts.append(100.0 * area / float(W * H))
            ars.append((x1 - x0) / float(y1 - y0))
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    im.save(dst_path.with_suffix(".png"))  # always png
    mean_area_pct = sum(area_pcts) / len(area_pcts) if area_pcts else 0.0
    mean_ar = sum(ars) / len(ars) if ars else 0.0
    return n, mean_area_pct, mean_ar

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ds", default=r".\data\processed\panels\yolo_pages_UNIQUE",
                    help="YOLO dataset root (contains images/{train,val}, labels/{train,val})")
    ap.add_argument("--split", default="val", choices=["train","val"], help="Split to preview")
    ap.add_argument("--n", type=int, default=100, help="How many random overlays to render")
    ap.add_argument("--out", default=None, help="Output folder for overlays (default: data/reviews/<ds_name>_<split>_overlays_big)")
    args = ap.parse_args()

    ds_root = Path(args.ds).resolve()
    if not ds_root.exists():
        print(f"[ERROR] Dataset root not found: {ds_root}")
        sys.exit(1)

    # Gather pairs
    train_pairs = collect_split(ds_root, "train")
    val_pairs   = collect_split(ds_root, "val")

    # Quick counts
    def count_pos_neg(pairs):
        pos = neg = 0
        for img, lbl in pairs:
            if not lbl.exists():
                neg += 1
                continue
            content = lbl.read_text(encoding="utf-8", errors="ignore").strip()
            if content:
                pos += 1
            else:
                neg += 1
        return pos, neg

    tr_pos, tr_neg = count_pos_neg(train_pairs)
    va_pos, va_neg = count_pos_neg(val_pairs)

    print(f"\n=== Dataset: {ds_root.name} ===")
    print(f"train imgs/labels: {len(train_pairs)} / {len(train_pairs)}  (pos {tr_pos} | neg {tr_neg})")
    print(f"val   imgs/labels: {len(val_pairs)} / {len(val_pairs)}  (pos {va_pos} | neg {va_neg})")
    print(f"TOTAL imgs: {len(train_pairs)+len(val_pairs)}\n")

    # Prepare output folder + CSV
    if args.out:
        out_dir = Path(args.out)
    else:
        out_dir = Path(r".\data\reviews") / f"{ds_root.name}_{args.split}_overlays_big"
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "qc.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["image","labels","boxes","mean_area_pct","mean_ar"])

    # Choose split and random sample
    pairs = train_pairs if args.split == "train" else val_pairs
    if not pairs:
        print(f"[ERROR] No pairs found for split={args.split}")
        sys.exit(1)

    sample = random.sample(pairs, k=min(args.n, len(pairs)))
    wrote = 0
    for img, lbl in sample:
        dst = out_dir / f"{img.stem}__overlay.png"
        boxes, mean_area, mean_ar = draw_overlay(img, lbl, dst)
        with csv_path.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([str(img), str(lbl), boxes, f"{mean_area:.3f}", f"{mean_ar:.3f}"])
        wrote += 1

    print(f"Overlays -> {out_dir}  (wrote {wrote})")
    print(f"CSV      -> {csv_path}")

if __name__ == "__main__":
    main()
