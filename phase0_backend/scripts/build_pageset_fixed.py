#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build a collision-proof YOLO pages dataset from a list of pages + JSONL annotations.

- Unique filenames: <pdf_id>__<basename>, e.g. 9500-1__page-0018.png
- Negatives: pages in the list with no boxes get an empty .txt label
- Outputs:
    <out>/
      data.yaml
      images/{train,val}/...
      labels/{train,val}/...
- Optional overlays for quick visual QC.
"""

import argparse, json, os, sys, shutil
from pathlib import Path
from typing import Dict, List, Tuple
from PIL import Image, ImageDraw  # pip install pillow

def abspath(p: str, root: Path) -> Path:
    p = p.strip()
    return Path(p).resolve() if os.path.isabs(p) else (root / p).resolve()

def read_list(list_path: Path, root: Path) -> List[Path]:
    lines = [ln.strip() for ln in list_path.read_text(encoding="utf-8-sig").splitlines() if ln.strip()]
    pages = []
    for ln in lines:
        ap = abspath(ln, root)
        if not ap.exists():
            print(f"[warn] Listed file missing: {ln}", file=sys.stderr)
            continue
        pages.append(ap)
    if not pages:
        raise SystemExit("ERROR: list resolved to 0 existing files.")
    return pages

def load_jsonl(jsonl_path: Path, root: Path) -> Dict[Path, List[dict]]:
    by_src: Dict[Path, List[dict]] = {}
    if not jsonl_path.exists():
        print(f"[warn] JSONL missing: {jsonl_path} (negatives-only build)", file=sys.stderr)
        return by_src
    with jsonl_path.open("r", encoding="utf-8-sig") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                r = json.loads(ln)
            except Exception:
                continue
            src = r.get("source_path") or ""
            if not src:
                continue
            sp = abspath(src, root)
            by_src.setdefault(sp, []).append(r)
    return by_src

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def to_unique_name(img_path: Path) -> str:
    """<setid>__<basename>, where setid = parent folder name (e.g., 9500-1)."""
    setid = img_path.parent.name
    return f"{setid}__{img_path.name}"

def to_yolo_lines(rows: List[dict], img_w: int, img_h: int) -> List[str]:
    lines = []
    for r in rows:
        x0 = float(r.get("x0", 0))
        y0 = float(r.get("y0", 0))
        x1 = float(r.get("x1", 0))
        y1 = float(r.get("y1", 0))
        # clamp
        x0 = max(0.0, min(img_w - 1.0, x0))
        x1 = max(0.0, min(img_w - 1.0, x1))
        y0 = max(0.0, min(img_h - 1.0, y0))
        y1 = max(0.0, min(img_h - 1.0, y1))
        bw = abs(x1 - x0)
        bh = abs(y1 - y0)
        if bw <= 0 or bh <= 0:
            continue
        cx = (x0 + x1) / 2.0
        cy = (y0 + y1) / 2.0
        lines.append(f"0 {cx/img_w:.6f} {cy/img_h:.6f} {bw/img_w:.6f} {bh/img_h:.6f}")
    return lines

def write_overlay(dst_png: Path, img: Image.Image, yolo_lines: List[str]):
    im = img.copy().convert("RGB")
    draw = ImageDraw.Draw(im)
    W, H = im.size
    for ln in yolo_lines:
        parts = ln.strip().split()
        if len(parts) < 5:
            continue
        cx = float(parts[1]) * W
        cy = float(parts[2]) * H
        bw = float(parts[3]) * W
        bh = float(parts[4]) * H
        x0 = cx - bw/2
        y0 = cy - bh/2
        x1 = cx + bw/2
        y1 = cy + bh/2
        draw.rectangle([x0, y0, x1, y1], outline=(0,255,0), width=max(2, W//600))
    im.save(dst_png, "PNG")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", required=True, help="Path to the 400-page list (relative/absolute).")
    ap.add_argument("--jsonl", required=True, help="annotations.step_panels[.FILTERED].jsonl")
    ap.add_argument("--out", default="data/processed/panels/yolo_pages_FULLLIST_FIXED2", help="Output dataset root")
    ap.add_argument("--split", type=float, default=0.88, help="Train fraction (default 0.88)")
    ap.add_argument("--overlays", default="", help="Optional overlays dir for VAL images")
    args = ap.parse_args()

    repo = Path.cwd()
    lst  = abspath(args.list, repo)
    jsn  = abspath(args.jsonl, repo)
    out  = abspath(args.out, repo)

    pages = read_list(lst, repo)
    ann   = load_jsonl(jsn, repo)

    # Prepare dirs
    for p in [out, out/"images/train", out/"images/val", out/"labels/train", out/"labels/val"]:
        ensure_dir(p)

    # Split
    split_idx = int(len(pages) * args.split)

    # Build
    train_images = val_images = 0
    train_labels = val_labels = 0

    for i, src in enumerate(pages, 1):
        split = "train" if i <= split_idx else "val"
        sub_img = out / "images" / split
        sub_lbl = out / "labels" / split

        uniq = to_unique_name(src)
        dst_img = sub_img / uniq
        dst_lbl = sub_lbl / (uniq.rsplit(".", 1)[0] + ".txt")

        # copy image
        shutil.copy2(src, dst_img)

        # make label
        rows = ann.get(src, [])
        # try to get size from rows; if absent, read from file
        w = int(rows[0]["img_w"]) if rows and rows[0].get("img_w") else None
        h = int(rows[0]["img_h"]) if rows and rows[0].get("img_h") else None
        if not (w and h):
            with Image.open(dst_img) as im:
                w, h = im.size
        lines = to_yolo_lines(rows, w, h)
        if lines:
            (sub_lbl / dst_lbl.name).write_text("\n".join(lines) + "\n", encoding="ascii")
        else:
            # explicit negative = empty file
            (sub_lbl / dst_lbl.name).write_text("", encoding="ascii")

        if split == "train":
            train_images += 1; train_labels += 1
        else:
            val_images += 1;   val_labels += 1

    # data.yaml
    (out / "data.yaml").write_text(
        f"""path: {out}
train: images/train
val: images/val
nc: 1
names: [step_panel]
""",
        encoding="ascii",
    )

    print(f"NEW DATASET: {out}")
    print(f"train imgs:  {train_images}")
    print(f"train labels:{train_labels}")
    print(f"val imgs:    {val_images}")
    print(f"val labels:  {val_labels}")

    # Optional overlays (20 random val images)
    if args.overlays:
        ovr = abspath(args.overlays, repo)
        ensure_dir(ovr)
        from random import shuffle
        val_list = list((out/"images/val").glob("*.*"))
        shuffle(val_list)
        val_list = val_list[:20]
        made = 0
        for im_path in val_list:
            lbl_path = (out/"labels/val"/(im_path.stem + ".txt"))
            with Image.open(im_path) as im:
                if lbl_path.exists():
                    lines = [ln.strip() for ln in lbl_path.read_text(encoding="ascii").splitlines()]
                else:
                    lines = []
                dst_png = ovr / im_path.name
                write_overlay(dst_png, im, [ln for ln in lines if ln])
                made += 1
        print(f"Overlays -> {ovr}  (wrote {made})")

if __name__ == "__main__":
    sys.exit(main())
