
import json, shutil, random
from pathlib import Path
from PIL import Image, ImageDraw

# === CONFIG ===
REPO_ROOT = Path.cwd()
LIST_PATH = REPO_ROOT / "data/processed/panels/page_annot_sample.list"
ANN_JSONL = REPO_ROOT / "data/processed/panels/annotations.step_panels.FILTERED.jsonl"
OUT_DIR   = REPO_ROOT / "data/processed/panels/yolo_pages_UNIQUE"
OVERLAYS  = REPO_ROOT / "data/reviews/yolo_pages_UNIQUE_val_overlays"
TRAIN_SPLIT = 0.9  # 90/10 split


def read_list(path: Path):
    return [p.strip() for p in path.read_text(encoding="utf-8-sig").splitlines() if p.strip()]


def abs_path(p: str) -> Path:
    p = Path(p)
    return p if p.is_absolute() else (REPO_ROOT / p).resolve()


def load_jsonl(path: Path):
    ann = {}
    if not path.exists():
        print(f"[warn] JSONL not found: {path}")
        return ann
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            src = obj.get("source_path")
            if not src:
                continue
            src_path = abs_path(src)
            ann.setdefault(src_path, []).append(obj)
        except Exception:
            continue
    return ann


def make_unique_name(src: Path):
    pdf_id = src.parent.name
    return f"{pdf_id}__{src.name}"


def write_yolo_label(rows, w, h, path):
    if not rows:
        path.write_text("", encoding="ascii")
        return
    lines = []
    for r in rows:
        x0, y0, x1, y1 = [float(r.get(k, 0)) for k in ("x0", "y0", "x1", "y1")]
        cx, cy = (x0 + x1) / 2 / w, (y0 + y1) / 2 / h
        bw, bh = abs(x1 - x0) / w, abs(y1 - y0) / h
        if bw > 0 and bh > 0:
            lines.append(f"0 {cx:.6f} {cy:.6f} {bw:.6f} {bh:.6f}")
    path.write_text("\n".join(lines) + "\n", encoding="ascii")


def draw_overlay(img_path: Path, lbl_path: Path, dst_path: Path):
    img = Image.open(img_path).convert("RGB")
    W, H = img.size
    draw = ImageDraw.Draw(img)
    if lbl_path.exists():
        for line in lbl_path.read_text().splitlines():
            if not line.strip():
                continue
            parts = line.split()
            if len(parts) < 5:
                continue
            cx, cy, bw, bh = [float(v) for v in parts[1:5]]
            x0, y0 = (cx - bw / 2) * W, (cy - bh / 2) * H
            x1, y1 = (cx + bw / 2) * W, (cy + bh / 2) * H
            draw.rectangle([x0, y0, x1, y1], outline="lime", width=max(2, W // 600))
    img.save(dst_path, "PNG")


def main():
    pages = [abs_path(p) for p in read_list(LIST_PATH) if abs_path(p).exists()]
    annotations = load_jsonl(ANN_JSONL)

    # prep dirs
    for d in [OUT_DIR / "images/train", OUT_DIR / "images/val",
              OUT_DIR / "labels/train", OUT_DIR / "labels/val"]:
        d.mkdir(parents=True, exist_ok=True)

    train_count = val_count = 0
    pos_count = neg_count = 0

    for i, src in enumerate(pages, 1):
        split = "train" if random.random() < TRAIN_SPLIT else "val"
        img_dst = OUT_DIR / f"images/{split}/{make_unique_name(src)}"
        lbl_dst = OUT_DIR / f"labels/{split}/{make_unique_name(src)}".replace(".png", ".txt")

        shutil.copy2(src, img_dst)

        rows = annotations.get(src, [])
        if rows:
            w, h = rows[0].get("img_w"), rows[0].get("img_h")
            if not (w and h):
                with Image.open(src) as im:
                    w, h = im.size
            write_yolo_label(rows, w, h, lbl_dst)
            pos_count += 1
        else:
            lbl_dst.write_text("", encoding="ascii")
            neg_count += 1

        if split == "train":
            train_count += 1
        else:
            val_count += 1

    # write data.yaml
    yaml = f"""path: {OUT_DIR.resolve()}
train: images/train
val: images/val
nc: 1
names: [step_panel]
"""
    (OUT_DIR / "data.yaml").write_text(yaml, encoding="ascii")

    print(f"\n✅ NEW DATASET: {OUT_DIR}")
    print(f"train imgs/labels: {train_count}")
    print(f"val imgs/labels:   {val_count}")
    print(f"positives: {pos_count}   negatives: {neg_count}")

    # overlays
    OVERLAYS.mkdir(parents=True, exist_ok=True)
    val_imgs = list((OUT_DIR / "images/val").glob("*.png"))
    random.shuffle(val_imgs)
    for img_path in val_imgs[:20]:
        lbl_path = OUT_DIR / "labels/val" / (img_path.stem + ".txt")
        draw_overlay(img_path, lbl_path, OVERLAYS / img_path.name)
    print(f"Overlays -> {OVERLAYS}  (wrote {len(list(OVERLAYS.glob('*.png')))})")


if __name__ == "__main__":
    main()
