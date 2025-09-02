from __future__ import annotations
import json
from pathlib import Path
from typing import Tuple, Dict, List

# Directories to scan (some may not exist—handled gracefully).
LDRAW_DIRS: List[Path] = [
    Path("data/raw/ldraw/parts"),
    Path("data/raw/ldraw/parts/s"),
    Path("data/raw/ldraw/p"),
    Path("data/raw/ldraw/unofficial/parts"),
    Path("data/raw/ldraw/unofficial/parts/s"),
    Path("data/raw/ldraw/unofficial/p"),
]

OUT_DIR = Path("data/raw/ldraw/_index")
OUT_JSON = OUT_DIR / "ldraw_index.json"

def first_meta_lines(fp: Path, max_lines: int = 80) -> Tuple[str, str]:
    header_line0 = ""
    ldraw_org = ""
    try:
        with open(fp, "r", encoding="utf-8", errors="ignore") as f:
            for i, raw in enumerate(f):
                if i > max_lines:
                    break
                if not raw:
                    continue
                ch = raw[0]
                if ch == "0":
                    s = raw.strip()
                    if not header_line0:
                        header_line0 = s
                    if s.upper().startswith("0 !LDRAW_ORG") and not ldraw_org:
                        ldraw_org = s
                    continue
                if ch in "12345":
                    break
    except Exception:
        pass
    return header_line0, ldraw_org

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    index: Dict[str, dict] = {}
    scanned_files = 0

    for base in LDRAW_DIRS:
        if not base.exists():
            continue
        for fp in base.glob("*.dat"):
            scanned_files += 1
            stem = fp.stem.lower()
            # Later directories can override earlier ones (e.g., unofficial superseding official)
            stat = fp.stat()
            header_line0, ldraw_org = first_meta_lines(fp)
            index[stem] = {
                "path": fp.as_posix(),
                "file": fp.name,
                "header_line0": header_line0,
                "ldraw_org": ldraw_org,
                "size_bytes": stat.st_size,
                "modified_ts": int(stat.st_mtime),
            }

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2, sort_keys=True)

    print(f"Scanned {scanned_files} .dat files across {sum(1 for d in LDRAW_DIRS if d.exists())} directories.")
    print(f"Indexed {len(index)} unique stems into {OUT_JSON.as_posix()}")

if __name__ == "__main__":
    main()
