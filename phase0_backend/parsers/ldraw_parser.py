import math
from pathlib import Path

LDU_TO_MM = 0.4

def _is_step(line: str) -> bool:
    return line.startswith("0 ") and "STEP" in line.upper()

def _is_placement(line: str) -> bool:
    return line.startswith("1 ")

def _tokenize(line: str):
    return line.strip().split()

def _euler_from_matrix(m):
    """
    Convert 3x3 rotation matrix to ZYX Euler angles (deg).
    Assumes proper rotation; best-effort for LDraw matrices.
    """
    r11, r12, r13 = m[0]
    r21, r22, r23 = m[1]
    r31, r32, r33 = m[2]
    # ZYX
    if abs(r31) < 1.0:
        y = math.asin(-r31)
        x = math.atan2(r32, r33)
        z = math.atan2(r21, r11)
    else:
        # Gimbal lock fallback
        y = math.pi/2 if r31 <= -1.0 else -math.pi/2
        x = 0.0
        z = math.atan2(-r12, r22)
    return [math.degrees(x), math.degrees(y), math.degrees(z)]

def parse_ldraw_steps(file_path: str, ldu_to_mm: float = LDU_TO_MM):
    """
    Parse an LDraw .ldr (or single-file .mpd without subfile blocks)
    into ordered steps and placements. Returns:
      {
        "steps": [ { "step_num": int, "placements": [ {...} ] }, ... ],
        "bom": [ { "subfile": str, "ldraw_color": int, "rb_part_num": None|str, "qty": int } ],
        "stats": { "placements": int, "steps": int }
      }
    """
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(file_path)

    steps = []
    current = {"step_num": 1, "placements": []}
    total_placements = 0

    with p.open("r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue

            if _is_step(line):
                # push previous step if it has any placements or is the first empty step
                if current["placements"] or (not steps and current["step_num"] == 1):
                    steps.append(current)
                current = {"step_num": len(steps) + 1, "placements": []}
                continue

            if _is_placement(line):
                # Format: 1 <col> a b c d e f g h i x y z subfile.dat [ .. ]
                tok = _tokenize(line)
                # guard: need at least 15 tokens (1 + color + 12 matrix/pos + subfile)
                if len(tok) < 15:
                    continue
                try:
                    color = int(tok[1])
                    a,b,c,d,e,f,g,h,i = map(float, tok[2:11])
                    x,y,z = map(float, tok[11:14])
                    subfile = tok[14]
                except Exception:
                    continue

                # Build rotation matrix and translation (mm)
                M = [[a,b,c],[d,e,f],[g,h,i]]
                pos_mm = [x*ldu_to_mm, y*ldu_to_mm, z*ldu_to_mm]
                rot_deg = _euler_from_matrix(M)

                placement = {
                    # rb_part_num can be injected later by resolver; keep placeholder
                    "rb_part_num": None,
                    "rb_color_id": None,         # will be resolved later if desired
                    "subfile": subfile,
                    "ldraw_color": color,
                    "qty": 1,
                    "transform": {
                        "position_mm": [round(pos_mm[0],3), round(pos_mm[1],3), round(pos_mm[2],3)],
                        "rotation_deg": [round(rot_deg[0],2), round(rot_deg[1],2), round(rot_deg[2],2)]
                    }
                }
                current["placements"].append(placement)
                total_placements += 1

    # append last step
    if current["placements"] or not steps:
        # ensure at least one step if placements exist with no explicit STEP
        if not steps and not current["placements"]:
            steps = []
        else:
            steps.append(current)

    # Build a simple BOM (by subfile+color when rb_part_num is not yet resolved)
    bom_counts = {}
    for st in steps:
        for pl in st["placements"]:
            key = (pl.get("rb_part_num"), pl["subfile"], pl["ldraw_color"])
            bom_counts[key] = bom_counts.get(key, 0) + pl.get("qty", 1)

    bom = []
    for (rb_num, subfile, ldraw_color), qty in bom_counts.items():
        bom.append({
            "rb_part_num": rb_num,
            "subfile": subfile,
            "ldraw_color": ldraw_color,
            "qty": qty
        })

    return {
        "steps": steps if steps else None,
        "bom": sorted(bom, key=lambda x: (-x["qty"], x["subfile"])),
        "stats": { "placements": total_placements, "steps": 0 if steps is None else len(steps) }
    }

def infer_ldraw_stem(filename: str) -> str:
    """Return lowercased stem without extension for mapping lookups."""
    return Path(filename).name.rsplit(".", 1)[0].lower()
