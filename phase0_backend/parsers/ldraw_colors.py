import re
from pathlib import Path

def load_ldraw_colors(ldraw_root: str):
    """
    Return dict { ldraw_color_code:int -> {"name": str, "hex": "#RRGGBB"} }.
    Looks for LDConfig.ldr (case-insensitive) in the given root.
    """
    root = Path(ldraw_root)
    candidates = [root / "LDConfig.ldr", root / "ldconfig.ldr"]
    path = next((p for p in candidates if p.exists()), None)
    if path is None:
        return {}

    pat = re.compile(r"^0\s+!COLOUR\s+(?P<name>.+?)\s+CODE\s+(?P<code>\d+)\s+VALUE\s+(?P<hex>#[0-9A-Fa-f]{6})")
    colors = {}
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            m = pat.match(line)
            if m:
                code = int(m.group("code"))
                colors[code] = {"name": m.group("name"), "hex": m.group("hex")}
    return colors
