from __future__ import annotations
from pathlib import Path
from typing import List, Tuple, Dict
import json
import numpy as np

# LDraw units: 1 LDU = 0.4 mm
LDU_TO_MM = 0.4

# Where we find the index and the ldraw root
LDRAW_ROOT = Path("data/raw/ldraw")
LDRAW_INDEX_JSON = Path("data/raw/ldraw/_index/ldraw_index.json")

class LDrawIndex:
    def __init__(self, index_path: Path):
        with open(index_path, "r", encoding="utf-8") as f:
            self.idx: Dict[str, dict] = json.load(f)

    def resolve(self, name: str) -> str | None:
        """
        Resolve a subfile by name (case-insensitive). Name may include or omit '.dat'.
        Returns POSIX path string or None.
        """
        key = name.lower().removesuffix(".dat")
        meta = self.idx.get(key)
        return meta["path"] if meta else None

class LDrawExpander:
    """
    Minimal expander for LDraw .dat geometry:
    - Handles line types: 0 (ignored), 1 (subfile ref with 3x3 + translation), 3 (tri), 4 (quad→tris)
    - Ignores colors/BFC for now (geometry-only)
    """
    def __init__(self, ldraw_root: Path, index: LDrawIndex, max_depth: int = 64):
        self.root = ldraw_root
        self.index = index
        self.max_depth = max_depth

    def expand_to_triangles(self, top_path: str) -> np.ndarray:
        tris: List[np.ndarray] = []
        self._walk(top_path, np.eye(4, dtype=np.float64), tris, depth=0)
        if not tris:
            return np.zeros((0, 3, 3), dtype=np.float32)
        arr = np.stack(tris, axis=0).astype(np.float32)
        return arr

    def _walk(self, dat_path: str, T_parent: np.ndarray, out_tris: List[np.ndarray], depth: int):
        if depth > self.max_depth:
            return
        p = Path(dat_path)
        try:
            with open(p, "r", encoding="utf-8", errors="ignore") as f:
                for raw in f:
                    if not raw:
                        continue
                    code = raw[0]
                    if code == "0":
                        # comment/meta; ignore for now
                        continue
                    parts = raw.strip().split()
                    if not parts:
                        continue
                    if parts[0] == "1":
                        # 1 col a b c d e f g h i x y z subfile.dat
                        if len(parts) < 15:
                            continue
                        # matrix row-major in LDraw line type 1
                        a,b,c,d,e,f,g,h,i,x,y,z = map(float, parts[2:14])
                        sub = parts[14]
                        # Build 4x4 affine (rotation+translation)
                        T = np.array([[a, d, g, x],
                                        [b, e, h, y],
                                        [c, f, i, z],
                                        [0, 0, 0, 1]], dtype=np.float64)
                        sub_path = self.index.resolve(sub)
                        if sub_path:
                            self._walk(sub_path, T_parent @ T, out_tris, depth+1)
                        # If unresolved, silently skip
                    elif parts[0] == "3":
                        # 3 col x1 y1 z1 x2 y2 z2 x3 y3 z3
                        if len(parts) < 11:
                            continue
                        coords = np.array(list(map(float, parts[2:11])), dtype=np.float64).reshape(3,3)
                        pts_h = np.c_[coords, np.ones((3,1))]
                        pts_w = (T_parent @ pts_h.T).T[:, :3]
                        out_tris.append(pts_w.astype(np.float64))
                    elif parts[0] == "4":
                        # 4 col x1 y1 z1 x2 y2 z2 x3 y3 z3 x4 y4 z4 -> split into two tris
                        if len(parts) < 14:
                            continue
                        coords = np.array(list(map(float, parts[2:14])), dtype=np.float64).reshape(4,3)
                        pts_h = np.c_[coords, np.ones((4,1))]
                        pts_w = (T_parent @ pts_h.T).T[:, :3]
                        out_tris.append(pts_w[[0,1,2]].astype(np.float64))
                        out_tris.append(pts_w[[0,2,3]].astype(np.float64))
                    else:
                        # Lines 2,5 not handled (edges/conditional lines) for mesh; safe to ignore
                        continue
        except FileNotFoundError:
            return
        except Exception:
            # Do not crash expansion for a single bad subfile; skip silently
            return

def triangle_bounds(tris_ldu: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    if tris_ldu.size == 0:
        return np.zeros(3), np.zeros(3)
    pts = tris_ldu.reshape(-1, 3)
    mn = pts.min(0)
    mx = pts.max(0)
    return mn, mx



