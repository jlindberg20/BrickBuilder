import pandas as pd
from pathlib import Path

def load_rebrickable_csvs(raw_dir: str):
    """
    Load core Rebrickable CSVs into DataFrames.
    """
    raw_path = Path(raw_dir)
    parts = pd.read_csv(raw_path / "parts.csv")
    colors = pd.read_csv(raw_path / "colors.csv")
    elements = pd.read_csv(raw_path / "elements.csv")
    categories = pd.read_csv(raw_path / "part_categories.csv")
    relationships = pd.read_csv(raw_path / "part_relationships.csv")

    return {
        "parts": parts,
        "colors": colors,
        "elements": elements,
        "categories": categories,
        "relationships": relationships,
    }
