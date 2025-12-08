from pathlib import Path
import pandas as pd
from typing import Dict, Optional


def data_loader(source_path: str | Path) -> Optional[Dict[str, pd.DataFrame]]:
    """
    Load all CSV files from a directory into a dictionary of DataFrames.
    
    Args:
        source_path: Path to directory containing CSV files
    
    Returns:
        Dict mapping filename (without .csv) → DataFrame, or None if invalid
    """
    source_path = Path(source_path)
    
    # Check if path exists first
    if not source_path.exists():
        print(f"Error: Path does not exist: {source_path}")
        return None
    
    if not source_path.is_dir():
        print(f"Error: Provided path is not a directory: {source_path}")
        return None
    
    csv_files = list(sorted(source_path.glob("*.csv")))
    
    if not csv_files:
        print(f"Warning: No CSV files found in {source_path}")
        return {}  # Return empty dict
    
    data_frames = {}
    for file in csv_files:
        try:
            # Use stem (filename without extension) as key
            df = pd.read_csv(file)
            data_frames[file.stem] = df
            print(f"Loaded {file.name} → {len(df):,} rows, {len(df.columns)} columns")
        except Exception as e:
            print(f"Failed to load {file.name}: {e}")
            # Continue loading others instead of crashing
    
    return data_frames
