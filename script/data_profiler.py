import pandas as pd
from typing import Dict


def profile_table(df: pd.DataFrame, table_name: str = "") -> pd.DataFrame:
    """
    Profiles a given DataFrame and returns a new DataFrame with statistics about the table.

    Parameters:
        df (pd.DataFrame): DataFrame to profile
        table_name (str, optional): Name of the table to use in the profile. Defaults to "".

    Returns:
        pd.DataFrame: DataFrame with statistics about the table
    """
    if df.empty:
        return pd.DataFrame()  # Return empty if no data
    
    profile_data = []
    total_rows = len(df)
    
    for col in df.columns:
        null_count = df[col].isnull().sum()
        distinct_count = df[col].nunique(dropna=True)
        
        row = {
            "table_name": table_name or "unknown",
            "column_name": col,
            "data_type": str(df[col].dtype),
            "row_count": total_rows,
            "null_count": null_count,
            "null_%": round(null_count / total_rows * 100, 4) if total_rows else 0,
            "distinct_count": distinct_count,
            "distinct_%": round(distinct_count / total_rows * 100, 4) if total_rows else 0,
        }
        
        # Top value (only if not too many unique values)
        if distinct_count > 0 and distinct_count < total_rows:
            try:
                top_val = df[col].value_counts(dropna=True).index[0]
                top_freq = df[col].value_counts(dropna=True).iloc[0]
                row["top_value"] = top_val
                row["top_value_freq"] = top_freq
            except Exception:
                row["top_value"] = None
                row["top_value_freq"] = 0
        
        # Min/Max for numeric columns
        if pd.api.types.is_numeric_dtype(df[col]):
            row["min_value"] = df[col].min()
            row["max_value"] = df[col].max()
        
        profile_data.append(row)
    
    return pd.DataFrame(profile_data)


def profile_all_tables(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Takes a dict of {table_name: DataFrame} → returns {table_name: profile_df}
    """
    if not data_dict:
        print("No DataFrames to profile.")
        return {}
    
    profiles = {}
    for table_name, df in data_dict.items():
        print(f"Profiling table: {table_name} ({len(df):,} rows)")
        try:
            profile_df = profile_table(df, table_name=table_name)
            profiles[table_name] = profile_df
            print(f"Done: {table_name} → {len(profile_df)} columns profiled")
        except Exception as e:
            print(f"Failed to profile {table_name}: {e}")
            profiles[table_name] = pd.DataFrame()  # empty placeholder
    
    return profiles