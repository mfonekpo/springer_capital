import pandas as pd
from typing import Dict


def clean_all_tables(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Clean all DataFrames in the given dictionary.

    This function takes a dictionary of table names to DataFrames and returns a new dictionary with cleaned DataFrames.
    It performs the following operations on each DataFrame:

    1. Standardize column names by stripping whitespace, converting to lowercase, and replacing spaces with underscores.
    2. Convert datetime columns to datetime type with UTC timezone.
    3. Fix reward value column by extracting numeric values from strings.
    4. Convert boolean columns to boolean type.
    5. Drop duplicates for all tables.
    6. Drop rows with missing critical keys for referrals table.
    7. Drop incomplete rows for other tables.

    Parameters:
        data_dict (dict): Dictionary of table names to DataFrames

    Returns:
        dict: Dictionary of table names to cleaned DataFrames
    """
    cleaned = {}
    
    for name, df in data_dict.items():
        if df.empty:
            cleaned[name] = df
            continue
            
        print(f"Cleaning table: {name} ({df.shape[0]:,} rows)")
        df = df.copy()
        
        # Standardize columns
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
        
        # Datetime conversion
        date_cols = ["created_at", "transaction_at", "referral_at", "updated_at", "membership_expired_date"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
                if col == "membership_expired_date":
                    df[col] = df[col].dt.tz_localize(None)
        
        # Reward value fix
        if "reward_value" in df.columns:
            df["reward_value"] = df["reward_value"].astype(str).str.extract(r"(\d+)").astype("float")
            df["reward_value"] = df["reward_value"].astype("Int64")
        
        # Booleans
        bool_map = {"True": True, "False": False, "true": True, "false": False}
        if "is_deleted" in df.columns:
            df["is_deleted"] = df["is_deleted"].map(bool_map).fillna(False).astype(bool)
        if "is_reward_granted" in df.columns:
            df["is_reward_granted"] = df["is_reward_granted"].astype(str).str.strip()
            df["is_reward_granted"] = df["is_reward_granted"].map(bool_map).fillna(False).astype(bool)
        
        # Drop duplicates
        df = df.drop_duplicates()
        
        # NULL HANDLING (no aggressive drop for referrals)
        if name == 'user_referrals':
            # Only drop if critical keys are null (keep expected nulls like transaction_id/reward_id)
            critical = ['referral_id', 'referral_at', 'referral_source', 'user_referral_status_id']
            before = len(df)
            df = df.dropna(subset=[c for c in critical if c in df.columns])
            print(f"   → Dropped {before - len(df):,} rows with missing critical keys only")
        elif name in ['referral_rewards', 'paid_transactions', 'user_logs', 'user_referral_statuses']:
            # Stricter for these: drop any full null rows
            before = len(df)
            df = df.dropna(how='any')
            print(f"   → Dropped {before - len(df):,} incomplete rows")
        else:
            # Logs: keep most
            df = df.dropna(subset=df.columns[:2], how='all')  # drop only if totally empty
            
        cleaned[name] = df
        print(f"   → Cleaned! Final shape: {df.shape} | Total nulls: {df.isnull().sum().sum()}")
    
    return cleaned
