import pandas as pd

def process_and_validate_referrals(cleaned_data: dict) -> pd.DataFrame:
    """
    Process and validate referral data from cleaned data.

    This function takes a dictionary of cleaned data from different tables and merges them based on common keys.
    It then applies various business logic rules to validate the referrals.

    The function returns a DataFrame with the final report of valid and invalid referrals.

    Parameters:
        cleaned_data (dict): Dictionary of cleaned data from different tables.

    Returns:
        pd.DataFrame: DataFrame with the final report of valid and invalid referrals.

    Raises:
        ValueError: If the user_referrals table is missing or empty.
    """
    referrals = cleaned_data.get('user_referrals', pd.DataFrame()).copy()
    referral_logs = cleaned_data.get('user_referral_logs', pd.DataFrame())
    rewards = cleaned_data.get('referral_rewards', pd.DataFrame())
    transactions = cleaned_data.get('paid_transactions', pd.DataFrame())
    users = cleaned_data.get('user_logs', pd.DataFrame())
    statuses = cleaned_data.get('user_referral_statuses', pd.DataFrame())
    leads = cleaned_data.get('lead_log', pd.DataFrame())  # filename is lead_log.csv

    if referrals.empty:
        raise ValueError("user_referrals table is missing or empty")

    print(f"Starting with {len(referrals)} referrals")

    # Convert key types for consistent merging
    if 'referral_reward_id' in referrals.columns:
        referrals['referral_reward_id'] = referrals['referral_reward_id'].astype(str)
    if not rewards.empty:
        rewards['id'] = rewards['id'].astype(str)
    if 'user_referral_status_id' in referrals.columns:
        referrals['user_referral_status_id'] = referrals['user_referral_status_id'].astype(str)
    if not statuses.empty:
        statuses['id'] = statuses['id'].astype(str)

    # 1. Status join
    if not statuses.empty:
        referrals = referrals.merge(statuses[['id', 'description']],
                                    left_on='user_referral_status_id', right_on='id', how='left')
        referrals.rename(columns={'description': 'referral_status'}, inplace=True)

    # 2. Reward join
    if not rewards.empty:
        referrals = referrals.merge(rewards[['id', 'reward_value']],
                                    left_on='referral_reward_id', right_on='id', how='left')

    # 3. Transaction join
    if not transactions.empty:
        trans_cols = ['transaction_id', 'transaction_status', 'transaction_at',
                      'timezone_transaction', 'transaction_type']
        referrals = referrals.merge(transactions[trans_cols], on='transaction_id', how='left')

    # 4. Referrer info
    if not users.empty:
        referrer_df = users[['user_id', 'membership_expired_date', 'is_deleted', 'timezone_homeclub']] \
            .drop_duplicates() \
            .rename(columns={
                'user_id': 'referrer_id',
                'membership_expired_date': 'referrer_membership_expired',
                'is_deleted': 'referrer_is_deleted',
                'timezone_homeclub': 'referrer_timezone'
            })
        referrals = referrals.merge(referrer_df, on='referrer_id', how='left')
    # Handle NaN in referrer fields conservatively
    referrals['referrer_is_deleted'] = referrals['referrer_is_deleted'].fillna(True).astype(bool)  # Assume deleted if unknown (strict)
    referrals['referrer_membership_expired'] = pd.to_datetime(referrals['referrer_membership_expired'], errors='coerce').fillna(pd.Timestamp('1900-01-01'))  # Assume expired if unknown

    # 5. source_transaction_id from logs
    if not referral_logs.empty:
        logs_src = referral_logs[['user_referral_id', 'source_transaction_id']].drop_duplicates()
        referrals = referrals.merge(logs_src, left_on='referral_id', right_on='user_referral_id', how='left')
        referrals.drop(columns=['user_referral_id'], errors='ignore', inplace=True)

    # 6. Reward granted
    if not referral_logs.empty:
        print("Granted counts:", referral_logs['is_reward_granted'].value_counts())
        granted = referral_logs[referral_logs['is_reward_granted'] == True][['user_referral_id']].drop_duplicates()
        granted['referee_reward_granted'] = True
        referrals = referrals.merge(granted, left_on='referral_id', right_on='user_referral_id', how='left')
        referrals.drop(columns=['user_referral_id'], errors='ignore', inplace=True)
        referrals['referee_reward_granted'] = referrals['referee_reward_granted'].fillna(False).astype(bool)

    # 7. Timezone selection (priority: transaction > lead > referrer > default)
    def get_timezone(row):
        if pd.notna(row.get('timezone_transaction')):
            return row['timezone_transaction']
        if row['referral_source'] == 'Lead' and pd.notna(row.get('source_transaction_id')) and not leads.empty:
            lead_row = leads[leads['lead_id'] == row['source_transaction_id']]
            if not lead_row.empty:
                return lead_row['timezone_location'].iloc[0]
        if pd.notna(row.get('referrer_timezone')):
            return row['referrer_timezone']
        return 'Asia/Jakarta'  # safe default

    referrals['effective_timezone'] = referrals.apply(get_timezone, axis=1)

    # Ensure timestamps are timezone-aware UTC first
    referrals['referral_at'] = pd.to_datetime(referrals['referral_at'], utc=True, errors='coerce')
    referrals['transaction_at'] = pd.to_datetime(referrals['transaction_at'], utc=True, errors='coerce')

    # Convert to local time safely AND MAKE NAIVE
    def convert_to_local(ts, tz):
        if pd.isna(ts) or pd.isna(tz):
            return pd.NaT
        try:
            return ts.tz_convert(tz).tz_localize(None)  # Convert and remove tz to make naive
        except Exception:
            return pd.NaT

    referrals['local_referral_at'] = referrals.apply(
        lambda row: convert_to_local(row['referral_at'], row['effective_timezone']), axis=1
    )

    referrals['local_transaction_at'] = referrals['transaction_at'].dt.tz_localize(None)  # Start naive
    mask_tx = referrals['timezone_transaction'].notna()
    if mask_tx.any():
        converted = referrals.loc[mask_tx].apply(
            lambda row: convert_to_local(row['transaction_at'], row['timezone_transaction']), axis=1
        )
        referrals.loc[mask_tx, 'local_transaction_at'] = converted

    # Enforce dtype (in case of all NaT or issues)
    referrals['local_referral_at'] = pd.to_datetime(referrals['local_referral_at'], errors='coerce')
    referrals['local_transaction_at'] = pd.to_datetime(referrals['local_transaction_at'], errors='coerce')

    # 8. String initcap
    for col in ['referee_name', 'referee_phone']:
        if col in referrals.columns:
            referrals[col] = referrals[col].astype(str).str.title()

    # 9. referral_source_category
    def get_source_category(row):
        source = row['referral_source']
        if source == 'User Sign Up':
            return 'Online'
        if source == 'Draft Transaction':
            return 'Offline'
        if source == 'Lead' and pd.notna(row.get('source_transaction_id')) and not leads.empty:
            lead = leads[leads['lead_id'] == row['source_transaction_id']]
            if not lead.empty:
                return lead['source_category'].iloc[0]
        return None

    referrals['referral_source_category'] = referrals.apply(get_source_category, axis=1)

    # 10. Business Logic - safe month extraction
    mask_ref = pd.notna(referrals['local_referral_at'])
    referrals.loc[mask_ref, 'referral_month'] = referrals.loc[mask_ref, 'local_referral_at'].dt.to_period('M')
    
    mask_tx_month = pd.notna(referrals['local_transaction_at'])
    referrals.loc[mask_tx_month, 'transaction_month'] = referrals.loc[mask_tx_month, 'local_transaction_at'].dt.to_period('M')

    current_date = pd.Timestamp('2025-12-09')

    # Reward handling (NaN/0 as invalid)
    referrals['reward_value'] = referrals['reward_value'].fillna(0)
    reward_valid = referrals['reward_value'] > 0
    reward_invalid = ~reward_valid

    valid_success = (
        reward_valid &
        (referrals['referral_status'] == 'Berhasil') &
        referrals['transaction_id'].notna() &
        (referrals['transaction_status'] == 'PAID') &
        (referrals['transaction_type'] == 'NEW') &
        (referrals['local_transaction_at'] > referrals['local_referral_at']) &
        (referrals['referral_month'] == referrals['transaction_month']) &
        (referrals['referrer_membership_expired'] >= current_date) &
        (~referrals['referrer_is_deleted']) &
        referrals['referee_reward_granted']
    )

    valid_pending_failed = (
        referrals['referral_status'].isin(['Menunggu', 'Tidak Berhasil']) &
        reward_invalid
    )

    conditions_valid = valid_success | valid_pending_failed

    conditions_invalid = (
        (reward_valid & (referrals['referral_status'] != 'Berhasil')) |
        (reward_valid & referrals['transaction_id'].isna()) |
        (reward_invalid & referrals['transaction_id'].notna() &
         (referrals['transaction_status'] == 'PAID') &
         (referrals['local_transaction_at'] > referrals['local_referral_at'])) |
        ((referrals['referral_status'] == 'Berhasil') & reward_invalid) |
        (referrals['local_transaction_at'] < referrals['local_referral_at'])
    )

    referrals['is_business_logic_valid'] = conditions_valid & ~conditions_invalid
    referrals['is_business_logic_valid'] = referrals['is_business_logic_valid'].fillna(False)

    # Final report
    final_cols = [
        'referral_id', 'referrer_id', 'referee_id', 'referral_at', 'referral_status',
        'transaction_id', 'transaction_status', 'transaction_type', 'reward_value',
        'referee_reward_granted', 'referrer_membership_expired', 'referrer_is_deleted',
        'referral_source_category', 'is_business_logic_valid'
    ]
    report = referrals[final_cols].copy()

    print(f"Final report has {len(report)} rows (should be 46)")
    print(f"Valid referrals: {report['is_business_logic_valid'].sum()}")
    print(f"Invalid referrals: {(~report['is_business_logic_valid']).sum()}")

    return report