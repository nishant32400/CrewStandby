import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Paths (use only the 3 files)
pairings_path = r'C:\Users\Nishant.x.1\Downloads\Yuvraj_code\Sql_Maker\DEL Start Pilot Pairing July25-Sept25.csv'
hc_path = r'C:\Users\Nishant.x.1\Downloads\Yuvraj_code\Sql_Maker\Operating Pilot Headcount.csv'
sby_path = r'C:\Users\Nishant.x.1\Downloads\Yuvraj_code\Sql_Maker\Standby_activation_data_JulytoSept.csv'
out_path = r'C:\Users\Nishant.x.1\Downloads\Yuvraj_code\DEL_SBY_prepared_from_consolidated.csv'

# Date range (exact match with notebook)
startDate = "'2025-07-01'"
endDate = "'2025-09-30'"

# IGO subfleets (exact match with notebook)
IGO_ac_subfleets = ['323', '32D', '32H', '32M', '32P', '32S', '32V', 'AT7', 'ATR']

# Window functions (exact match with notebook)
def window(x):
    if x<4:
        return "0-4"
    if x<8:
        return "4-8"
    if x<12:
        return "8-12"
    if x<16:
        return "12-16"
    if x<20:
        return "16-20"
    return "20-24"

def window_num(x):
    if x<4:
        return 1
    if x<8:
        return 2
    if x<12:
        return 3
    if x<16:
        return 4
    if x<20:
        return 5
    return 6

print("Loading 3 source files...")
pairings_df = pd.read_csv(pairings_path)
hc_df = pd.read_csv(hc_path)
sby_df = pd.read_csv(sby_path)

print(f"Loaded - Pairings: {pairings_df.shape}, Headcount: {hc_df.shape}, Standby: {sby_df.shape}")

# ---------------------------
# Process roster/pairing data (exact match with notebook logic)
# ---------------------------
print("Processing roster data...")
roster_data = pairings_df.copy()

# Clean data (exact match with notebook)
for col in ['CWBASE','TripCode','DutyCode','DEP','ARR','FleetType','Subfleet','PairingStartDEP']:
    if col in roster_data.columns:
        roster_data[col] = roster_data[col].astype(str).str.strip()

# Parse STD column (exact match with notebook)
if 'STD' in roster_data.columns:
    roster_data['STD'] = pd.to_datetime(roster_data['STD'], format='mixed', errors='coerce')
else:
    print("Warning: STD column not found in pairings data")

# Add rank mapping (exact match with notebook)
if 'Pos' in roster_data.columns:
    roster_data['Rank'] = roster_data['Pos'].apply(lambda x: 'CP' if x==1 else ('FO' if x==2 else None))
elif 'Rank' in roster_data.columns:
    roster_data['Rank'] = roster_data['Rank'].apply(lambda x: 'CP' if 'CP' in str(x) else ('FO' if 'FO' in str(x) else None))

# Add duty windows (exact match with notebook)
if 'STD' in roster_data.columns:
    roster_data['Duty Hour of Day'] = roster_data['STD'].dt.hour
    roster_data['Duty Window'] = roster_data['Duty Hour of Day'].apply(window)
    roster_data['Duty Window Number'] = roster_data['Duty Hour of Day'].apply(window_num)

# Filter and process exactly like notebook
if all(col in roster_data.columns for col in ['DutyCode','Publact','Subfleet','FleetType']):
    roster_df_fdut_pairing_count = roster_data[
        (roster_data['DutyCode']=='FDUT') & 
        (roster_data['Publact']=='A') & 
        (roster_data['Subfleet'].isin(IGO_ac_subfleets)) & 
        (roster_data['FleetType'].isin(['320','321']))
    ].copy()
    
    if 'STD' in roster_df_fdut_pairing_count.columns:
        roster_df_fdut_pairing_count = roster_df_fdut_pairing_count.sort_values('STD', ascending=True)
    
    # Drop duplicates exactly like notebook
    roster_df_fdut_pairing_count = roster_df_fdut_pairing_count.drop_duplicates(['ID','PairingStartDate','TripCode'], keep='first')
    
    # Group exactly like notebook
    roster_df_fdut_pairing_count = roster_df_fdut_pairing_count.groupby([
        'PairingStartDate','PairingStartDEP','Duty Window','Duty Window Number','Rank'
    ], as_index=False).size().rename(columns={
        'PairingStartDate':'Date',
        'PairingStartDEP':'Station',
        'size':'Pairing Start Count'
    }).sort_values(['Date','Duty Window Number','Rank']).reset_index(drop=True)
    
    # Convert Date to date type for consistent merging
    roster_df_fdut_pairing_count['Date'] = pd.to_datetime(roster_df_fdut_pairing_count['Date']).dt.date
else:
    print("Warning: Required columns not found for pairing filtering")
    roster_df_fdut_pairing_count = pd.DataFrame()

print(f"Pairing counts shape: {roster_df_fdut_pairing_count.shape}")

# ---------------------------
# Process standby activation data (exact match with notebook logic)
# ---------------------------
print("Processing standby activation data...")
standby_activation_data = sby_df.copy()

# Clean duty codes (exact match with notebook)
for col in ['Old_duty_code','New_duty_code']:
    if col in standby_activation_data.columns:
        standby_activation_data[col] = standby_activation_data[col].astype(str).str.strip()

# Parse FDUT_time_IST (exact match with notebook)
standby_activation_data['FDUT_time_IST'] = pd.to_datetime(standby_activation_data['FDUT_time_IST'], errors='coerce')

# Add duty windows (exact match with notebook)
standby_activation_data['Duty Hour of Day'] = standby_activation_data['FDUT_time_IST'].dt.hour
standby_activation_data['Duty Window'] = standby_activation_data['Duty Hour of Day'].apply(window)
standby_activation_data['Duty Window Number'] = standby_activation_data['Duty Hour of Day'].apply(window_num)
standby_activation_data['Date'] = standby_activation_data['FDUT_time_IST'].dt.date

# Process headcount data (exact match with notebook)
headcount_data = hc_df.copy()
headcount_data['Rank2'] = headcount_data['Rank'].apply(lambda x: 'CP' if 'CP' in x else ('FO' if 'FO' in x else None))

# Merge standby with headcount (exact match with notebook)
standby_activation_count = standby_activation_data.merge(
    headcount_data[['IGA','CrewBase','Rank2']].rename(columns={
        'IGA':'CREW ID',
        'CrewBase':'Station',
        'Rank2':'Rank'
    }).drop_duplicates('CREW ID'),
    how='left',
    on='CREW ID'
)

# Group standby activations (exact match with notebook)
standby_activation_count = standby_activation_count.groupby([
    'Date','Station','Duty Window','Duty Window Number','Rank'
], as_index=False).size().rename(columns={
    'size':'Standby Activation Count'
}).sort_values(['Date','Duty Window Number','Rank']).reset_index(drop=True)

# Ensure Date column is date type for consistent merging
standby_activation_count['Date'] = pd.to_datetime(standby_activation_count['Date']).dt.date

print(f"Standby activation counts shape: {standby_activation_count.shape}")

# ---------------------------
# Final merge (exact match with notebook logic)
# ---------------------------
print("Creating final output...")

if not roster_df_fdut_pairing_count.empty and not standby_activation_count.empty:
    # Merge exactly like notebook
    final_df = roster_df_fdut_pairing_count.merge(
        standby_activation_count.drop('Duty Window Number', axis=1),
        how='left',
        on=['Date','Station','Duty Window','Rank']
    )
    final_df['Standby Activation Count'] = final_df['Standby Activation Count'].fillna(0).astype(int)
    
    # Apply date filter exactly like notebook
    start_date_obj = pd.to_datetime(startDate.strip("'")).date()
    end_date_obj = pd.to_datetime(endDate.strip("'")).date()
    # Ensure Date column is date type before filtering
    final_df['Date'] = pd.to_datetime(final_df['Date']).dt.date
    final_df = final_df[final_df['Date'].between(start_date_obj, end_date_obj)]
    
    # Sort exactly like notebook
    final_df = final_df.sort_values(['Date','Station','Duty Window','Rank']).reset_index(drop=True)
    
    # Save output
    final_df.to_csv(out_path, index=False)
    
    print(f"Saved: {out_path}")
    print(f"Final output shape: {final_df.shape}")
    print("\nSample output:")
    print(final_df.head(10))
    
    if len(final_df) == 1:
        print("\nWARNING: Only 1 row in output. Checking data...")
        print("Unique dates:", final_df['Date'].unique())
        print("Unique stations:", final_df['Station'].unique())
        print("Unique windows:", final_df['Duty Window'].unique())
        print("Unique ranks:", final_df['Rank'].unique())
else:
    print("ERROR: One or both dataframes are empty")
    print(f"Pairing data empty: {roster_df_fdut_pairing_count.empty}")
    print(f"Standby data empty: {standby_activation_count.empty}")