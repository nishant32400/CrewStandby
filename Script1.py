
import pandas as pd
import numpy as np
from datetime import datetime

# Paths (CSV versions)
pairings_path = r'c:\Users\Nishant.x.1\Downloads\Yuvraj_code\TextFiles\DELStartPilotPairingJuly25Sept25.csv'
hc_path       = r'c:\Users\Nishant.x.1\Downloads\Yuvraj_code\TextFiles\OperatingPilotHeadcount.csv'
sby_path      = r'c:\Users\Nishant.x.1\Downloads\Yuvraj_code\TextFiles\StandbyActivationDataJulytoSept.csv'
out_path      = r'c:\Users\Nishant.x.1\Downloads\Yuvraj_code\TextFiles\DEL_SBY_prepared_from_consolidated.csv'

# Date range
startDate = "'2025-07-01'"
endDate   = "'2025-09-30'"

# IGO subfleets (same as notebook)
IGO_ac_subfleets = ['323', '32D', '32H', '32M', '32P', '32S', '32V', 'AT7', 'ATR']

def window(x):
    if x < 4:  return "0-4"
    if x < 8:  return "4-8"
    if x < 12: return "8-12"
    if x < 16: return "12-16"
    if x < 20: return "16-20"
    return "20-24"

def window_num(x):
    if x < 4:  return 1
    if x < 8:  return 2
    if x < 12: return 3
    if x < 16: return 4
    if x < 20: return 5
    return 6

def safe_strip(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

def parse_hhmm_on_date(hhmm_str, date_scalar):
    # Accepts "HH:MM", "HHMM", or with trailing ".0"; invalid returns NaT
    try:
        if pd.isna(date_scalar):
            return pd.NaT
        s = str(hhmm_str).strip()
        if not s or s.lower() == 'nan':
            return pd.NaT
        # drop fractional part like ".0"
        s = s.split('.')[0]
        # normalize "HHMM" -> "HH:MM"
        if ':' not in s and s.isdigit() and len(s) in (3,4):
            # e.g. "930" -> "09:30", "1700" -> "17:00"
            s = s.zfill(4)
            s = f"{s[:2]}:{s[2:]}"
        if ':' not in s:
            return pd.NaT
        d = pd.to_datetime(date_scalar, errors='coerce')
        if pd.isna(d):
            return pd.NaT
        d = d.date()
        return pd.to_datetime(f"{d} {s}", errors='coerce')
    except Exception:
        return pd.NaT

def derive_std_datetime(row):
    # 1) Try STD as full datetime
    std_raw = row.get('STD', None)
    std_dt = pd.to_datetime(std_raw, errors='coerce')
    if pd.notna(std_dt):
        return std_dt
    # 2) Try HH:MM on DutyDay
    duty_day = pd.to_datetime(row.get('DutyDay', pd.NaT), errors='coerce')
    if pd.notna(duty_day):
        std_dt2 = parse_hhmm_on_date(std_raw, duty_day)
        if pd.notna(std_dt2):
            return std_dt2
        # 3) Fallback to Reporting (HH:MM) on DutyDay
        rep_raw = row.get('Reporting', None)
        rep_dt = parse_hhmm_on_date(rep_raw, duty_day)
        if pd.notna(rep_dt):
            return rep_dt
    return pd.NaT

print("Loading source files...")
# Pairings: auto-detect delimiter (CSV or TXT)
roster_data = pd.read_csv(pairings_path, dtype=str, sep=None, engine='python')
if roster_data.shape[1] == 1:
    # fallback to comma, then tab
    try:
        roster_data = pd.read_csv(pairings_path, dtype=str, sep=',')
    except Exception:
        pass
if roster_data.shape[1] == 1:
    roster_data = pd.read_csv(pairings_path, dtype=str, sep='\t')

# Headcount: comma-delimited
headcount_data = pd.read_csv(hc_path, dtype=str)

# Standby activation: read and drop any unnamed index column
sby_df = pd.read_csv(sby_path, dtype=str)
unnamed_cols = [c for c in sby_df.columns if str(c).lower().startswith('unnamed')]
if unnamed_cols:
    sby_df = sby_df.drop(columns=unnamed_cols)

# Normalize roster headers (strip and alias by lower-case)
roster_data.columns = roster_data.columns.str.strip()
alias_map = {
    'publact':'Publact','id':'ID','cwbase':'CWBASE','pos':'Pos',
    'pairingstartdate':'PairingStartDate','dutyday':'DutyDay','tripcode':'TripCode',
    'dutycode':'DutyCode','std':'STD','reporting':'Reporting',
    'fleettype':'FleetType','subfleet':'Subfleet','pairingstartdep':'PairingStartDEP',
    'dep':'DEP','arr':'ARR'
}
roster_data = roster_data.rename(columns={c: alias_map.get(c.lower(), c) for c in roster_data.columns})

print(f"Loaded - Pairings: {roster_data.shape}, Headcount: {headcount_data.shape}, Standby: {sby_df.shape}")

# ---------------------------
# Validate required roster columns; fallback Publact='A' if absent
# ---------------------------
required_cols = [
    'Publact','ID','Pos','PairingStartDate','DutyDay','TripCode','DutyCode',
    'STD','Reporting','FleetType','Subfleet','PairingStartDEP'
]
missing = [c for c in required_cols if c not in roster_data.columns]
if missing:
    if 'Publact' in missing:
        # Assume roster dump is Active; create Publact
        roster_data['Publact'] = 'A'
        missing.remove('Publact')
    if missing:
        raise ValueError(f"Pairings file missing columns: {missing}. Fix headers or update alias_map.")

# ---------------------------
# Roster / Pairing processing
# ---------------------------
roster_data = safe_strip(roster_data, ['Publact','CWBASE','TripCode','DutyCode','DEP','ARR','FleetType','Subfleet','PairingStartDEP'])

# Parse dates needed
for c in ['PairingStartDate', 'DutyDay']:
    roster_data[c] = pd.to_datetime(roster_data[c], errors='coerce')

# Derive STD robustly
roster_data['STD_dt'] = roster_data.apply(derive_std_datetime, axis=1)

# Rank from Pos
roster_data['Pos'] = pd.to_numeric(roster_data['Pos'], errors='coerce')
roster_data['Rank'] = roster_data['Pos'].map({1: 'CP', 2: 'FO'})

# Duty window
roster_data['Duty Hour of Day'] = roster_data['STD_dt'].dt.hour
roster_data['Duty Window'] = roster_data['Duty Hour of Day'].apply(lambda h: window(h) if pd.notna(h) else np.nan)
roster_data['Duty Window Number'] = roster_data['Duty Hour of Day'].apply(lambda h: window_num(h) if pd.notna(h) else np.nan)

# Filters matching notebook + DEL-only
pairing_filt = roster_data[
    (roster_data['DutyCode'] == 'FDUT') &
    (roster_data['Publact'] == 'A') &
    (roster_data['Subfleet'].isin(IGO_ac_subfleets)) &
    (roster_data['FleetType'].isin(['320','321'])) &
    (roster_data['PairingStartDEP'] == 'DEL') &
    (roster_data['STD_dt'].notna())
].copy()

# Earliest STD per (ID, PairingStartDate, TripCode)
pairing_filt = pairing_filt.sort_values('STD_dt', ascending=True)
pairing_earliest = pairing_filt.drop_duplicates(['ID','PairingStartDate','TripCode'], keep='first')

# Group to pairing start count
roster_df_fdut_pairing_count = (
    pairing_earliest
    .groupby(['PairingStartDate','PairingStartDEP','Duty Window','Duty Window Number','Rank'], as_index=False)
    .size()
    .rename(columns={'PairingStartDate':'Date','PairingStartDEP':'Station','size':'Pairing Start Count'})
    .sort_values(['Date','Duty Window Number','Rank'])
    .reset_index(drop=True)
)
roster_df_fdut_pairing_count['Date'] = pd.to_datetime(roster_df_fdut_pairing_count['Date']).dt.date

print(f"Pairing counts shape: {roster_df_fdut_pairing_count.shape}")

# ---------------------------
# Standby activation processing
# ---------------------------
standby_activation_data = sby_df.copy()
standby_activation_data = safe_strip(standby_activation_data, ['Old_duty_code','New_duty_code'])

# Parse FDUT_time_IST and derive window/date
standby_activation_data['FDUT_time_IST'] = pd.to_datetime(standby_activation_data['FDUT_time_IST'], errors='coerce')
standby_activation_data['Duty Hour of Day'] = standby_activation_data['FDUT_time_IST'].dt.hour
standby_activation_data['Duty Window'] = standby_activation_data['Duty Hour of Day'].apply(lambda h: window(h) if pd.notna(h) else np.nan)
standby_activation_data['Duty Window Number'] = standby_activation_data['Duty Hour of Day'].apply(lambda h: window_num(h) if pd.notna(h) else np.nan)
standby_activation_data['Date'] = standby_activation_data['FDUT_time_IST'].dt.date

# Headcount station and rank
headcount_data = safe_strip(headcount_data, ['IGA','Rank','CrewBase'])
headcount_data['Rank2'] = headcount_data['Rank'].apply(lambda x: 'CP' if pd.notna(x) and 'CP' in x else ('FO' if pd.notna(x) and 'FO' in x else None))

standby_activation_count = standby_activation_data.merge(
    headcount_data[['IGA','CrewBase','Rank2']].rename(columns={'IGA':'CREW ID','CrewBase':'Station','Rank2':'Rank'}).drop_duplicates('CREW ID'),
    how='left',
    on='CREW ID'
)

# DEL-only for standby as well
standby_activation_count = standby_activation_count[standby_activation_count['Station'] == 'DEL'].copy()

standby_activation_count = (
    standby_activation_count
    .groupby(['Date','Station','Duty Window','Duty Window Number','Rank'], as_index=False)
    .size()
    .rename(columns={'size':'Standby Activation Count'})
    .sort_values(['Date','Duty Window Number','Rank'])
    .reset_index(drop=True)
)
standby_activation_count['Date'] = pd.to_datetime(standby_activation_count['Date']).dt.date

print(f"Standby activation counts shape: {standby_activation_count.shape}")

# ---------------------------
# Final merge (same keys as notebook)
# ---------------------------
start_date_obj = pd.to_datetime(startDate.strip("'")).date()
end_date_obj   = pd.to_datetime(endDate.strip("'")).date()

final_df = roster_df_fdut_pairing_count.merge(
    standby_activation_count.drop(columns=['Duty Window Number']),
    how='left',
    on=['Date','Station','Duty Window','Rank']
)
final_df['Standby Activation Count'] = final_df['Standby Activation Count'].fillna(0).astype(int)
final_df['Date'] = pd.to_datetime(final_df['Date']).dt.date
final_df = final_df[final_df['Date'].between(start_date_obj, end_date_obj)]
final_df = final_df.sort_values(['Date','Station','Duty Window','Rank']).reset_index(drop=True)

final_df.to_csv(out_path, index=False)
print(f"Saved: {out_path}")
print(f"Final output shape: {final_df.shape}")
print(final_df.head(10))
