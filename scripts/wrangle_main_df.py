import pandas as pd
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Paths 
ACS_PATH = 'data/processed/acs_merged.csv'
FOOD_PATH = 'data/processed/FoodAccessAtlasData.xlsx'
PLACES_PATH = ('data/processed/'
               'PLACES__Local_Data_for_Better_Health__Census_Tract_Data_2024_release_20250615.csv')
GDB_PATH = 'data/processed/gdb_merged_by_bg.csv'

# Load 
acs_df = pd.read_csv(ACS_PATH, low_memory=False)
food_access_df = pd.read_excel(FOOD_PATH)
places_df = pd.read_csv(PLACES_PATH)
gdb_df = pd.read_csv(GDB_PATH)

# GDB to tract level 
gdb_df = gdb_df[gdb_df['CSA_Name_smart'].notna() & (gdb_df['CSA_Name_smart'].str.strip() != '')]

def trim_geoid(g):
    s = str(g)
    return s[:-1] if len(s) == 12 else s
gdb_df['GEOID'] = gdb_df['GEOID10_smart'].apply(trim_geoid)

explanatory_cols = [
    'GEOID10_smart', 'GEOID20', 'STATEFP_smart', 'COUNTYFP_smart', 'TRACTCE_smart',
    'BLKGRPCE_smart', 'CSA_smart', 'CSA_Name_smart', 'CBSA_smart', 'CBSA_Name_smart',
    'GEOID10_walk', 'STATEFP_walk', 'COUNTYFP_walk', 'TRACTCE_walk', 'BLKGRPCE_walk',
    'CSA_walk', 'CSA_Name_walk', 'CBSA_walk', 'CBSA_Name_walk', 'Region'
]

sum_cols = [
    'CBSA_POP', 'CBSA_EMP', 'CBSA_WRK', 'Ac_Total_smart', 'Ac_Water_smart',
    'Ac_Land_smart', 'Ac_Unpr_smart', 'TotPop_smart', 'CountHU_smart', 'HH_smart',
    'AutoOwn0', 'AutoOwn1', 'AutoOwn2p', 'Workers_smart', 'R_LowWageWk',
    'R_MedWageWk', 'R_HiWageWk', 'TotEmp', 'E5_Ret', 'E5_Off', 'E5_Ind', 'E5_Svc',
    'E5_Ent', 'E8_Ret', 'E8_off', 'E8_Ind', 'E8_Svc', 'E8_Ent', 'E8_Ed', 'E8_Hlth',
    'E8_Pub', 'E_LowWageWk', 'E_MedWageWk', 'E_HiWageWk', 'Households', 'Workers_1',
    'Residents', 'Drivers', 'Vehicles', 'White', 'Male', 'Lowwage', 'Medwage',
    'Highwage', 'Annual_GHG', 'C_R_Households', 'C_R_Pop', 'C_R_Workers',
    'C_R_Drivers', 'C_R_Vehicles', 'C_R_White', 'C_R_Male', 'C_R_Lowwage',
    'C_R_Medwage', 'C_R_Highwage',
    'Ac_Total_walk', 'Ac_Water_walk', 'Ac_Land_walk', 'Ac_Unpr_walk', 'TotPop_walk',
    'CountHU_walk', 'HH_walk', 'Workers_walk'
]

avg_cols = [
    'P_WrkAge', 'Pct_AO0', 'Pct_AO1', 'Pct_AO2p', 'R_PCTLOWWAGE', 'E_PctLowWage',
    'D1A', 'D1B', 'D1C', 'D1C5_RET', 'D1C5_OFF', 'D1C5_IND', 'D1C5_SVC',
    'D1C5_ENT', 'D1C8_RET', 'D1C8_OFF', 'D1C8_IND', 'D1C8_SVC', 'D1C8_ENT',
    'D1C8_ED', 'D1C8_HLTH', 'D1C8_PUB', 'D1D', 'D1_FLAG', 'D2A_JPHH', 'D2B_E5MIX',
    'D2B_E5MIXA', 'D2B_E8MIX', 'D2B_E8MIXA_smart', 'D2A_EPHHM_smart', 'D2C_TRPMX1',
    'D2C_TRPMX2', 'D2C_TRIPEQ', 'D2R_JOBPOP', 'D2R_WRKEMP', 'D2A_WRKEMP',
    'D2C_WREMLX', 'D3A', 'D3AAO', 'D3AMM', 'D3APO', 'D3B_smart', 'D3BAO',
    'D3BMM3', 'D3BMM4', 'D3BPO3', 'D3BPO4', 'D4A_smart', 'D4B025', 'D4B050',
    'D4C', 'D4D', 'D4E', 'D5AR', 'D5AE', 'D5BR', 'D5BE', 'D5CR', 'D5CRI', 'D5CE',
    'D5CEI', 'D5DR', 'D5DRI', 'D5DE', 'D5DEI', 'D2A_Ranked_smart',
    'D2B_Ranked_smart', 'D3B_Ranked_smart', 'D4A_Ranked_smart', 'NatWalkInd_smart',
    'W_P_Lowwage', 'W_P_Medwage', 'W_P_Highwage', 'GasPrice', 'logd1a', 'logd1c',
    'logd3aao', 'logd3apo', 'd4bo25', 'd5dei_1', 'logd4d', 'UPTpercap',
    'NonCom_VMT_Per_Worker', 'Com_VMT_Per_Worker', 'VMT_per_worker',
    'GHG_per_worker', 'SLC_score', 'B_C_constant', 'B_C_male', 'B_C_ld1c',
    'B_C_drvmveh', 'B_C_ld1a', 'B_C_ld3apo', 'B_C_inc1', 'B_C_gasp', 'B_N_constant',
    'B_N_inc2', 'B_N_inc3', 'B_N_white', 'B_N_male', 'B_N_drvmveh', 'B_N_gasp',
    'B_N_ld1a', 'B_N_ld1c', 'B_N_ld3aao', 'B_N_ld3apo', 'B_N_d4bo25', 'B_N_d5dei',
    'B_N_UPTpc', 'C_R_DrmV', 'NatWalkInd_walk', 'D2B_E8MIXA_walk',
    'D2A_EPHHM_walk', 'D3B_walk', 'D4A_walk', 'D2A_Ranked_walk', 'D2B_Ranked_walk',
    'D3B_Ranked_walk', 'D4A_Ranked_walk', 'VMT_tot_avg'
]

agg = {c: 'first' for c in explanatory_cols if c in gdb_df.columns}
agg.update({c: 'sum' for c in sum_cols if c in gdb_df.columns})

if 'VMT_tot_min' in gdb_df.columns:
    agg['VMT_tot_min'] = 'min'
if 'VMT_tot_max' in gdb_df.columns:
    agg['VMT_tot_max'] = 'max'

agg.update({c: 'mean' for c in avg_cols if c in gdb_df.columns})

gdb_tract = gdb_df.groupby('GEOID').agg(agg).reset_index()

# PLACES
places_df = places_df.rename(columns={'LocationName': 'GEOID'})
places_df['GEOID'] = places_df['GEOID'].astype(str).str.zfill(11)
places_wide = (places_df[['GEOID', 'Measure', 'Data_Value']]
               .pivot_table(index='GEOID', columns='Measure', values='Data_Value')
               .reset_index())

# Helpers 
def drop_unwanted_columns(df, drop_margins=True):
    cols = [c for c in df.columns
            if c.lower().startswith('unnamed') or
               (drop_margins and re.search(r'_[0-9]{3}M_', c))]
    return df.drop(columns=cols)

def collapse_age_buckets(df, gender, offset):
    buckets = {'under18': range(3, 7), '18to64': range(7, 20), '65plus': range(20, 26)}
    for label, rng in buckets.items():
        cols = [f'b01001_{i+offset:03d}E' for i in rng if f'b01001_{i+offset:03d}E' in df.columns]
        if cols:
            df[f'{gender}_{label}'] = df[cols].sum(axis=1)
            df.drop(columns=cols, inplace=True)

def geoid_filter(df):
    out = df.copy()
    out['GEOID'] = pd.to_numeric(out['GEOID'], errors='coerce')
    return out[out['GEOID'] < 79000000000]

# ACS + FoodAccess clean 
acs_df = drop_unwanted_columns(acs_df)
food_access_df = drop_unwanted_columns(food_access_df, drop_margins=False)

collapse_age_buckets(acs_df, 'male', 0)
collapse_age_buckets(acs_df, 'female', 26)

acs_df = acs_df.rename(columns={'tract_geoid': 'GEOID'})
food_access_df = food_access_df.rename(columns={'CensusTract': 'GEOID'})

for df in [acs_df, food_access_df, places_wide, gdb_tract]:
    df['GEOID'] = df['GEOID'].astype(str).str.zfill(11)

acs_df = geoid_filter(acs_df)
food_access_df = geoid_filter(food_access_df)
places_wide = geoid_filter(places_wide)
gdb_tract = geoid_filter(gdb_tract)

# Merge 
merged = (acs_df
          .merge(food_access_df, on='GEOID', how='left')
          .merge(places_wide, on='GEOID', how='left')
          .merge(gdb_tract, on='GEOID', how='left')
          .sort_values('GEOID')
          .reset_index(drop=True))

# Duplicate / sparsity cleanup 
def deduplicate_rows(df):
    dup_rows = df.duplicated().sum()
    if dup_rows:
        logging.info('Removing %d exact duplicate rows', dup_rows)
        df = df.drop_duplicates()

    dup_geoids = df.duplicated('GEOID', keep=False)
    if dup_geoids.any():
        logging.info('%d rows share a GEOID; keeping the most complete per tract',
                     dup_geoids.sum())
        df = (df.assign(_nans=df.isna().sum(1))
                .sort_values(['GEOID', '_nans'])
                .drop_duplicates('GEOID', keep='first')
                .drop(columns='_nans'))
    return df

def deduplicate_columns(df):
    to_drop = []
    cols = df.columns.tolist()
    for i, c1 in enumerate(cols):
        if c1 in to_drop:
            continue
        for c2 in cols[i+1:]:
            if c2 not in to_drop and df[c1].equals(df[c2]):
                to_drop.append(c2)
                logging.info('Column %s duplicates %s; dropping', c2, c1)
    return df.drop(columns=to_drop)

def drop_sparse(df, thresh=0.5):
    sparse = df.columns[df.isna().mean() > thresh]
    if sparse.any():
        logging.info('Dropping %d sparse columns (>%.0f%% NaNs)', len(sparse), thresh*100)
        df = df.drop(columns=sparse)
    return df

merged = (merged
          .pipe(deduplicate_rows)
          .pipe(deduplicate_columns)
          .pipe(drop_sparse, thresh=0.5))


# Save 
merged.to_csv('data/processed/final_df.csv', index=False)

if __name__ == '__main__':
    print('Final shape:', merged.shape)
