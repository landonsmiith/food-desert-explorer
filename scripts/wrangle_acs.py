import pandas as pd
import os
import re

# Map filenames to table IDs and data dictionary filenames
acs_files = {
    "ACSDT5Y2023.B25070-Data.csv": ("B25070", "B25070.xlsx"),
    "ACSDT5Y2023.B01001-Data.csv": ("B01001", "B01001.xlsx"),
    "ACSDT5Y2023.B17001-Data.csv": ("B17001", "B17001.xlsx"),
    "ACSDT5Y2023.B08301-Data.csv": ("B08301", "B08301.xlsx"),
    "ACSDT5Y2023.B19001-Data.csv": ("B19001", "B19001.xlsx"),
    "ACSDT5Y2023.B25044-Data.csv": ("B25044", "B25044.xlsx")
}

RAW_DIR = "data/raw/"
PROCESSED_DIR = "data/processed/"
os.makedirs(PROCESSED_DIR, exist_ok=True)

def load_data_dict(dict_path):
    # Loads a data dictionary Excel file and returns a mapping from line number to description
    df = pd.read_excel(dict_path)
    mapping = {}
    for _, row in df.iterrows():
        if pd.notnull(row['Line Number']):
            # Convert to int, then string, e.g., 3.0 -> "3"
            line = str(int(float(row['Line Number'])))
            desc = str(row['Description']).strip().lower()
            desc = desc.replace('%', 'pct').replace('$', 'dollars')
            desc = re.sub(r'[^a-zA-Z0-9_]', '', desc.replace(' ', '_'))
            mapping[line] = desc
    return mapping

def map_acs_columns(cols, table_id, mapping):
    # Systematically map ACS columns to human-readable names using the data dictionary
    new_cols = []
    for col in cols:
        m = re.match(rf'({table_id})_(\d{{3}})([EM])', col, re.IGNORECASE)
        if m:
            line = str(int(m.group(2)))  # "003" -> "3"
            suffix = m.group(3).upper()
            desc = mapping.get(line, line)
            desc_clean = re.sub(r'[^a-zA-Z0-9_]', '', desc.lower())
            new_col = f"{table_id.lower()}_{m.group(2)}{suffix}_{desc_clean}"
            new_cols.append(new_col)
        else:
            new_cols.append(col.lower())
    return new_cols

def wrangle_acs_file(filename, table_id, dict_filename):
    # Load the data dictionary mapping
    dict_path = os.path.join(RAW_DIR, dict_filename)
    mapping = load_data_dict(dict_path)
    # Read the ACS CSV (first row is header)
    df = pd.read_csv(os.path.join(RAW_DIR, filename), header=0)
    # Drop the second row if it's a label row (starts with 'Geography')
    if df.iloc[0, 0].startswith("Geography"):
        df = df.iloc[1:].reset_index(drop=True)
    # Extract tract GEOID
    df["tract_geoid"] = df["GEO_ID"].astype(str).str[-11:]
    # Drop original GEO_ID and NAME columns if present
    for col in ["GEO_ID", "NAME"]:
        if col in df.columns:
            df = df.drop(columns=[col])
    # Map columns
    df.columns = map_acs_columns(df.columns, table_id, mapping)
    # Keep tract_geoid first
    cols = ["tract_geoid"] + [c for c in df.columns if c != "tract_geoid"]
    return df[cols]

# Wrangle all files and merge
dfs = []
for filename, (table_id, dict_filename) in acs_files.items():
    print(f"Processing {filename} ({table_id})...")
    df = wrangle_acs_file(filename, table_id, dict_filename)
    dfs.append(df)

from functools import reduce
acs_merged = reduce(lambda left, right: pd.merge(left, right, on="tract_geoid", how="outer"), dfs)
acs_merged.to_csv(os.path.join(PROCESSED_DIR, "acs_merged.csv"), index=False)
print("ACS wrangling complete. Merged file saved to data/processed/acs_merged.csv")
