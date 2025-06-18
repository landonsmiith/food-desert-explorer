import geopandas as gpd
import pandas as pd

# Paths to GDBs and layers
smart_gdb_path = "data/processed/smartlocation.gdb"
walk_gdb_path = "data/processed/walkability.gdb"
smart_layer = "EPA_SLD_Database_V3"
walk_layer = "NationalWalkabilityIndex"

# Read attribute tables
smart_gdf = gpd.read_file(smart_gdb_path, layer=smart_layer)
walk_gdf = gpd.read_file(walk_gdb_path, layer=walk_layer)

# Drop geometry columns if present
if 'geometry' in smart_gdf.columns:
    smart_gdf = smart_gdf.drop(columns='geometry')
if 'geometry' in walk_gdf.columns:
    walk_gdf = walk_gdf.drop(columns='geometry')

# Ensure both have a column named GEOID20 (12-digit block group FIPS)
if 'GEOID20' not in smart_gdf.columns:
    raise ValueError("No GEOID20 column found in Smart Location GDB.")
if 'GEOID20' not in walk_gdf.columns:
    # Try to create it from another column if possible
    if 'BlkGrpID' in walk_gdf.columns:
        walk_gdf['GEOID20'] = walk_gdf['BlkGrpID'].astype(str).str.zfill(12)
    else:
        raise ValueError("No GEOID20 or BlkGrpID column found in Walkability GDB.")

# Remove duplicates if any
smart_gdf = smart_gdf.drop_duplicates(subset='GEOID20')
walk_gdf = walk_gdf.drop_duplicates(subset='GEOID20')

# Merge on GEOID20 (block group-level join)
merged = pd.merge(smart_gdf, walk_gdf, on='GEOID20', how='outer', suffixes=('_smart', '_walk'))

# Export to CSV
merged.to_csv("data/processed/gdb_merged_by_bg.csv", index=False)
print("Merged CSV saved to data/processed/gdb_merged_by_bg.csv")
