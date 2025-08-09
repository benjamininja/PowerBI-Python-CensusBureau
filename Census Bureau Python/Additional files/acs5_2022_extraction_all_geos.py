import pandas as pd
import censusdis.data as ced
from censusdis.datasets import ACS5
from requests.exceptions import ReadTimeout
from tqdm import tqdm
import time
import os
import re

# --- Configuration ---
ACS_YEAR = 2022
DATASET = ACS5
MAX_VARS_PER_CALL = 50
RETRY_LIMIT = 3
WAIT_BETWEEN_RETRIES = 2

# --- Step 1: Load curated table list and variable list ---
excel_path = r"C:\Users\benha\OneDrive\Projects\Census Bureau\acs5_2022_variables_FINAL.xlsx"
csv_path = r"C:\Users\benha\OneDrive\Projects\Census Bureau\acs5_2022_variables_filtered.csv"

tables_df = pd.read_excel(excel_path, sheet_name="Tables_Curated")
table_list = tables_df["table"].astype(str).unique().tolist()
print(f"Loaded {len(table_list)} ACS tables. Example: {table_list[:5]}")

vars_df = pd.read_csv(csv_path)

# Keep only variables in the selected tables
vars_final = vars_df[vars_df["table"].astype(str).isin(table_list)].copy()

# Only keep variables that match ACS variable code pattern (like B01001_001E or DP03_0001M)
acs_pattern = re.compile(r'^[A-Z]+\d+_\d+[EM]$')
vars_final = vars_final[vars_final["VARIABLE"].str.match(acs_pattern, na=False)]

# Add MOE variables
vars_final["MOE_VAR"] = vars_final["VARIABLE"].str.replace("E$", "M", regex=True)

# --- Step 2: Prepare geographies ---
geographies = ["tract", "zcta", "county", "state"]

def fetch_data_chunk(variable_list, geo_level):
    if geo_level == "tract":
        query_params = dict(state="*", county="*", tract="*")
    elif geo_level == "zcta":
        query_params = dict(zip_code_tabulation_area="*")
    elif geo_level == "county":
        query_params = dict(state="*", county="*")
    elif geo_level == "state":
        query_params = dict(state="*")
    else:
        raise ValueError("Unsupported geography level.")
    
    df = ced.download(DATASET, ACS_YEAR, variable_list, **query_params, with_geometry=True)
    
    # Standardize ID columns for join and output
    if geo_level == "tract":
        df["state"] = df["state"].astype(str).str.zfill(2)
        df["county"] = df["county"].astype(str).str.zfill(3)
        df["tract"] = df["tract"].astype(str).str.zfill(6)
        df["GEOID"] = df["state"] + df["county"] + df["tract"]
        df.set_index("GEOID", inplace=True)
    elif geo_level == "zcta":
        zcta_col = [c for c in df.columns if "zip" in c.lower() or c.upper()=="ZCTA"][0]
        df.rename(columns={zcta_col: "ZCTA"}, inplace=True)
        df["ZCTA"] = df["ZCTA"].astype(str).str.zfill(5)
        df.set_index("ZCTA", inplace=True)
    elif geo_level == "county":
        df["state"] = df["state"].astype(str).str.zfill(2)
        df["county"] = df["county"].astype(str).str.zfill(3)
        df["FIPS"] = df["state"] + df["county"]
        df.set_index("FIPS", inplace=True)
    elif geo_level == "state":
        df["state"] = df["state"].astype(str).str.zfill(2)
        df.set_index("state", inplace=True)
    
    # Convert numerics
    for col in df.columns:
        if col not in ["geometry", "NAME"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    return df

# --- Step 3: Loop through tables and geographies ---
for table_id in tqdm(table_list, desc="Downloading ACS data by table"):
    table_vars = vars_final[vars_final["table"] == table_id]["VARIABLE"].tolist()
    table_moes = [v.replace("E", "M") for v in table_vars]
    variables_all = table_vars + table_moes

    # Skip tables with no valid variables
    if not variables_all:
        print(f"Skipping {table_id}: No valid ACS variables found.")
        continue
    
    for geo in geographies:
        geo_gdf = None
        for i in range(0, len(variables_all), MAX_VARS_PER_CALL):
            chunk_vars = variables_all[i:i+MAX_VARS_PER_CALL]
            retries = RETRY_LIMIT
            while retries > 0:
                try:
                    chunk_df = fetch_data_chunk(chunk_vars, geo)
                    break
                except ReadTimeout:
                    retries -= 1
                    time.sleep(WAIT_BETWEEN_RETRIES)
            if geo_gdf is None:
                geo_gdf = chunk_df
            else:
                geo_gdf = geo_gdf.join(chunk_df, how="left")
        
        # Calculate CV for each estimate
        for var in table_vars:
            moe_var = var.replace("E", "M")
            if var in geo_gdf.columns and moe_var in geo_gdf.columns:
                geo_gdf[var + "_CV"] = (geo_gdf[moe_var] / 1.645) / geo_gdf[var] * 100.0
        
        # Save to Parquet
        output_file = f"acs5_{ACS_YEAR}_{table_id}_{geo}.parquet"
        geo_gdf.to_parquet(output_file)
        print(f"Saved {geo} data for table {table_id} to {output_file}")
