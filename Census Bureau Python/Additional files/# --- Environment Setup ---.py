# --- Environment Setup ---
import censusdis.data as ced
from censusdis.datasets import ACS5
import pandas as pd

# --- Configuration ---
HOUSTON_STATE_FIPS = '48'   # Texas
HARRIS_COUNTY_FIPS = '201'  # Harris County
ACS_VINTAGE = 2022          # Adjust to latest ACS 5-year release

# Selected ACS variables (Estimates + MOEs)
download_variables = [
    'B01003_001E', 'B01003_001M',  # Total Population
    'B19013_001E', 'B19013_001M',  # Median Household Income
    'B25058_001E', 'B25058_001M',  # Median Rent
]

# Friendly column names
column_renames = {
    'B01003_001E': 'TotalPopulation',
    'B01003_001M': 'TotalPopulation_MOE',
    'B19013_001E': 'MedianHouseholdIncome',
    'B19013_001M': 'MedianHouseholdIncome_MOE',
    'B25058_001E': 'MedianMonthlyRent',
    'B25058_001M': 'MedianMonthlyRent_MOE',
}

# --- Step 1: Download ACS Data ---
print("Downloading ACS 5-year data for Harris County, TX...")
gdf_houston_tracts = ced.download(
    dataset=ACS5,
    vintage=ACS_VINTAGE,
    download_variables=download_variables,
    state=HOUSTON_STATE_FIPS,
    county=HARRIS_COUNTY_FIPS,
    tract='*',
    with_geometry=True
)
print("Download complete.")

# --- Step 2: Clean and Rename Columns ---
gdf_houston_tracts.rename(columns=column_renames, inplace=True)
for col in column_renames.values():
    if col != 'NAME':
        gdf_houston_tracts[col] = pd.to_numeric(gdf_houston_tracts[col], errors='coerce')

# --- Step 3: Compute CV for Estimates ---
estimate_moe_pairs = [
    ('TotalPopulation', 'TotalPopulation_MOE'),
    ('MedianHouseholdIncome', 'MedianHouseholdIncome_MOE'),
    ('MedianMonthlyRent', 'MedianMonthlyRent_MOE')
]

for estimate_col, moe_col in estimate_moe_pairs:
    se_col = f"{estimate_col}_SE"
    cv_col = f"{estimate_col}_CV"

    gdf_houston_tracts[se_col] = gdf_houston_tracts[moe_col] / 1.645
    gdf_houston_tracts[cv_col] = (
        gdf_houston_tracts[se_col] / gdf_houston_tracts[estimate_col]
    ) * 100

print("CV calculation complete.")
print(gdf_houston_tracts.head())

# --- Step 4: Write to Microsoft Fabric Lakehouse ---
lakehouse_path = "/lakehouse/default/Files/census_data/"
output_filename = "houston_acs5_tract_data.parquet"

gdf_houston_tracts.to_parquet(f"{lakehouse_path}{output_filename}", index=False)
print(f"Data written to {lakehouse_path}{output_filename}")

# --- Optional: Local CSV Backup ---
# gdf_houston_tracts.to_csv("houston_acs5_tract_data_backup.csv", index=False)
# print("Local CSV backup created.")
