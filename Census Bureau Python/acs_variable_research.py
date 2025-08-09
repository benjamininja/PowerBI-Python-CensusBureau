"""
Optimized ACS 5-Year Variable Research Tool
-------------------------------------------
- Filters to B, C, DP, and S tables
- Retries API calls on timeout
- Shows progress bar
- Provides search and table preview functions
"""
import pandas as pd
import censusdis.data as ced
from censusdis.datasets import ACS5
from requests.exceptions import ReadTimeout
import time
import os

# --- Configuration ---
ACS_VINTAGE = 2022           # ACS 5-year vintage
MAX_RESULTS = 20             # Max rows to display in search/preview
RETRY_LIMIT = 3              # Number of retries for API timeouts
WAIT_BETWEEN_RETRIES = 3     # Seconds to wait before retrying a failed request

# Step 1: Load all ACS5 groups for the specified vintage
# This returns a DataFrame with columns like DATASET, YEAR, GROUP, DESCRIPTION
groups_df = ced.variables.all_groups(ACS5, ACS_VINTAGE)
groupCount_start = {len(groups_df)}
print(f"Total groups (tables): {len(groups_df)}")

# Step 2: Extract table names from the GROUP column and filter to relevant ACS prefixes
# - B and C tables: Detailed tables, support tract/ZCTA
# - DP tables: Data profiles, tract-level summaries
# - S tables: Subject tables, tract-level summaries
groups = groups_df['GROUP'].tolist()
groups = [g for g in groups if g.startswith(('B','C','DP','S'))]
print(f"Filtered to relevant groups: {len(groups)}")
print("Sample filtered groups:", groups[:100])

# Step 3: Loop through each table and fetch all variables
# Includes retry logic to handle Census API timeouts (ReadTimeout)
all_vars = []
for i, group in enumerate(groups, start=1):
    retries = RETRY_LIMIT
    while retries > 0:
        try:
            # Download all variables for a given table (group) and vintage
            df = ced.variables.all_variables(ACS5, ACS_VINTAGE, group)
            df["table"] = group  # Track which table each variable belongs to
            all_vars.append(df)
            break  # Exit retry loop on success
        except ReadTimeout:
            retries -= 1
            time.sleep(WAIT_BETWEEN_RETRIES)

# Combine all variables into a single DataFrame for exploration
variables_df = pd.concat(all_vars, ignore_index=True)

# Step 4: Add simple flags to identify small-area support
# Tracts are always supported for ACS 5-year; ZCTAs generally for B/C tables
variables_df['supports_tract'] = True
variables_df['supports_zcta'] = variables_df['table'].str.startswith(('B','C','DP','S'))

print(f"Total variables collected: {len(variables_df)}")
print(variables_df.head())  # Preview first few variables

# Step 5: Define helper functions for research
def search_variables(keyword, table_prefix=None, tract_only=False, zcta_only=False, limit=MAX_RESULTS):
    """
    Search variables by keyword with optional filters:
    - table_prefix: 'B', 'C', 'DP', or 'S'
    - tract_only/zcta_only: filter by geography support
    """
    df = variables_df
    if table_prefix:
        df = df[df['table'].str.startswith(table_prefix)]
    if tract_only:
        df = df[df['supports_tract']]
    if zcta_only:
        df = df[df['supports_zcta']]
    return df[df['label'].str.contains(keyword, case=False, na=False)].head(limit)[['name','label','concept','table']]

def preview_table(table_name):
    """
    Return all variables for a given ACS table (e.g., 'B19013', 'DP03', 'S2701').
    """
    return variables_df[variables_df['table'] == table_name][['name','label','concept','table']]

if __name__ == "__main__":
    # Example usage to confirm the script works interactively
    print("\n--- Example: Search 'income' in B tables ---")
    print(search_variables("income", table_prefix='B', tract_only=True, limit=10))

    print("\n--- Example: Preview table B19013 ---")
    print(preview_table('B19013'))

    # Optional: Save for offline research
output_file = os.path.join(os.getcwd(), "acs5_2022_variables_filtered.csv")
variables_df.to_csv(output_file, index=False)
print(f"\nFull variable list saved to: {output_file}")
