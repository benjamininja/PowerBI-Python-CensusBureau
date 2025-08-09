# Metric_Research.py
# Metric Research: Analyzing ACS Variables
# This script demonstrates how to access and analyze ACS variables using the `censusdis` package.
import censusdis.data as ced
from censusdis.datasets import ACS5

#Define the ACS 5-year dataset and vintage
# ACS5 is the 5-year American Community Survey dataset
ACS_VINTAGE = 2022  # ACS 5-year vintage
variables_df = ced.variables[ACS5, ACS_VINTAGE].df
print(f"Total variables in ACS5 {ACS_VINTAGE}: {len(variables_df)}")


detailed_vars = variables_df[variables_df['name'].str.startswith(('B','C'))]
dp_vars = variables_df[variables_df['name'].str.startswith('DP')]
subject_vars = variables_df[variables_df['name'].str.startswith('S')]

print(f"Detailed tables: {len(detailed_vars)}")
print(f"Data profiles: {len(dp_vars)}")
print(f"Subject tables: {len(subject_vars)}")

# --- Keyword Search Function ---
def search_variables(keyword, table_type=None, limit=20):
    """
    Search ACS variables by keyword and optional table type prefix.
    table_type can be 'B', 'C', 'DP', or 'S'.
    """
    df = variables_df
    if table_type:
        df = df[df['name'].str.startswith(table_type)]
    result = df[df['label'].str.contains(keyword, case=False, na=False)]
    return result[['name', 'label']].head(limit)

# Example searches:
print("\n--- Search for 'income' ---")
print(search_variables('income'))

print("\n--- Search for 'health insurance' in Subject Tables ---")
print(search_variables('health insurance', table_type='S'))


# --- 3. Filter by Table Prefix ---
# Detailed tables (B and C)
detailed_vars = variables_df[variables_df['name'].str.startswith(('B','C'))]

# Data Profiles (DP)
dp_vars = variables_df[variables_df['name'].str.startswith('DP')]

# Subject Tables (S)
subject_vars = variables_df[variables_df['name'].str.startswith('S')]

print(f"Detailed tables: {len(detailed_vars)} variables")
print(f"Data profiles: {len(dp_vars)} variables")
print(f"Subject tables: {len(subject_vars)} variables")

# --- 4. Keyword Search Function ---
def search_variables(keyword, table_type=None):
    """
    Search ACS variables by keyword and optional table type.
    table_type can be 'B', 'C', 'DP', or 'S'
    """
    df = variables_df
    if table_type:
        df = df[df['name'].str.startswith(table_type)]
    result = df[df['label'].str.contains(keyword, case=False, na=False)]
    return result[['name', 'label']].head(20)  # show first 20 matches

# Example Searches
print("\n--- Example: Search for 'income' in all tables ---")
print(search_variables('income'))

print("\n--- Example: Search for 'health insurance' in Subject Tables (S) ---")
print(search_variables('health insurance', table_type='S'))

# --- 5. Export Variable List (Optional) ---
# Save to CSV if you want to browse in Excel
variables_df[['name','label']].to_csv('acs5_2022_variables.csv', index=False)
