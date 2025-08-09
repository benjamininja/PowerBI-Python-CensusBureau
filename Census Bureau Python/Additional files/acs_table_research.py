import pandas as pd
import censusdis.data as ced
from censusdis.datasets import ACS5
from requests.exceptions import ReadTimeout
from tqdm import tqdm
import time
import os

# --- Configuration ---
ACS_VINTAGE = 2022
MAX_RESULTS = 20
RETRY_LIMIT = 3
WAIT_BETWEEN_RETRIES = 2
CHECKPOINT_INTERVAL = 250
MIN_VARIABLES_PER_TABLE = 3

print(f"Loading ACS5 {ACS_VINTAGE} variable groups...")
groups_df = ced.variables.all_groups(ACS5, ACS_VINTAGE)
print(f"Total groups (tables): {len(groups_df)}")
print(groups_df.head())

# Filter to relevant ACS prefixes
groups_df = groups_df[groups_df['GROUP'].str.startswith(('B','C','DP','S'))]
groups = groups_df['GROUP'].tolist()
print(f"Filtered to relevant groups: {len(groups)}")
print("\nSample table names and descriptions:")
print(groups_df[['GROUP','DESCRIPTION']].head(10))

# --- Step 1: Initial Summary by Table Prefix ---
groups_summary = pd.Series(groups).str[0].value_counts()
print("\nInitial Table Prefix Summary:")
print(groups_summary)

# --- Step 2: Pre-check variable counts to skip low-value tables ---
print("\nChecking variable counts to filter out very small tables...")
precheck_counts = []
for group in tqdm(groups, desc="Prechecking variable counts"):
    try:
        count = len(ced.variables.all_variables(ACS5, ACS_VINTAGE, group))
        precheck_counts.append((group, count))
    except ReadTimeout:
        precheck_counts.append((group, 0))

precheck_df = pd.DataFrame(precheck_counts, columns=['table','variable_count'])
precheck_df = precheck_df.merge(groups_df[['GROUP','DESCRIPTION']], left_on='table', right_on='GROUP', how='left')
precheck_df.drop(columns='GROUP', inplace=True)

# Filter small tables
valid_groups = precheck_df[precheck_df['variable_count'] >= MIN_VARIABLES_PER_TABLE]['table'].tolist()
print(f"\nTables retained after filtering (<{MIN_VARIABLES_PER_TABLE} vars dropped): {len(valid_groups)}")
print("\nTop 10 tables by variable count (pre-check):")
print(precheck_df.sort_values('variable_count', ascending=False).head(10)[['table','DESCRIPTION','variable_count']])

print("\nBottom 10 tables by variable count (pre-check):")
print(precheck_df.sort_values('variable_count', ascending=True).head(10)[['table','DESCRIPTION','variable_count']])

# --- Step 3: Download variables with checkpointing ---
all_vars = []
checkpoint_counter = 0

for idx, group in enumerate(tqdm(valid_groups, desc="Downloading ACS Variables"), start=1):
    retries = RETRY_LIMIT
    while retries > 0:
        try:
            df = ced.variables.all_variables(ACS5, ACS_VINTAGE, group)
            df["table"] = group
            all_vars.append(df)
            break
        except ReadTimeout:
            retries -= 1
            time.sleep(WAIT_BETWEEN_RETRIES)
    
    # Checkpoint every N tables
    if idx % CHECKPOINT_INTERVAL == 0:
        checkpoint_counter += 1
        checkpoint_file = f"acs5_2022_variables_checkpoint_{checkpoint_counter}.csv"
        pd.concat(all_vars, ignore_index=True).to_csv(checkpoint_file, index=False)
        print(f"\nCheckpoint saved to: {os.path.join(os.getcwd(), checkpoint_file)}")

variables_df = pd.concat(all_vars, ignore_index=True)
variables_df['supports_tract'] = True
variables_df['supports_zcta'] = variables_df['table'].str.startswith(('B','C'))

print(f"\nTotal variables collected: {len(variables_df)}")
print(variables_df.head())

# --- Step 4: Table-level summary for duplication check ---
table_counts = variables_df.groupby('table').size().reset_index(name='variable_count')
table_counts = table_counts.merge(groups_df[['GROUP','DESCRIPTION']], left_on='table', right_on='GROUP', how='left')
table_counts.drop(columns='GROUP', inplace=True)
table_counts.sort_values('variable_count', ascending=False, inplace=True)

# print("\nTop 10 tables by variable count:")
# print(table_counts.head(10)[['table','DESCRIPTION','variable_count']])
# print("\nBottom 10 tables by variable count:")
# print(table_counts.tail(10)[['table','DESCRIPTION','variable_count']])

# --- Step 5: Summary by table prefix with average variable count ---
table_counts['prefix'] = table_counts['table'].str[0]
prefix_summary = table_counts.groupby('prefix')['variable_count'].agg(['count','mean','max','min']).reset_index()
print("\nSummary by table prefix (count of tables, avg var count, max/min):")
print(prefix_summary)

# --- Step 6: Helper functions for research ---
def search_variables(keyword, table_prefix=None, tract_only=False, zcta_only=False, limit=MAX_RESULTS):
    df = variables_df
    if table_prefix:
        df = df[df['table'].str.startswith(table_prefix)]
    if tract_only:
        df = df[df['supports_tract']]
    if zcta_only:
        df = df[df['supports_zcta']]
    return df[df['label'].str.contains(keyword, case=False, na=False)].head(limit)[['name','label','concept','table']]

def preview_table(table_name):
    return variables_df[variables_df['table'] == table_name][['name','label','concept','table']]

if __name__ == "__main__":
    print("\n--- Example: Search 'income' in B tables ---")
    print(search_variables("income", table_prefix='B', tract_only=True, limit=10))

    print("\n--- Example: Preview table B19013 ---")
    print(preview_table('B19013'))

    output_file = os.path.join(os.getcwd(), "acs5_2022_variables_filtered.csv")
    variables_df.to_csv(output_file, index=False)
    print(f"\nFull variable list saved to: {output_file}")

output_file_tables = os.path.join(os.getcwd(), "acs5_2022_table_summary.csv")
table_counts[['table','DESCRIPTION','variable_count','prefix']].to_csv(output_file_tables, index=False)
print(f"Table-level summary saved to: {output_file_tables}")