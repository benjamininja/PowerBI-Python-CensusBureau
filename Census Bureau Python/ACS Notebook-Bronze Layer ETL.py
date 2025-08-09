#!/usr/bin/env python
# coding: utf-8

# ## ACS Notebook-Bronze Layer ETL
# 
# New notebook

# ### **Bronze Layer Ingestion of 2022 ACS 5-Year Data with PySpark and censusdis**
# #### Introduction
# This PySpark notebook demonstrates a Bronze layer ingestion of the 2018-2022 American Community Survey (ACS) 5-Year estimates using the censusdis library. In a medallion architecture, the Bronze layer stores raw data (in this case, ACS census data with minimal processing) as a foundation for downstream Silver/Gold transformations. We will load curated ACS table definitions, filter variables to a selected subset of tables, retrieve the data for multiple geographic levels (census tracts, ZIP Code Tabulation Areas (ZCTAs), counties, and states) including geometry (boundaries), and store each result as a Delta/Parquet dataset in the Fabric Lakehouse Bronze schema. Along the way, we compute reliability metrics (margins of error and coefficient of variation) and document each step for clarity and maintenance.

# In[ ]:


# Ensure required libraries are available (censusdis, openpyxl for Excel if needed)
# %pip install --upgrade --quiet openpyxl censusdis


# **Step 1: Load ACS 5-Year Table and Variable Metadata from Excel Files**
# This step reads the curated list of ACS tables and the corresponding variable metadata from two Excel files stored in the Fabric Lakehouse (OneLake) via ABFS paths. We will use Spark’s Excel reader to directly load data from these Excel files using their abfss:// paths, leveraging the com.crealytics.spark.excel connector
# stackoverflow.com
# . This avoids needing to download files or use Pandas (which cannot directly read abfss paths). The result will be two PySpark DataFrames prepared for our ETL pipeline: one for the curated ACS tables, and one for the filtered variables belonging to those tables.
# 
# Read Curated Table List from the Filtered Tables Excel
# First, load the curated list of ACS tables from the Filtered Tables Excel file. The relevant sheet (named "Tables_Filtered") contains the table ID along with its description and other info. We use Spark to read this sheet directly from the OneLake path. We enable the header row and infer the schema so that numeric fields are properly typed (you could also define a schema explicitly for performance). Reading a specific sheet by name helps avoid parsing the entire workbook, improving efficiency.
# 
# The curated_tables_df now holds the list of ACS tables we want to focus on. Each entry includes the table ID (e.g. B19013), a descriptive label, and possibly additional info like the variable count or prefix. We will use these table IDs in subsequent steps to fetch data via the Census API (using censusdis) and to filter the variables metadata.
# 

# In[1]:


# ABFS file paths for the Excel files in the Fabric Lakehouse (OneLake)
filtered_tables_path = "abfss://eb839f94-4ea2-408b-aae1-1e942045b263@onelake.dfs.fabric.microsoft.com/2d9b734c-dc6c-4bed-acc5-6a7d80071788/Files/default/ACS%205yr%20-%202022%20vintage%20-%20Filtered%20Tables.xlsx"
all_variables_path   = "abfss://eb839f94-4ea2-408b-aae1-1e942045b263@onelake.dfs.fabric.microsoft.com/2d9b734c-dc6c-4bed-acc5-6a7d80071788/Files/default/ACS%205yr%20-%202022%20vintage%20-%20All%20Variables.xlsx"

# Read the curated tables sheet into a Spark DataFrame
curated_tables_df = (
    spark.read.format("com.crealytics.spark.excel")
         .option("header", "true")
         .option("inferSchema", "true")
         .option("sheetName", "Tables_Filtered")  # sheet with curated table list
         .load(filtered_tables_path)
)
curated_tables_df.printSchema()
curated_tables_df.show(5)


# Filter the Full Variables Metadata to Curated Tables
# 
# Next, load the All Variables Excel, which contains metadata for all ACS 5-year variables (across all tables). We will filter this comprehensive list down to only the variables from our curated tables. If the curated table list is small, Spark will efficiently broadcast it for the join/filter operation. (For a very large list of tables, you might prefer a join without collecting to driver, as shown below.)
# 
# Note: If reading the entire “All Variables” file is not necessary (for example, if a pre-filtered variables list is already available or if you plan to retrieve variables via an API directly), you can skip loading this file to save time. In our case, we demonstrated the filtering step for completeness. We now have filtered_vars_df, which contains only the variables (codes, labels, etc.) for the tables in our curated list. This will serve as a focused data dictionary for the variables we plan to ingest, effectively acting as a variable metadata reference. Such a filtered metadata set will be useful when building a data model (e.g., creating a DimVariableMetadata table mapping codes to friendly descriptions).
# 

# In[ ]:


# ABFS file paths for the Excel files in the Fabric Lakehouse (OneLake)
filtered_tables_path = "abfss://eb839f94-4ea2-408b-aae1-1e942045b263@onelake.dfs.fabric.microsoft.com/2d9b734c-dc6c-4bed-acc5-6a7d80071788/Files/default/ACS%205yr%20-%202022%20vintage%20-%20Filtered%20Tables.xlsx"
all_variables_path   = "abfss://eb839f94-4ea2-408b-aae1-1e942045b263@onelake.dfs.fabric.microsoft.com/2d9b734c-dc6c-4bed-acc5-6a7d80071788/Files/default/ACS%205yr%20-%202022%20vintage%20-%20All%20Variables.xlsx"

# Read the curated tables sheet into a Spark DataFrame
curated_tables_df = (
    spark.read.format("com.crealytics.spark.excel")
         .option("header", "true")
         .option("inferSchema", "true")
         .option("sheetName", "Tables_Filtered")  # sheet with curated table list
         .load(filtered_tables_path)
)
curated_tables_df.printSchema()
curated_tables_df.show(5)

# Collect curated table IDs into a Python list for filtering
curated_table_ids = [row["table"] for row in curated_tables_df.select("table").collect()]
print(f"Total curated tables: {len(curated_table_ids)}")  # e.g., number of tables
print(curated_table_ids[:5])  # print a sample of table IDs

# Read all ACS variables metadata from the Excel (could be large)
all_vars_df = (
    spark.read.format("com.crealytics.spark.excel")
         .option("header", "true")
         .option("inferSchema", "true")
         .option("sheetName", "Variables_All")
         .load(all_variables_path)
)

# Filter the variables to only those belonging to the curated tables
filtered_vars_df = all_vars_df.filter( all_vars_df["table"].isin(curated_table_ids) )

# (Alternative approach without collecting to list, using join for scalability)
# filtered_vars_df = all_vars_df.join(curated_tables_df.select("table").distinct(), on="table", how="inner")
filtered_vars_df.printSchema()
print(f"Filtered variable count: {filtered_vars_df.count()}")  # number of variables after filtering
filtered_vars_df.show(5)


# Prepare DataFrames for Lakehouse Bronze Layer Ingestion
# At this point we have two DataFrames ready to be saved or used in downstream processing:
# curated_tables_df – Contains the curated ACS table list (Table ID and its description/label, and possibly a topic or category). This can be considered a reference list of tables we will ingest.
# filtered_vars_df – Contains the detailed variable metadata for only those tables (including variable codes and human-readable labels). This will allow us to interpret the data and serve as a lookup for variable definitions.
# To integrate with the broader ETL process, we ensure these DataFrames are compatible with the data extraction step. The table IDs from curated_tables_df can drive the census API calls (e.g., using censusdis to fetch each table’s data by geography), and the filtered variable list ensures we only handle relevant fields. Finally, we will save these DataFrames as managed tables in the Bronze schema of our Fabric Lakehouse. Using Spark’s saveAsTable on a Fabric notebook will create Delta Lake tables by default (Fabric Lakehouse tables use the Delta format under the hood [source: microsoftlearning.github.io]). Storing these as Bronze-level tables means they are raw metadata ready for use in subsequent transformations and joins. This also allows other notebooks or Power BI to directly query these tables.
# 

# In[ ]:


# Save the DataFrames as managed Delta tables in the Bronze schema of the Lakehouse
spark.sql("CREATE SCHEMA IF NOT EXISTS Bronze")  # Ensure Bronze schema exists

curated_tables_df.write.format("delta").mode("overwrite").saveAsTable("Bronze.ACS_Curated_Tables")
filtered_vars_df.write.format("delta").mode("overwrite").saveAsTable("Bronze.ACS_Filtered_Variables")

# Now the tables are stored in the Lakehouse (Tables/Bronze folder in OneLake) as Delta tables.
# They can be queried with Spark SQL or used for joins in downstream analysis.


# **Step 2: Configure censusdis for Data Extraction**
# We will use the censusdis library to download ACS data. The ACS 5-Year 2022 dataset is identified in censusdis by the dataset name "acs/acs5" and year 2022. We define the list of target geographies for ingestion: census tract, ZCTA (ZIP Code Tabulation Area), county, and state. For each geography, we prepare the appropriate query parameters to retrieve all records nationwide. The censusdis.data.download function allows us to specify wildcard selectors for geographies – for example, state="*" and county="*" will retrieve all counties in all states【9†L226-L234】. For tracts, we use state="*", county="*", tract="*" to get every tract nationally【25†L45-L53】. For ZCTAs, the parameter is zip_code_tabulation_area="*"【25†L45-L53】. We also set with_geometry=True to have censusdis include the geographic boundary shape for each region in the resulting data frame【8†L46-L53】. This means the output will contain a column with geometry (as polygons) in addition to the numeric variables, saving us the trouble of merging separate shapefiles. Before extraction, we note that the Census API imposes a limit of 50 variables per API call【23†L228-L236】. To handle tables with many variables (each table has both estimate and margin-of-error fields for each indicator), we will chunk the requests into batches of ≤50 variables and merge the results back together. The censusdis library can handle group downloads, but we explicitly implement chunking to stay within limits and ensure reliability. We will also incorporate basic error-handling (e.g., retries) for robustness, though not shown here for brevity.
# 
# In the code below, fetch_geo_data will retrieve data for the given list of variables (vars_list) at the specified geography level. We use wildcards (*) to fetch all geographic units of that level nationwide【9†L226-L234】. After the first chunk, we standardize the geographic identifier columns by zero-padding and create a unique key (e.g., 11-digit tract GEOID, 5-digit county FIPS, etc.), then set it as the index for easy merging. Subsequent chunks are joined on this index to produce a complete dataset for that table and geography. Non-numeric values are coerced to numeric, and the NAME (area name) and geometry columns are preserved as-is. Using with_geometry=True ensures that each row includes a polygon geometry for the area【8†L46-L53】.
# 

# In[ ]:


import pandas as pd
import censusdis.data as ced

ACS_DATASET = "acs/acs5"
ACS_YEAR = 2022
MAX_VARS_PER_CALL = 50

geographies = ["tract", "zcta", "county", "state"]

# Helper function to download data for a given list of variables and geography
def fetch_geo_data(vars_list, geo_level):
    """Download ACS data for the specified variables and geography level, including geometry."""
    # Determine query params for the geography level
    if geo_level == "tract":
        params = dict(state="*", county="*", tract="*")
    elif geo_level == "zcta":
        params = dict(zip_code_tabulation_area="*")
    elif geo_level == "county":
        params = dict(state="*", county="*")
    elif geo_level == "state":
        params = dict(state="*")
    else:
        raise ValueError(f"Unsupported geography: {geo_level}")
    # Download in chunks if variable list is large
    df_full = None
    for i in range(0, len(vars_list), MAX_VARS_PER_CALL):
        chunk_vars = vars_list[i:i+MAX_VARS_PER_CALL]
        # Perform the API call with geometry
        chunk_df = ced.download(ACS_DATASET, ACS_YEAR, chunk_vars, **params, with_geometry=True)
        if df_full is None:
            df_full = chunk_df    # Set up the DataFrame for the first chunk
            if geo_level == "tract":
                df_full["state"]  = df_full["state"].astype(str).str.zfill(2)
                df_full["county"] = df_full["county"].astype(str).str.zfill(3)
                df_full["tract"]  = df_full["tract"].astype(str).str.zfill(6)
                df_full["GEOID"]  = df_full["state"] + df_full["county"] + df_full["tract"]  # 11-digit tract FI:contentReference[oaicite:4]{index=4}     df_full.set_index("GEOID", inplace=True)
            elif geo_level == "zcta":
                # The API might return column named either 'zip code tabulation area' or 'ZCTA' depending on source
                geo_col = [c for c in df_full.columns if c.lower().startswith("zip") or c.upper()=="ZCTA"][0]
                df_full.rename(columns={geo_col: "ZCTA"}, inplace=True)
                df_full["ZCTA"] = df_full["ZCTA"].astype(str).str.zfill(5)
                df_full.set_index("ZCTA", inplace=True)
            elif geo_level == "county":
                df_full["state"]  = df_full["state"].astype(str).str.zfill(2)
                df_full["county"] = df_full["county"].astype(str).str.zfill(3)
                df_full["FIPS"]   = df_full["state"] + df_full["county"]  # 5-digit county FIPS
                df_full.set_index("FIPS", inplace=True)
            elif geo_level == "state":
                df_full["state"] = df_full["state"].astype(str).str.zfill(2)
                df_full.set_index("state", inplace=True)
        else:
            # Ensure the chunk has the same index setup for joining
            if geo_level == "tract":
                chunk_df["state"]  = chunk_df["state"].astype(str).str.zfill(2)
                chunk_df["county"] = chunk_df["county"].astype(str).str.zfill(3)
                chunk_df["tract"]  = chunk_df["tract"].astype(str).str.zfill(6)
                chunk_df["GEOID"]  = chunk_df["state"] + chunk_df["county"] + chunk_df["tract"]
                chunk_df.set_index("GEOID", inplace=True)
            elif geo_level == "zcta":
                geo_col = [c for c in chunk_df.columns if c.lower().startswith("zip") or c.upper()=="ZCTA"][0]
                chunk_df.rename(columns={geo_col: "ZCTA"}, inplace=True)
                chunk_df["ZCTA"] = chunk_df["ZCTA"].astype(str).str.zfill(5)
                chunk_df.set_index("ZCTA", inplace=True)
            elif geo_level == "county":
                chunk_df["state"]  = chunk_df["state"].astype(str).str.zfill(2)
                chunk_df["county"] = chunk_df["county"].astype(str).str.zfill(3)
                chunk_df["FIPS"]   = chunk_df["state"] + chunk_df["county"]
                chunk_df.set_index("FIPS", inplace=True)
            elif geo_level == "state":
                chunk_df["state"] = chunk_df["state"].astype(str).str.zfill(2)
                chunk_df.set_index("state", inplace=True)
            # Join on index (geographic key) to combine chunks
            df_full = df_full.join(chunk_df, how="left")
    # Convert all numeric columns from string to numeric types (coerce errors to NaN)
    for col in df_full.columns:
        if col not in ("NAME", "geometry"):
            df_full[col] = pd.to_numeric(df_full[col], errors="coerce")
    return df_full


# **Step 3: Download ACS Data by Table and Geography**
# Now we loop through each curated table and download its data for each geography in our list. For each table, we will retrieve all estimate variables and their corresponding Margin of Error (MOE) variables. The ACS API provides MOE fields with the same base code but ending in "M" instead of "E" for estimates (for example, B01001_001E is the estimate and B01001_001M is its MOE). We generate the full list of variables for each table (E and M) and pass them to our fetch_geo_data function. After fetching, we compute the Coefficient of Variation (CV) for each estimate. The CV is a relative measure of sampling error, calculated as the ratio of the standard error to the estimate【2†L43-L51】. Since the MOE in ACS is given at a 90% confidence level, we derive the standard error as MOE/1.645【2†L58-L61】. We then compute CV% = (MOE / 1.645) / Estimate * 100 for each estimate variable, expressing it as a percentage of the estimate. Each table-geography combination is written immediately to the Lakehouse Bronze layer as a Delta/Parquet file. We use mode("overwrite") to replace any existing data for that combination on repeat runs, ensuring idempotent ingestion. For large datasets (like tract or county level), we will partition the output by state to optimize downstream queries (so that queries filtering by state will read only that state's partition). The outputs are stored in the Bronze schema under a logical path grouping by dataset (e.g. table B01001 at county level might be saved as Bronze.census_acs2022_B01001_county). Note: The loop below may take significant time [source: censusdis.readthedocs.io] - it represents a large volume of data from the Census API. In a production setting, consider distributing the work or caching intermediate results. Also ensure that a valid Census API key is configured if required (the censusdis library will use your environment's API key by default).
# 

# In[ ]:


from tqdm import tqdm  # progress indicator (if not installed, use pip to install tqdm)

# Collect variable info into Python for iteration
vars_pd = filtered_vars_df.toPandas()  # this is safe as the number of variables is moderate
table_groups = vars_pd.groupby("table")

for table_id, vars_subset in tqdm(table_groups, desc="Downloading ACS data by table"):
    # Prepare list of estimate variables and corresponding MOE variables for this table
    var_codes = vars_subset["VARIABLE"].tolist()              # e.g. ['B01001_001E', 'B01001_002E', ...]
    moe_codes = [v.replace("E", "M") for v in var_codes]      # corresponding MOE codes
    all_vars  = var_codes + moe_codes
    
    if not all_vars:  # skip if no variables (should not happen for curated tables)
        continue
    
    for geo in geographies:
        print(f"Processing table {table_id} at {geo} level...")
        # Fetch data for this table and geography (with geometry)
        geo_df = fetch_geo_data(all_vars, geo) 
        # Compute Coefficient of Variation for each estimate variable
        for est_var in var_codes:
            moe_var = est_var.replace("E", "M")
            if est_var in geo_df.columns and moe_var in geo_df.columns:
                geo_df[f"{est_var}_CV"] = (geo_df[moe_var] / 1.645) / geo_df[est_var] * 100.0
        
        # Reset index to turn the geo identifier into a column, and convert to Spark DataFrame
        geo_df.reset_index(inplace=True)  # index is GEOID, ZCTA, FIPS, or state depending on geo
        # If geometry is present as shapely objects, convert to Well-Known Text for Spark compatibility
        if "geometry" in geo_df.columns:
            geo_df["geometry_wkt"] = geo_df["geometry"].apply(lambda geom: geom.wkt if geom is not None else None)
            geo_df.drop(columns=["geometry"], inplace=True)
        spark_df = spark.createDataFrame(geo_df)
        
        # Partition by state for large geographies to improve write performance and downstream querying
        writer = spark_df.write.mode("overwrite").format("delta")
        if geo in ("tract", "county"):
            writer = writer.partitionBy("state")
        target_table_name = f"Bronze.census_acs2022_{table_id}_{geo}"
        writer.saveAsTable(target_table_name)
        print(f"Saved {table_id} {geo} data to Bronze layer as {target_table_name}")


# **Step 4: Cache Metadata – Table and Variable Reference**
# Finally, we store the reference metadata (table and variable definitions) in the Bronze layer as well, so that downstream processes or analysts can easily look up descriptions for any variable. We combine the curated table info with the variable list to create a comprehensive reference table. This census_variable_reference table will include the table ID, table description, variable code, and the variable’s descriptive label. This allows users or later AI integration to interpret ACS codes with human-readable labels【19†L98-L102】.
# 
# The Bronze.census_variable_reference table now contains a row for each variable ingested, annotated with the table it belongs to, the table's description, and the variable's label (e.g., "Median household income in the past 12 months"). This metadata will be invaluable in the Silver/Gold layer for providing context (for example, enriching data with descriptions or enabling AI to explain variables by name). With all ACS 2022 data ingested at the Bronze stage and a reference metadata table created, the data is now ready to be transformed and integrated in the Silver layer. This completes the Bronze ingestion process, ensuring raw ACS data (with geometries and reliability metrics) is securely stored in the Fabric Lakehouse and organized for efficient access in subsequent analytics workflows.
# 

# In[ ]:


# Prepare a metadata DataFrame for variables and tables
vars_meta_df = filtered_vars_df.join(curated_tables_df, on="table", how="left") \
    .select("table", "DESCRIPTION", "VARIABLE", "LABEL")
#:contentReference[oaicite:9]{index=9}:contentReference[oaicite:10]{index=10}arity
vars_meta_df = vars_meta_df.withColumnRenamed("DESCRIPTION", "table_description") \
                           .withColumnRenamed("VARIABLE", "variable_code") \
                           .withColumnRenamed("LABEL", "variable_label")
# Write the variable reference table to Bronze schema
vars_meta_df.write.mode("overwrite").format("delta").saveAsTable("Bronze.census_variable_reference")

