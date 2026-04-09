# 1. Define your Target Base Path
silver_path = "abfss://silver@samratlake92.dfs.core.windows.net/Transformed_Data/"

# 2. List of tables you want to "Export" to ADLS
tables = [
    "Transformed_Calender", 
    "Transformed_Customers", 
    "Transformed_Products", 
    "Transformed_Sales", 
    "Transformed_Returns", 
    "Transformed_Territories",
    "Sales_Analysis_Daily",
    "Raw_Product_Subcategories",
    "Raw_Product_Categories"
]

# 3. Loop through and write them out to your specific folders
for table_name in tables:
    print(f"Exporting {table_name} to ADLS...")
    
    # Read the data from the DLT output (Unity Catalog)
    df = spark.read.table(f"`synapse-proj-cata`.`silver`.`{table_name}`")
    
    # Write to your specific ADLS folder as Delta (or Parquet)
    # This gives you the "Fixed" folder structure you want
    df.write.mode("overwrite").format("delta").save(f"{silver_path}{table_name}")

print("All tables successfully saved to the Silver container!")