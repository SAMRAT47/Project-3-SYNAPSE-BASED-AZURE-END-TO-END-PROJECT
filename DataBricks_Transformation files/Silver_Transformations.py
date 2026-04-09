import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# --- 1. CALENDAR ---
@dlt.table(name="Transformed_Calender")
@dlt.expect_or_fail("valid_Date", "Date IS NOT NULL")
def Transformed_Calender():
    return (
        dlt.read_stream("Raw_Calendar")
        .withColumn("Date", coalesce(
            to_date(trim(col("Date")), "M/d/yyyy"),
            to_date(trim(col("Date")), "MM-dd-yyyy")
        ))
        .withColumn('Month', month(col('Date')))
        .withColumn('Year', year(col('Date')))
        .drop("_rescued_data")
    )

# --- 2. CUSTOMERS ---
@dlt.table(name="Transformed_Customers")
@dlt.expect_or_drop("valid_email", "EmailAddress LIKE '%@%.%'")
@dlt.expect("valid_customer_key", "CustomerKey IS NOT NULL")
def Transformed_Customers():
    return (
        dlt.read_stream("Raw_Customers")
        .withColumn("FullName", concat_ws(" ", col("Prefix"), col("FirstName"), col("LastName")))
        .drop("_rescued_data")
    )

# --- 3. PRODUCTS ---
@dlt.table(name="Transformed_Products")
@dlt.expect("positive_cost", "ProductCost > 0")
@dlt.expect_or_drop("valid_sku", "ProductSKU IS NOT NULL")
def Transformed_Products():
    return (
        dlt.read_stream("Raw_Products")
        .withColumn("ProductCategory", split(col("ProductName"), " ")[0])
        .withColumn("SKU_Prefix", split(col("ProductSKU"), "-")[0])
        .drop("_rescued_data")
    )

@dlt.table(
    name="Transformed_Sales",
    comment="Consolidated sales data with mixed date format handling"
)
@dlt.expect_or_drop("valid_order_qty", "OrderQuantity > 0")
def Transformed_Sales():
    return (
        dlt.read_stream("Raw_Sales")
        # Handle StockDate with mixed formats
        .withColumn("StockDate", coalesce(
            to_date(trim(col("StockDate")), "M/d/yyyy"),   # Try slashes (1/13/2015)
            to_date(trim(col("StockDate")), "MM-dd-yyyy"), # Try hyphens (01-01-2015)
            to_date(trim(col("StockDate")), "yyyy-MM-dd")  # Try ISO (2015-01-01)
        ))
        # Handle OrderDate with mixed formats
        .withColumn("OrderDate", coalesce(
            to_date(trim(col("OrderDate")), "M/d/yyyy"),
            to_date(trim(col("OrderDate")), "MM-dd-yyyy"),
            to_date(trim(col("OrderDate")), "yyyy-MM-dd")
        ))
        .withColumn("StockDate", col("StockDate").cast("timestamp"))
        # Business transformation: replace S with T
        .withColumn("OrderNumber", regexp_replace(col("OrderNumber"), "S", "T"))
        # Drop technical metadata
        .drop("_rescued_data")
    )

# --- 7. RETURNS ---
@dlt.table(name="Transformed_Returns")
@dlt.expect("valid_return_qty", "ReturnQuantity > 0")
def Transformed_Returns():
    return (
        dlt.read_stream("Raw_Returns")
        .withColumn("ReturnDate", coalesce(
            to_date(trim(col("ReturnDate")), "M/d/yyyy"),
            to_date(trim(col("ReturnDate")), "MM-dd-yyyy")
        ))
        .drop("_rescued_data")
    )

# --- 8. TERRITORIES ---
@dlt.table(name="Transformed_Territories")
@dlt.expect_or_fail("valid_territory_key", "SalesTerritoryKey IS NOT NULL")
def Transformed_Territories():
    return dlt.read_stream("Raw_Territories").drop("_rescued_data")


@dlt.table(
    name="Sales_Analysis_Daily",
    comment="Aggregated sales data: total orders by date for visualization."
)
def Sales_Analysis_Daily():
    return (
        dlt.read("Transformed_Sales")
        .groupBy("OrderDate")
        .agg(count("OrderNumber").alias("TotalOrders"))
        .orderBy("OrderDate")
    )

