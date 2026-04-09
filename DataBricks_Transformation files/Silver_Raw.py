import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name="Raw_Calendar",
    comment="Raw calender data from ADLS using DLT"
)
def Raw_Calendar():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://bronze@samratlake92.dfs.core.windows.net/Calendar/")
    )

@dlt.table(
    name="Raw_Customers",
    comment="Raw customers data from ADLS using DLT"
)
def Raw_Customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://bronze@samratlake92.dfs.core.windows.net/Customers/")
    )

@dlt.table(
    name="Raw_Product_Categories",
    comment="Raw Product_Categories data from ADLS using DLT"
)
def Raw_Product_Categories():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://bronze@samratlake92.dfs.core.windows.net/Product_Categories/")
    )

@dlt.table(
    name="Raw_Product_Subcategories",
    comment="Raw Product_Subcategories data from ADLS using DLT"
)
def Raw_Product_Subcategories():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://bronze@samratlake92.dfs.core.windows.net/Product_Subcategories/")
    )

@dlt.table(
    name="Raw_Products",
    comment="Raw Products data from ADLS using DLT"
)
def Raw_Products():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://bronze@samratlake92.dfs.core.windows.net/Products/")
    )

@dlt.table(
    name="Raw_Returns",
    comment="Raw Returns data from ADLS using DLT"
)
def Raw_Returns():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://bronze@samratlake92.dfs.core.windows.net/Returns/")
    )


@dlt.table(
    name="Raw_Sales",
    comment="Raw Sales data from ADLS using DLT"
)
def Raw_Sales():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://bronze@samratlake92.dfs.core.windows.net/Sales_*/")
    )


@dlt.table(
    name="Raw_Territories",
    comment="Raw Territories data from ADLS using DLT"
)
def Raw_Territories():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("abfss://bronze@samratlake92.dfs.core.windows.net/Territories/")
    )
