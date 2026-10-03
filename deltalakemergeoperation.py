# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake MERGE Operation

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Schema

# COMMAND ----------

spark.sql('create schema if not exists merge')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import PySpark Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define Sales Data Schema

# COMMAND ----------

sales_schema = StructType([
    StructField("OrderID", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Product", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("UnitPrice", IntegerType(), True),
    StructField("Discount", IntegerType(), True),
    StructField("OrderDate", DateType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Read Sales Data from CSV

# COMMAND ----------

sales_df=spark.read.format('csv').option('header',True).schema(sales_schema).load('/Volumes/digibrainsade/pyspark/files/Sales.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Display the Sales Data

# COMMAND ----------

sales_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create the Target Delta Table

# COMMAND ----------

sales_df.write.format('delta').mode('overwrite').option('overwriteSchema',True).saveAsTable('merge.sales_target')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create Source Data for MERGE

# COMMAND ----------

source_data = [

    (1001, "Rahul", "Hyderabad", "Laptop",
     "Electronics", 5, 55000, 5, "2026-01-05"),

    (1002, "Priya", "Bangalore", "Mobile",
     "Electronics", 4, 22000, 10, "2026-01-06"),

    (1016, "Varshini", "Hyderabad", "Mobile",
     "Electronics", 8, 30000, 5, "2026-09-22")
]



# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Define Source DataFrame Columns
# MAGIC

# COMMAND ----------

columns = [
    "OrderID",
    "CustomerName",
    "City",
    "Product",
    "Category",
    "Quantity",
    "UnitPrice",
    "Discount",
    "OrderDate"
]



# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Create Source DataFrame
# MAGIC

# COMMAND ----------

source_df = spark.createDataFrame(source_data, columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Create Temporary View for Source Data

# COMMAND ----------

source_df.createOrReplaceTempView("sales_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. MERGE Operation Using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO merge.sales_target AS target
# MAGIC
# MAGIC USING sales_source AS source
# MAGIC
# MAGIC ON target.OrderID = source.OrderID
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC
# MAGIC   UPDATE SET
# MAGIC
# MAGIC     target.CustomerName = source.CustomerName,
# MAGIC     target.City = source.City,
# MAGIC     target.Product = source.Product,
# MAGIC     target.Category = source.Category,
# MAGIC     target.Quantity = source.Quantity,
# MAGIC     target.UnitPrice = source.UnitPrice,
# MAGIC     target.Discount = source.Discount,
# MAGIC     target.OrderDate = source.OrderDate
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC
# MAGIC   INSERT (
# MAGIC     OrderID,
# MAGIC     CustomerName,
# MAGIC     City,
# MAGIC     Product,
# MAGIC     Category,
# MAGIC     Quantity,
# MAGIC     UnitPrice,
# MAGIC     Discount,
# MAGIC     OrderDate
# MAGIC   )
# MAGIC
# MAGIC   VALUES (
# MAGIC     source.OrderID,
# MAGIC     source.CustomerName,
# MAGIC     source.City,
# MAGIC     source.Product,
# MAGIC     source.Category,
# MAGIC     source.Quantity,
# MAGIC     source.UnitPrice,
# MAGIC     source.Discount,
# MAGIC     source.OrderDate
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. View the Target Table After MERGE

# COMMAND ----------

select * from merge.sales_target;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. MERGE Using Inline SQL Source Data

# COMMAND ----------

MERGE INTO merge.sales_target AS target

USING (

    SELECT
        1001 AS OrderID,
        'Ravi' AS CustomerName,
        'Laptop' AS Product,
        5 AS Quantity,
        55000 AS UnitPrice

    UNION ALL

    SELECT
        1002,
        'Priya',
        'Mobile',
        4,
        22000

    UNION ALL

    SELECT
        1016,
        'Varshini',
        'Mobile',
        8,
        30000

) AS source

ON target.OrderID = source.OrderID

WHEN MATCHED THEN
    UPDATE SET
        target.CustomerName = source.CustomerName,
        target.Product = source.Product,
        target.Quantity = source.Quantity,
        target.UnitPrice = source.UnitPrice

WHEN NOT MATCHED THEN
    INSERT (
        OrderID,
        CustomerName,
        Product,
        Quantity,
        UnitPrice
    )
    VALUES (
        source.OrderID,
        source.CustomerName,
        source.Product,
        source.Quantity,
        source.UnitPrice
    );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. MERGE Operation Using PySpark DeltaTable API

# COMMAND ----------

from delta.tables import DeltaTable

target_table = DeltaTable.forName(spark, "Sales")

target_table.alias("target") \
    .merge(
        source_df.alias("source"),
        "target.OrderID = source.OrderID"
    ) \
    .whenMatchedUpdate(set={
        "CustomerName": "source.CustomerName",
        "City": "source.City",
        "Product": "source.Product",
        "Category": "source.Category",
        "Quantity": "source.Quantity",
        "UnitPrice": "source.UnitPrice",
        "Discount": "source.Discount",
        "OrderDate": "source.OrderDate"
    }) \
    .whenNotMatchedInsert(values={
        "OrderID": "source.OrderID",
        "CustomerName": "source.CustomerName",
        "City": "source.City",
        "Product": "source.Product",
        "Category": "source.Category",
        "Quantity": "source.Quantity",
        "UnitPrice": "source.UnitPrice",
        "Discount": "source.Discount",
        "OrderDate": "source.OrderDate"
    }) \
    .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. MERGE Using whenMatchedUpdateAll() and whenNotMatchedInsertAll()

# COMMAND ----------

from delta.tables import DeltaTable

# Load the target Delta table
target_table = DeltaTable.forName(spark, "Sales")

# Perform MERGE operation
target_table.alias("target") \
    .merge(
        source_df.alias("source"),
        "target.OrderID = source.OrderID"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Summary
# MAGIC
# MAGIC This notebook demonstrates Delta Lake MERGE operations using SQL and PySpark.
# MAGIC It covers matched-record updates, not-matched record inserts, inline SQL source data,
# MAGIC and the `whenMatchedUpdateAll()` / `whenNotMatchedInsertAll()` approach.