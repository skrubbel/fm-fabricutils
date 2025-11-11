## Template for SQL -> PySpark convertion

In the following prompts use the following PySpark coding style and example as a template for translating both PySpark and T-SQL code provided. The output is used in a PySpark notebook. Code content to be translated/refactored will be identified with SQL > PySpark or PySpark > PySpark to identify source type and target. Template code:

## General Coding Guidelines

### Translation of T-SQL to PySpark and PySpark to PySpark

1. Use snake_case for table-, column, and variable names, unless source requires otherwise, until aliasing is required in result_df
2. Use PascalCase aliasing for columns in the result_df unless column name and alias both are the same (compared on a case sensitive basis)
3. Use aliasing for tables in joins
4. Use aliasing for tables in select statements
5. Use F alias for pyspark.sql.functions
6. Prefer using F.col("column_name") or F.col("alias.column_name") syntax for column references
7. Prefix table names with "fno_" to indicate that they are from the source system
8. Postfix table names with "_df" to indicate that they are DataFrames
9. Prefer result_df as the name for the resulting DataFrame to be saved in the target lakehouse
10. Alwways use the DeltaLakehouse class for source and target lakehouses
11. Always include new colums for DataLoadTimestampUtc, DataLoadTimestampLocal and SurrogateKey
12. SourceSystem, DataLoadTimestampUtc and DataLoadTimestampLocal should be first columns in the select statement


```python
from datetime import date import pyspark.sql.functions as F

# Initate source and target lakehouses, using the DeltaLakehouse class from_lakehouse_name function
source_lh = DeltaLakehouse.from_lakehouse_name("raw_lh_enterprisedata")
target_lh = DeltaLakehouse.from_lakehouse_name("enriched_lh_enterprisedata")

# Load dataframes using the DeltaLakehouse.get_data_frame function
fno_tableone_df = source_lh.get_data_frame(
    table_name="tableone",
    columns=["col1", "col2", "col3"]
    )

fno_tabletwo_df = source_lh.get_data_frame(
    table_name="tabletwo",
    columns=["col1", "col2", "col4"]
    )

# Perform table joins with alias's 
joined_df = fno_tableone_df.alias("one").join(
    fno_tabletwo_df.alias("two"),
    (F.col("one.col1") == F.col("two.col1")) &
    (F.col("one.col2") == F.col("two.col2")),
    "left"
)

# Select specific columns from the joined DataFrame
# Add additional columns for SourceSystem, DataLoadTimestampUtc, DataLoadTimestampLocal and SurrogateKey (dim tables only)
result_df = joined_df.select(
    F.lit("D365FO").alias("SourceSystem"),
    F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("DataLoadTimestampUtc") ,
    F.date_format(F.from_utc_timestamp(F.current_timestamp(), "Europe/Copenhagen"), "yyyy-MM-dd HH:mm:ss").alias("DataLoadTimestampLocal"),
    # Use this only for Dim tables, where we need a SurrogateKey:
    (F.monotonically_increasing_id() + 100000000).alias("SurrogateKey"),
    F.col("one.col1").alias("ColumnOne"),
    F.col("one.col2").alias("ColumnTwo"),
    F.col("one.col3").alias("ColumnThree"),
    F.col("two.col4").alias("ColumnFour"),
)

# Save table in target lakehouse using the DeltaLakehouse.save_data_frame function
target_lh.save_data_frame(
    data_frame=result_df,
    table_name="DimTableName"
    )
```