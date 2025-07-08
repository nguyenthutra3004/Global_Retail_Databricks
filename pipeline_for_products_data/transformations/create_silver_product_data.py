import dlt
from pyspark.sql.functions import (
    col, when, current_timestamp
)


# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


@dlt.table(
    name="bronze_products",
    comment="Raw product data from bronze layer"
)
def bronze_products():
    return spark.read.table("globalretail_bronze.bronze_products")
