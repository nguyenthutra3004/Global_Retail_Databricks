import dlt
from pyspark.sql.functions import (
    col, when, current_timestamp
)


# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


@dlt.table(
    name="bronze_transactions",
    comment="Raw order data from bronze layer"
)
def bronze_transactions():
    return spark.read.table("globalretail_bronze.bronze_transactions")
