import dlt
from pyspark.sql.functions import col, when, current_timestamp, datediff, current_date
from utilities import utils


# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


@dlt.table(
  name="bronze_customer",
  comment="Raw customer data from bronze layer"
)
def bronze_customer():
    return spark.read.table("globalretail_bronze.bronze_customer")
