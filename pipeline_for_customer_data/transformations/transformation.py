import dlt
from pyspark.sql.functions import col, when, current_timestamp, datediff, current_date


# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


@dlt.table(
  name="silver_customers",
  comment="Cleaned and enriched customer data"
)
@dlt.expect("valid_email", "email IS NOT NULL")
@dlt.expect("valid_age", "age BETWEEN 18 AND 100")
@dlt.expect("non_negative_purchases", "total_purchases >= 0")
def silver_customers():
    bronze_df = dlt.read("bronze_customer")

    enriched_df = bronze_df.filter(
        (col("age").between(18, 100)) &
        (col("email").isNotNull()) &
        (col("total_purchases") >= 0)
    ).select(
        col("customer_id"),
        col("name"),
        col("email"),
        col("country"),
        col("customer_type"),
        col("registration_date"),
        col("age"),
        col("gender"),
        col("total_purchases"),
        when(col("total_purchases") > 10000, "High Value")
          .when(col("total_purchases") > 5000, "Medium Value")
          .otherwise("Low Value")
          .alias("customer_segment"),
        datediff(current_date(), col("registration_date")).alias("days_since_registration"),
        current_timestamp().alias("last_updated")
    )

    return enriched_df
