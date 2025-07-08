import dlt
from pyspark.sql.functions import (
    col, when, current_timestamp
)

@dlt.table(
    comment="Raw order data from bronze layer"
)
def bronze_transactions_transform():
    return spark.read.table("workspace.default.bronze_transactions")

@dlt.table(
    name="silver_orders",
    comment="Cleaned and enriched order data"
)
@dlt.expect("valid_transaction_date", "transaction_date IS NOT NULL")
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_product_id", "product_id IS NOT NULL")
@dlt.expect("non_negative_quantity", "quantity >= 0")
@dlt.expect("non_negative_amount", "total_amount >= 0")
def silver_orders():
    df = dlt.read("bronze_transactions_transform")

    cleaned_df = df.filter(
        col("transaction_date").isNotNull() &
        col("customer_id").isNotNull() &
        col("product_id").isNotNull()
    ).select(
        col("transaction_id"),
        col("customer_id"),
        col("product_id"),
        when(col("quantity") < 0, 0).otherwise(col("quantity")).alias("quantity"),
        when(col("total_amount") < 0, 0).otherwise(col("total_amount")).alias("total_amount"),
        col("transaction_date").cast("date").alias("transaction_date"),
        col("payment_method"),
        col("store_type"),
        when((col("quantity") == 0) | (col("total_amount") == 0), "Cancelled")
            .otherwise("Completed").alias("order_status"),
        current_timestamp().alias("last_updated")
    )

    return cleaned_df
