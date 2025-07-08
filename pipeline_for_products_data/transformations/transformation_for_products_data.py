import dlt
from pyspark.sql.functions import (
    col, when, current_timestamp
)

@dlt.table(
    comment="Raw product data from bronze layer"
)
def bronze_products_transform():
    return spark.read.table("workspace.default.bronze_products")

@dlt.table(
    name="silver_products",
    comment="Cleaned and enriched product data"
)
@dlt.expect("valid_name", "name IS NOT NULL")
@dlt.expect("valid_category", "category IS NOT NULL")
@dlt.expect("valid_price", "price >= 0")
@dlt.expect("valid_rating", "rating BETWEEN 0 AND 5")
@dlt.expect("valid_stock", "stock_quantity >= 0")
def silver_products():
    df = dlt.read("bronze_products_transform")

    cleaned_df = df.filter(
        col("name").isNotNull() & col("category").isNotNull()
    ).select(
        col("product_id"),
        col("name"),
        col("category"),
        col("brand"),
        when(col("price") < 0, 0).otherwise(col("price")).alias("price"),
        when(col("stock_quantity") < 0, 0).otherwise(col("stock_quantity")).alias("stock_quantity"),
        when(col("rating") < 0, 0)
            .when(col("rating") > 5, 5)
            .otherwise(col("rating")).alias("rating"),
        col("is_active"),
        when(col("price") > 1000, "Premium")
            .when(col("price") > 100, "Standard")
            .otherwise("Budget").alias("price_category"),
        when(col("stock_quantity") == 0, "Out of Stock")
            .when(col("stock_quantity") < 10, "Low Stock")
            .when(col("stock_quantity") < 50, "Moderate Stock")
            .otherwise("Sufficient Stock").alias("stock_status"),
        current_timestamp().alias("last_updated")
    )

    return cleaned_df