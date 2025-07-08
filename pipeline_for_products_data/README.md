This folder defines all source code for the pipeline:

- `explorations`: Ad-hoc notebooks used to explore the data processed by this pipeline.
- `transformations`: All dataset definitions and transformations.
- `utilities`: Utility functions and Python modules used in this pipeline.

# Delta Live Tables Pipeline – Silver Products

This repository contains a Delta Live Tables (DLT) pipeline in Python that transforms raw product data from the bronze layer into a clean, enriched silver table.

---

## Purpose

Transform the raw product data (`bronze_products`) into a trusted silver layer (`silver_products`) for downstream analytics and reporting by:
- Filtering out invalid records
- Fixing data inconsistencies (e.g., negative prices, ratings > 5)
- Enriching with derived columns such as `price_category`, `stock_status`
- Tracking data freshness with `last_updated`

---
### Input Table: `globalretail_bronze.bronze_products`
Raw product data ingested from upstream systems (e.g., ETL, streaming).

### Output Table: `LIVE.silver_products`
Cleaned and enriched product data table, ready for BI and ML use cases.

---
## Transformation

### Field Fixes
- `price`: set to 0 if negative
- `stock_quantity`: set to 0 if negative
- `rating`: capped between 0 and 5

### Derived Columns
- `price_category`:  
  - > 1000 → Premium  
  - > 100 → Standard  
  - else → Budget
- `stock_status`:  
  - 0 → Out of Stock  
  - < 10 → Low Stock  
  - < 50 → Moderate Stock  
  - else → Sufficient Stock

### Data Quality Expectations
- `name IS NOT NULL`
- `category IS NOT NULL`
- `price >= 0`
- `rating BETWEEN 0 AND 5`
- `stock_quantity >= 0`
