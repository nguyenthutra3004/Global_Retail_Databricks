This folder defines all source code for the pipeline:

- `explorations`: Ad-hoc notebooks used to explore the data processed by this pipeline.
- `transformations`: All dataset definitions and transformations.
- `utilities`: Utility functions and Python modules used in this pipeline.

# Delta Live Tables Pipeline - Silver Customers

This repository/notebook contains a Delta Live Tables (DLT) pipeline that processes raw customer data from the `globalretail_bronze.bronze_customer` table and generates a cleaned, enriched `silver_customers` table for analytics.

## Purpose

Transform raw bronze customer data into a well-structured silver table by:
- Filtering invalid or incomplete records
- Enriching data with customer segments
- Adding calculated fields such as `days_since_registration` and `last_updated`

---
### Input Table: `globalretail_bronze.bronze_customer`
Raw customer data ingested from the bronze layer.

### Output Table: `LIVE.silver_customers`
Processed silver-level table with the following fields:
- `customer_id`, `name`, `email`, `country`, `customer_type`
- `registration_date`, `age`, `gender`, `total_purchases`
- `customer_segment` (derived)
- `days_since_registration` (calculated)
- `last_updated` (timestamp)

---
## Processing Logic

1. **Filter Conditions**
   - Age must be between 18 and 100
   - Email must not be null
   - Total purchases must be non-negative

2. **Derived Fields**
   - `customer_segment`: categorized into `High Value`, `Medium Value`, or `Low Value`
   - `days_since_registration`: calculated from `registration_date`
   - `last_updated`: current system timestamp

3. **Data Quality Constraints (via `@dlt.expect`)**
   - Valid email
   - Valid age range
   - Non-negative purchases
---
