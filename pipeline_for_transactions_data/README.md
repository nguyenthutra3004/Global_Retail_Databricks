This folder defines all source code for the pipeline:

- `explorations`: Ad-hoc notebooks used to explore the data processed by this pipeline.
- `transformations`: All dataset definitions and transformations.
- `utilities`: Utility functions and Python modules used in this pipeline.

# Delta Live Tables Pipeline – Silver Orders

This repository contains a Python-based Delta Live Tables (DLT) pipeline that processes raw transaction data into a clean and enriched `silver_orders` table.

---

## Purpose

Convert raw order transactions from the bronze layer into a reliable silver-level table for analytics, reporting, and downstream use cases by:
- Cleaning invalid or missing data
- Fixing negative values
- Deriving additional business logic fields like `order_status` and `last_updated`

---

## Table Overview

### Input Table: `globalretail_bronze.bronze_transactions`
Raw transactions including quantity, amount, customer, and product IDs.

### Output Table: `LIVE.silver_orders`
Structured order data including:
- `transaction_id`, `customer_id`, `product_id`
- `quantity`, `total_amount`, `transaction_date`
- `payment_method`, `store_type`, `order_status`
- `last_updated` timestamp

---

## Transformation Logic

### Data Cleansing
- Negative `quantity` or `total_amount` values are set to `0`
- Cast `transaction_date` to `DATE`
- Null checks for essential fields: `transaction_date`, `customer_id`, `product_id`

### Derived Fields
- `order_status`:  
  - If `quantity = 0` OR `total_amount = 0` → `Cancelled`  
  - Else → `Completed`
- `last_updated`: current timestamp of processing

---

## Data Quality Expectations

DLT automatically validates the following:
- `transaction_date IS NOT NULL`
- `customer_id IS NOT NULL`
- `product_id IS NOT NULL`
- `quantity >= 0`
- `total_amount >= 0`
