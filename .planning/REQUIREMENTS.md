# Requirements - Ecommerce Pipeline Implementation

## 1. Data Ingestion
- Support for `ecommerce_sales_34500.csv`.
- Header mapping for:
    - `order_id` -> `order_id`
    - `order_date` -> `order_date`
    - `customer_id` -> `customer.name` (as placeholder or update model)
    - `product_id` -> `product.name`
    - `category` -> `product.category`
    - `price` -> `unit_price`
    - `discount` -> `discount`
    - `quantity` -> `quantity`
    - `total_amount` -> `total_sales`
    - `profit_margin` -> `profit`
    - `payment_method` -> `payment_mode`
    - `region` -> `region`

## 2. Model Extensions
- Update `Customer` model to include:
    - `age` (Integer)
    - `gender` (String/Choice)
- Update `Sale` model to include:
    - `delivery_time_days` (Integer)
    - `returned` (Boolean)
    - `shipping_cost` (Decimal)

## 3. ETL Pipeline Updates
- Enhance `_auto_map_columns` with new aliases.
- Add support for new fields in `clean_data` and `feature_engineering`.
- Update `load_to_db` to handle new model fields.

## 4. Verification
- Management command `python manage.py ingest_ecommerce --file ../ecommerce_sales_34500.csv` must run successfully.
- Database should contain 34,500 records (minus duplicates/invalid rows).
