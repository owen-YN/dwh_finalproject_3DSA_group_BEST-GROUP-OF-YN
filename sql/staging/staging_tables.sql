-- INT STAGING LAYER ("basket" nung ingested data natin)

-- All columns are in TEXT format to prevent ingestion errors (like "6px").
-- We will clean and transform the data sa transformation step (Task 4).

-- From Business Dept
CREATE TABLE IF NOT EXISTS staging_product_list (
    product_id TEXT, 
    product_name TEXT,
    product_type TEXT,
    price TEXT
);

-- From Customer Management
CREATE TABLE IF NOT EXISTS staging_user_data (
    user_id TEXT,
    creation_date TEXT,
	name TEXT,
	street TEXT,
	state TEXT,
	city TEXT, 
	country TEXT, 
	birthdate TEXT,
	gender TEXT,
	device_address TEXT,
	user_type TEXT
);

CREATE TABLE IF NOT EXISTS staging_user_job (
    user_id TEXT,
    name TEXT,
    job_title TEXT,
    job_level TEXT
);

CREATE TABLE IF NOT EXISTS staging_user_credit_card (
    user_id TEXT,
	name TEXT,
    credit_card_number TEXT,
	issuing_bank TEXT
);

-- From Enterprise
CREATE TABLE IF NOT EXISTS staging_merchant_data (
    merchant_id TEXT,
    creation_date TEXT,
    name TEXT,
    street TEXT,
    state TEXT,
    city TEXT,
    country TEXT,
    contact_number TEXT
);

CREATE TABLE IF NOT EXISTS staging_staff_data (
    staff_id TEXT,
    name TEXT,
    job_level TEXT,
    street TEXT,
    state TEXT,
    city TEXT,
    country TEXT,
    contact_number TEXT,
    creation_date TEXT
);

-- This ONE table will hold the UNION of all 3 'order_with_merchant' files
CREATE TABLE IF NOT EXISTS staging_order_with_merchant_data (
    order_id TEXT,
    merchant_id TEXT,
    staff_id TEXT
);

-- From Marketing
CREATE TABLE IF NOT EXISTS staging_campaign_data (
    campaign_id TEXT,
    campaign_name TEXT,
    campaign_description TEXT,
    discount TEXT
);

CREATE TABLE IF NOT EXISTS staging_transactional_campaign_data (
    transaction_date TEXT,
    campaign_id TEXT,
    order_id TEXT,
    estimated_arrival TEXT,
    availed TEXT
);

-- From Operations
-- This ONE table will hold the UNION of all 6 'order_data' files
CREATE TABLE IF NOT EXISTS staging_order_data (
    order_id TEXT,
    user_id TEXT,
    estimated_arrival TEXT,
    transaction_date TEXT
);

-- This ONE table will hold the UNION of all 3 'line_item_data_prices' files
CREATE TABLE IF NOT EXISTS staging_line_item_prices (
    order_id TEXT,
    price TEXT,
    quantity TEXT
);

-- This ONE table will hold the UNION of all 3 'line_item_data_products' files
CREATE TABLE IF NOT EXISTS staging_line_item_products (
    order_id TEXT,
    product_name TEXT,
    product_id TEXT
);

CREATE TABLE IF NOT EXISTS staging_order_delays (
    order_id TEXT,
    delay_in_days TEXT
);

-- data quality check table
-- landing pad of any schema drift (unexpected columns) found during ingestion
-- 
CREATE TABLE IF NOT EXISTS audit_schema_drift (
    drift_id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name TEXT,
    file_name TEXT,
    unknown_column_name TEXT,
    action_taken TEXT DEFAULT 'Dropped Column'
);