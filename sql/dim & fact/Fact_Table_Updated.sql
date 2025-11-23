-- RUN IN CHUNKS, DO NOT RUN WHOLE FILE AS SINGLE QUERY

-- Indexes on all Dimensions
CREATE INDEX IF NOT EXISTS idx_dim_prod_id ON Dim_Product(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_cust_id ON Dim_Customer(user_id);
CREATE INDEX IF NOT EXISTS idx_dim_merch_id ON Dim_Merchant(merchant_id);
CREATE INDEX IF NOT EXISTS idx_dim_staff_id ON Dim_Staff(staff_id);
CREATE INDEX IF NOT EXISTS idx_dim_camp_id ON Dim_Campaign(campaign_id);
CREATE INDEX IF NOT EXISTS idx_dim_date_val ON Dim_Date(Full_Date);
CREATE INDEX IF NOT EXISTS idx_dim_order_id ON Dim_Order(Order_ID);


-- Create Fact Table
DROP TABLE IF EXISTS Fact_Order_Line_Items CASCADE;
CREATE TABLE Fact_Order_Line_Items (
    Order_Key INTEGER,        
    Date_Key INTEGER,
    Product_Key INTEGER,
    Customer_Key INTEGER,
    Merchant_Key INTEGER,
    Staff_Key INTEGER,
    Campaign_Key INTEGER,

    quantity INTEGER,
    total_price DECIMAL(10, 2),
    unit_price DECIMAL(10, 2),
    delay_in_days INTEGER
);

-- Create a Temporary "Base" Table for the Zipper Join (index file)
DROP TABLE IF EXISTS temp_fact_base;
CREATE TEMP TABLE temp_fact_base AS
WITH Prod_Rows AS (
    SELECT 
        order_id, 
        product_id,
        ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY product_id) as rn
    FROM staging_line_item_products
),
Price_Rows AS (
    SELECT 
        order_id, 
        price, 
        quantity,
        ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY price) as rn
    FROM staging_line_item_prices
)
SELECT 
    p.order_id,
    p.product_id,
    pr.price,
    pr.quantity
FROM Prod_Rows p
JOIN Price_Rows pr ON p.order_id = pr.order_id AND p.rn = pr.rn;

-- Index the temp table
CREATE INDEX idx_temp_base_order ON temp_fact_base(order_id);
CREATE INDEX idx_temp_base_product ON temp_fact_base(product_id);


-- Insert into Fact Table 
INSERT INTO Fact_Order_Line_Items (
    Order_Key,               
    Date_Key,
    Product_Key,
    Customer_Key,
    Merchant_Key,
    Staff_Key,
    Campaign_Key,
    quantity,
    total_price,
    unit_price,
    delay_in_days
)
SELECT
    -- Order Key
    COALESCE(dord.Order_Key, -1),

    -- Date Key
    COALESCE(dd.Date_Key, -1),

    -- Product Key
    COALESCE(dp.Product_Key, -1),

    -- Customer Key
    COALESCE(dc.Customer_Key, -1),

    -- Merchant Key
    COALESCE(dm.Merchant_Key, -1),

    -- Staff Key 
    COALESCE(ds.Staff_Key, -1),

    -- Campaign Key 
    COALESCE(dcam.Campaign_Key, -1), 
	-- EX:
	-- if certain tables return -1, kunwari sa campaign key sa fact table:
	-- that just means the order was not from any campaign. same logic goes
	-- for the other cols that return -1, working as intended.

    -- Measures for Fact Table
    REGEXP_REPLACE(base.quantity::TEXT, '[^0-9]', '', 'g')::INTEGER as quantity,
    base.price::DECIMAL(10,2) as total_price,
    CASE 
        WHEN REGEXP_REPLACE(base.quantity::TEXT, '[^0-9]', '', 'g')::INTEGER = 0 THEN 0
        ELSE (base.price::DECIMAL(10,2) / REGEXP_REPLACE(base.quantity::TEXT, '[^0-9]', '', 'g')::INTEGER)
    END as unit_price,
    COALESCE(sdely.delay_in_days::INTEGER, 0)

FROM temp_fact_base AS base

-- Join Staging Tables
LEFT JOIN staging_order_data AS sod ON base.order_id = sod.order_id
LEFT JOIN staging_order_with_merchant_data AS somd ON base.order_id = somd.order_id
LEFT JOIN staging_transactional_campaign_data AS stcd ON base.order_id = stcd.order_id
LEFT JOIN staging_order_delays AS sdely ON base.order_id = sdely.order_id

-- Join Dimensions
LEFT JOIN Dim_Date AS dd ON sod.transaction_date::DATE = dd.Full_Date
LEFT JOIN Dim_Product AS dp ON base.product_id = dp.Product_ID
LEFT JOIN Dim_Customer AS dc ON sod.user_id = dc.User_ID
LEFT JOIN Dim_Merchant AS dm ON somd.merchant_id = dm.Merchant_ID
LEFT JOIN Dim_Staff AS ds ON somd.staff_id = ds.Staff_ID
LEFT JOIN Dim_Campaign AS dcam ON stcd.campaign_id = dcam.Campaign_ID
LEFT JOIN Dim_Order AS dord ON base.order_id = dord.Order_ID;

-- delete temp table to free space
DROP TABLE IF EXISTS temp_fact_base;

-- View the fact table pero in order_key asc view para readable 
SELECT * FROM fact_order_line_items 
ORDER BY order_key ASC 
LIMIT 600;
-- bahala na kayo mag set ng limit, DO NOT select without limit
-- 4M+ ang rows nito, di kaya lahat

SELECT COUNT(*) FROM fact_order_line_items;
-- should be 4536177