DROP TABLE IF EXISTS Fact_Order_Line_Items; --pag matagal mag run drop niyo muna tas run test.sql tsaka index.

SELECT * FROM Fact_Order_Line_Items;

CREATE TABLE IF NOT EXISTS Fact_Order_Line_Items (
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

INSERT INTO Fact_Order_Line_Items (
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
WITH Unique_Products AS (
    SELECT 
        order_id, 
        product_id,
        ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY product_id) as row_id
    FROM staging_line_item_products
),
Unique_Prices AS (
    SELECT 
        order_id, 
        price, 
        quantity,
        ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY price) as row_id
    FROM staging_line_item_prices
)
SELECT 
    dd.Date_Key,
    dp.Product_Key,
    dc.Customer_Key,
    dm.Merchant_Key,
    ds.Staff_Key,
    dcam.Campaign_Key,
    
    u_price.quantity::INTEGER,
    u_price.price::DECIMAL(10,2),
    (u_price.price::DECIMAL(10,2) / NULLIF(u_price.quantity::INTEGER, 0)),
    
    sdely.delay_in_days::INTEGER

FROM Unique_Products AS u_prod
INNER JOIN Unique_Prices AS u_price 
    ON u_prod.order_id = u_price.order_id 
    AND u_prod.row_id = u_price.row_id --make sure di nag eexplode ah push ko nalang din how to check

LEFT JOIN staging_order_data AS sod 
    ON u_prod.order_id = sod.order_id
LEFT JOIN staging_order_with_merchant_data AS somd 
    ON u_prod.order_id = somd.order_id
LEFT JOIN staging_transactional_campaign_data AS stcd 
    ON u_prod.order_id = stcd.order_id
LEFT JOIN staging_order_delays AS sdely 
    ON u_prod.order_id = sdely.order_id

LEFT JOIN Dim_Date AS dd 
    ON sod.transaction_date::DATE = dd.Transaction_date
LEFT JOIN Dim_Product AS dp 
    ON u_prod.product_id = dp.product_id
LEFT JOIN Dim_Customer AS dc 
    ON sod.user_id = dc.user_id
LEFT JOIN Dim_Merchant AS dm 
    ON somd.merchant_id = dm.merchant_id
LEFT JOIN Dim_Staff AS ds 
    ON somd.staff_id = ds.staff_id
LEFT JOIN Dim_Campaign AS dcam 
    ON stcd.campaign_id = dcam.campaign_id;