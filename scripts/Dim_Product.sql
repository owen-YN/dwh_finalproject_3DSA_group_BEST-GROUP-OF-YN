CREATE TABLE IF NOT EXISTS Dim_Product (
    Product_Key SERIAL PRIMARY KEY,       
    product_id VARCHAR(255),              
    product_name VARCHAR(255),            
    product_type VARCHAR(255),           
    price DECIMAL(10, 2)                  
);

INSERT INTO Dim_Product (product_id, product_name, product_type, price)
SELECT DISTINCT 
    product_id, 
    product_name, 
    product_type, 
    price::DECIMAL(10,2)       
FROM staging_product_list;