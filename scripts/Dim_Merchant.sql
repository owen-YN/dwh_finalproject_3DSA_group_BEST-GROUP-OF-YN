CREATE TABLE IF NOT EXISTS Dim_Merchant (
    Merchant_Key SERIAL PRIMARY KEY,        
    merchant_id VARCHAR,                
    merchant_name VARCHAR,                    
    merchant_street VARCHAR, 
    merchant_state VARCHAR,          
    merchant_city VARCHAR,                         
    merchant_country VARCHAR,
    merchant_contant_number VARCHAR
);

INSERT INTO Dim_Merchant (
    merchant_id, 
    merchant_name, 
    merchant_street, 
    merchant_state, 
    merchant_city, 
    merchant_country,
    merchant_contant_number
)

SELECT DISTINCT 
    merchant_id,
    merchant_name,
    merchant_street,
    merchant_state,
    merchant_city,
    merchant_country,
    merchant_contact_number TEXT     
FROM staging_merchant_data;