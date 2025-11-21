CREATE TABLE IF NOT EXISTS Dim_Customer (
    Customer_Key SERIAL PRIMARY KEY,      -- Surrogate Key
    user_id VARCHAR,                  -- Natural Key
    customer_name VARCHAR,
    job_title VARCHAR,
    job_level VARCHAR,
    credit_card_cumber VARCHAR,
    issuing_bank VARCHAR,
    customer_gender VARCHAR,          -- Renamed
    customer_street VARCHAR,         -- Renamed
    customer_state VARCHAR,          -- Renamed
    customer_city VARCHAR,           -- Renamed
    customer_country VARCHAR,        -- Renamed
    customer_user_type VARCHAR        -- Renamed
);

INSERT INTO Dim_Customer (
    user_id, 
    customer_name, 
    job_title, 
    job_level, 
    credit_card_number, 
    issuing_bank, 
    customer_gender, 
    customer_street, 
    customer_state, 
    customer_city, 
    customer_country, 
    customer_user_type
)
SELECT DISTINCT
    sd.user_id,                -- From User Data
    sd.name,                   -- From User Data
    sj.job_title,              -- From Job Table
    sj.job_level,              -- From Job Table
    scc.credit_card_number,    -- From Credit Card Table
    scc.issuing_bank,          -- From Credit Card Table
    sd.gender,                 -- Renaming to Customer_Gender
    sd.street,                 -- Renaming to Customer_Street
    sd.state,                  -- Renaming to Customer_State
    sd.city,                   -- Renaming to Customer_City
    sd.country,                -- Renaming to Customer_Country
    sd.user_type               -- Renaming to Customer_User_type
FROM staging_user_data AS sd
LEFT JOIN staging_user_job AS sj 
    ON sd.user_id = sj.user_id
LEFT JOIN staging_user_credit_card AS scc 
    ON sd.user_id = scc.user_id;
