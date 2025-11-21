CREATE TABLE IF NOT EXISTS Dim_Campaign (
    campaign_key SERIAL PRIMARY KEY,    
    campaign_id VARCHAR,                
    campaign_name VARCHAR,
    discount DECIMAL (10,2)                       
);

INSERT INTO Dim_Campaign (campaign_id, campaign_name, discount)
SELECT DISTINCT 
    campaign_id, 
    campaign_name, 
    discount::DECIMAL (10,2)
FROM staging_campaign_data;

SELECT * FROM Dim_Campaign;

SELECT * FROM staging_campaign_data

UPDATE staging_campaign_data
SET discount = SUBSTRING(discount FROM '^[0-9]+');

ALTER TABLE staging_campaign_data
ALTER COLUMN discount TYPE NUMERIC(10,2) 
USING discount::NUMERIC / 100.0;

SELECT * FROM staging_campaign_data
