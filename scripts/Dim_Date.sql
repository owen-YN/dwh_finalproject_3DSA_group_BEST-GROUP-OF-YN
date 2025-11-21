CREATE TABLE IF NOT EXISTS Dim_Date (
    Date_Key SERIAL PRIMARY KEY,    
    Transaction_date DATE,                
    estimated_arrival VARCHAR,
    availed BOOLEAN,
    Year INT,
    Month INT,
    Day_Val INT
);


INSERT INTO Dim_Date (Transaction_date, estimated_arrival, availed, Year, Month, Day_Val)
SELECT DISTINCT 
    Transaction_date,                
    estimated_arrival,
    availed::BOOLEAN,
    Year,
    Month,
    Day_Val
FROM staging_transactional_campaign_data;
