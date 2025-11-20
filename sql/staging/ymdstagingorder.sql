SELECT * FROM staging_order_data;


ALTER TABLE staging_order_data
ADD COLUMN "Year" INTEGER,
ADD COLUMN "Month" INTEGER,
ADD COLUMN "Day_Val" INTEGER;

ALTER TABLE staging_order_data
ALTER COLUMN transaction_date TYPE DATE 
USING transaction_date::DATE;

UPDATE staging_order_data
SET 
    "Year" = EXTRACT(YEAR FROM transaction_date::DATE),
    "Month" = EXTRACT(MONTH FROM transaction_date::DATE),
    "Day_Val" = EXTRACT(DAY FROM transaction_date::DATE);