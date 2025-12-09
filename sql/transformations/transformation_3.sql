ALTER TABLE staging_order_data
ADD COLUMN IF NOT EXISTS "Year" INTEGER,
ADD COLUMN IF NOT EXISTS "Month" INTEGER,
ADD COLUMN IF NOT EXISTS "Day_Val" INTEGER;

ALTER TABLE staging_order_data
ALTER COLUMN transaction_date TYPE DATE 
USING transaction_date::DATE;

UPDATE staging_order_data
SET 
    "Year" = EXTRACT(YEAR FROM transaction_date::DATE),
    "Month" = EXTRACT(MONTH FROM transaction_date::DATE),
    "Day_Val" = EXTRACT(DAY FROM transaction_date::DATE);