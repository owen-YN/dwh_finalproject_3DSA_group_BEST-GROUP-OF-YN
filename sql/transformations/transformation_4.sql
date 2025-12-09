ALTER TABLE staging_transactional_campaign_data
ADD COLUMN IF NOT EXISTS "Year" INTEGER,
ADD COLUMN IF NOT EXISTS "Month" INTEGER,
ADD COLUMN IF NOT EXISTS "Day_Val" INTEGER;

-- This part is fine to run multiple times
ALTER TABLE staging_transactional_campaign_data
ALTER COLUMN transaction_date TYPE DATE
USING transaction_date::DATE;

UPDATE staging_transactional_campaign_data
SET 
	"Year" = EXTRACT(YEAR FROM transaction_date::DATE),
    "Month" = EXTRACT(MONTH FROM transaction_date::DATE),
    "Day_Val" = EXTRACT(DAY FROM transaction_date::DATE);

