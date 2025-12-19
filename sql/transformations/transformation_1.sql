-- FIX: Cast discount to TEXT for regex, and output back to NUMERIC for storage
UPDATE staging_campaign_data
SET discount = (regexp_match(discount::TEXT, '([0-9]+(\.[0-9]+)?)'))[1]::NUMERIC;

ALTER TABLE staging_campaign_data
ALTER COLUMN discount TYPE NUMERIC(10,2) 
USING discount::NUMERIC / 100.0;

-- discount is in percent, may seem less intuitive but more 
-- applicable for calculations sa BI layer