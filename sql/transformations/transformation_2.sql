-- FIX: Cast input to TEXT for regex, and output back to INTEGER for storage
UPDATE staging_line_item_prices
SET quantity = SUBSTRING(quantity::TEXT FROM '[0-9]+')::INTEGER;

ALTER TABLE staging_line_item_prices
ALTER COLUMN quantity TYPE INTEGER 
USING quantity::INTEGER;