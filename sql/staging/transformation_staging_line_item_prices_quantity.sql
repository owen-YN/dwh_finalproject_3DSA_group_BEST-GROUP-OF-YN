SELECT 
    quantity AS original,
    SUBSTRING(quantity FROM '^[0-9]+') AS clean_number
FROM staging_line_item_prices;
--run niyo muna to double check bago ichange permanently. magiging clean number talaga yung column


UPDATE staging_line_item_prices
SET quantity = SUBSTRING(quantity FROM '[0-9]+');

ALTER TABLE staging_line_item_prices
ALTER COLUMN quantity TYPE INTEGER 
USING quantity::INTEGER;

SELECT * FROM staging_line_item_prices;

SELECT * FROM staging_line_item_prices 
WHERE quantity::TEXT ~ '[^0-9]';
-- dapat 0 rows mareturn neto