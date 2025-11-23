SELECT order_id, COUNT(*) as item_count
FROM staging_line_item_products
GROUP BY order_id
HAVING COUNT(*) > 1
LIMIT 1;
--lalabas yung order id na 4 counts yung item count


SELECT 
    prod.order_id,
    prod.product_name,
    pric.price
FROM staging_line_item_products AS prod
JOIN staging_line_item_prices AS pric 
    ON prod.order_id = pric.order_id
WHERE prod.order_id = '00002264-81a7-43b0-9864-f01e255ccdb2';
--makita dito if nag explode na data before 
--explains bat matagal mag run
--chat pag ganun nga nangyare


SELECT count(*) 
FROM staging_line_item_products 
WHERE order_id = '00002264-81a7-43b0-9864-f01e255ccdb2';
--dapat maging isa nalang
--tas dapat 4