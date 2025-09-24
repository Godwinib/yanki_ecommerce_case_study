-- Window Functions:
-- Calculate the total sales amount from each order along with the individual product sales
SELECT
    o.order_id,
    p.product_id,
    p.price,
    o.quantity,
    o.total_price,
    SUM(p.price * o.quantity) OVER (PARTITION BY o.order_id) AS total_sales_amount
FROM 
    yanki.order o
JOIN
    yanki.products p ON o.product_id = p.product_id;
	