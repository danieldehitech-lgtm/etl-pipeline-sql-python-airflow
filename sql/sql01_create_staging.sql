CREATE TABLE stg_orders (
  order_id INT,
  customer_id INT,
  order_total DECIMAL(12,2),
  order_status VARCHAR(30),
  created_at TIMESTAMP
);
