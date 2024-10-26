DROP TABLE IF EXISTS mart.f_customer_retention;
CREATE TABLE IF NOT EXISTS mart.f_customer_retention
    (new_customers_count INT NOT NULL,
    returning_customers_count INT NOT NULL,
    refunded_customer_count INT NOT NULL,
    period_name VARCHAR(20) NOT NULL,
    period_id INT NOT NULL,
    item_id INT NOT NULL,
    new_customers_revenue NUMERIC(14,2) NOT NULL,
    returning_customers_revenue NUMERIC(14,2) NOT NULL,
    customers_refunded INT NOT NULL);


