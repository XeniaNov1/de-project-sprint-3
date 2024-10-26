INSERT INTO mart.f_customer_retention (new_customers_count,returning_customers_count,refunded_customer_count,period_name,period_id,item_id,new_customers_revenue,returning_customers_revenue,customers_refunded)
    WITH new_customers AS (
        SELECT SUM(payment_amount) AS new_payment,
                customer_id,
                date_id
            FROM mart.f_sales
        GROUP BY customer_id, date_id
        HAVING COUNT(item_id)=1 AND SUM(payment_amount)>0
    ),
    old_customers AS (
       SELECT SUM(payment_amount) AS old_payment,
                customer_id,
                date_id
            FROM mart.f_sales
        GROUP BY customer_id, date_id
        HAVING COUNT(item_id)>1 AND SUM(payment_amount)>0
    ),
    refunded_customers AS (
        SELECT COUNT(payment_amount) AS count_refunded,
                date_id,
                customer_id
            FROM mart.f_sales
        WHERE payment_amount<0
        GROUP BY customer_id, date_id
    )
    SELECT  COUNT(nc.customer_id) AS new_customers_count,
            COUNT(oc.customer_id) AS returning_customers_count,
            COUNT(rc.customer_id) AS refunded_customer_count,
            'weekly' AS period_name,
            dc.week_of_year AS period_id,
            fs.item_id AS item_id,
            SUM(nc.new_payment) AS new_customers_revenue,
            SUM(oc.old_payment) AS returning_customers_revenue,
            COUNT(rc.count_refunded) AS customers_refunded
        FROM mart.f_sales fs 
        LEFT JOIN new_customers nc ON fs.date_id=nc.date_id
        LEFT JOIN old_customers oc ON fs.date_id=oc.date_id
        LEFT JOIN refunded_customers rc ON fs.date_id=rc.date_id
        LEFT JOIN mart.d_calendar dc ON fs.date_id=dc.date_id
        GROUP BY dc.week_of_year,fs.item_id;