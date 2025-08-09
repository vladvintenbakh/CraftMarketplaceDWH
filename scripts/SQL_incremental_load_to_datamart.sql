WITH
dwh_delta AS (
    SELECT     
	    dcs.customer_id AS customer_id,
	    dcs.customer_name AS customer_name,
	    dcs.customer_address AS customer_address,
	    dcs.customer_birthday AS customer_birthday,
	    dcs.customer_email AS customer_email,
	    fo.order_id AS order_id,
	    dp.product_id AS product_id,
		dp.product_type AS product_type,
	    dp.product_price AS product_price,
		dc.craftsman_id AS craftsman_id,
	    fo.order_completion_date - fo.order_created_date AS diff_order_date, 
	    fo.order_status AS order_status,
	    TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
	    crd.customer_id AS exist_customer_id,
	    dc.load_dttm AS craftsman_load_dttm,
	    dcs.load_dttm AS customers_load_dttm,
	    dp.load_dttm AS products_load_dttm
    FROM dwh.f_order fo 
    INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id 
    INNER JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id 
    INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id 
    LEFT JOIN dwh.customer_report_datamart crd ON dcs.customer_id = crd.customer_id
    WHERE 
        (fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
        (dc.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
        (dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
        (dp.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart))
),
dwh_update_delta AS (
    SELECT     
    	dd.exist_customer_id AS customer_id
    FROM dwh_delta dd 
    WHERE dd.exist_customer_id IS NOT NULL        
),
dwh_delta_insert_result AS (
    SELECT  
	    T4.customer_id AS customer_id,
	    T4.customer_name AS customer_name,
	    T4.customer_address AS customer_address,
	    T4.customer_birthday AS customer_birthday,
	    T4.customer_email AS customer_email,
	    T4.customer_spend AS customer_spend,
	    T4.platform_money AS platform_money,
	    T4.count_order AS count_order,
	    T4.avg_price_order AS avg_price_order,
	    T4.median_time_order_completed AS median_time_order_completed,
	    T4.count_order_created AS count_order_created,
	    T4.count_order_in_progress AS count_order_in_progress,
	    T4.count_order_delivery AS count_order_delivery,
	    T4.count_order_done AS count_order_done,
	    T4.count_order_not_done AS count_order_not_done,
	    T4.report_period AS report_period 
	FROM (
    	SELECT
	        *,
	        RANK() OVER (PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_count_product,
	        ROW_NUMBER() OVER (PARTITION BY T2.customer_id ORDER BY count_order_per_craftsman DESC) AS rn_count_order_per_craftsman,
	        FROM ( 
	            SELECT
	                T1.customer_id AS customer_id,
	                T1.customer_name AS customer_name,
	                T1.customer_address AS customer_address,
	                T1.customer_birthday AS customer_birthday,
	                T1.customer_email AS customer_email,
	                SUM(T1.product_price) AS customer_spend,
	                SUM(T1.product_price) * 0.1 AS platform_money,
	                COUNT(order_id) AS count_order,
	                AVG(T1.product_price) AS avg_price_order,
	                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,
	                SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
	                SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
	                SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
	                SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
	                SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
	                T1.report_period AS report_period
                FROM dwh_delta AS T1
                WHERE T1.exist_customer_id IS NULL
                GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period
                ) AS T2 
                INNER JOIN (
                	SELECT
                        dd.customer_id AS customer_id_for_product_type, 
                        dd.product_type, 
                        COUNT(dd.product_id) AS count_product
                    FROM dwh_delta AS dd
                    GROUP BY dd.customer_id, dd.product_type
                    ORDER BY count_product DESC
                ) AS T3 ON T2.customer_id = T3.customer_id_for_product_type
                INNER JOIN (
                	SELECT
                		dd.customer_id AS customer_id_for_craftsman_id,
                		dd.craftsman_id,
                		COUNT(dd.order_id) AS count_order_per_craftsman
            		FROM dwh_delta AS dd
            		GROUP BY dd.customer_id, dd.craftsman_id
            		ORDER BY count_order_per_craftsman DESC
                ) AS T3_2 ON T2.customer_id = T3_2.customer_id_for_craftsman_id
    ) AS T4
    WHERE T4.rank_count_product = 1 AND T4.rn_count_order_per_craftsman = 1
    ORDER BY report_period
)
SELECT 'increment datamart';
