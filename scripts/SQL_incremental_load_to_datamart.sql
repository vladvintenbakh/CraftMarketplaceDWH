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
)
SELECT 'increment datamart';
