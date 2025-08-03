/* COMBINE SOURCE TABLES */

DROP TABLE IF EXISTS tmp_source;

CREATE TEMP TABLE tmp_source AS 
(
	SELECT
	    cpo.order_id,
	    cpo.order_created_date,
	    cpo.order_completion_date,
	    cpo.order_status,
	    
	    cpo.craftsman_id,
	    cpo.craftsman_name,
	    cpo.craftsman_address,
	    cpo.craftsman_birthday,
	    cpo.craftsman_email,
	    
	    cpo.product_id,
	    cpo.product_name,
	    cpo.product_description,
	    cpo.product_type,
	    cpo.product_price,
	    
	    cust.customer_id,
	    cust.customer_name,
	    cust.customer_address,
	    cust.customer_birthday,
	    cust.customer_email
	FROM external_source.craft_products_orders cpo
	JOIN external_source.customers cust
	USING(customer_id)
);

/* UPDATE THE DIMENSIONS */

MERGE INTO dwh.d_craftsman d
USING (SELECT DISTINCT craftsman_name, craftsman_address, craftsman_birthday, craftsman_email FROM tmp_source) t
ON d.craftsman_name = t.craftsman_name AND d.craftsman_email = t.craftsman_email
WHEN MATCHED THEN
  UPDATE SET craftsman_address = t.craftsman_address, craftsman_birthday = t.craftsman_birthday, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
  VALUES (t.craftsman_name, t.craftsman_address, t.craftsman_birthday, t.craftsman_email, current_timestamp);


MERGE INTO dwh.d_product d
USING (SELECT DISTINCT product_name, product_description, product_type, product_price FROM tmp_source) t
ON d.product_name = t.product_name AND d.product_description = t.product_description AND d.product_price = t.product_price
WHEN MATCHED THEN
  UPDATE SET product_type = t.product_type, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_name, product_description, product_type, product_price, load_dttm)
  VALUES (t.product_name, t.product_description, t.product_type, t.product_price, current_timestamp);


MERGE INTO dwh.d_customer d
USING (SELECT DISTINCT customer_name, customer_address, customer_birthday, customer_email FROM tmp_source) t
ON d.customer_name = t.customer_name AND d.customer_email = t.customer_email
WHEN MATCHED THEN
  UPDATE SET customer_address = t.customer_address, customer_birthday = t.customer_birthday, load_dttm = current_timestamp
WHEN NOT MATCHED then
	INSERT (customer_name, customer_address, customer_birthday, customer_email, load_dttm)
	VALUES (t.customer_name, t.customer_address, t.customer_birthday, t.customer_email, current_timestamp);

/* UPDATE THE FACT TABLE */

DROP TABLE IF EXISTS tmp_source_fact;

CREATE TEMP TABLE tmp_source_fact AS 
SELECT 
	dp.product_id,
	dc.craftsman_id,
	dcust.customer_id,
	src.order_created_date,
	src.order_completion_date,
	src.order_status,
	current_timestamp 
FROM tmp_source src
JOIN dwh.d_craftsman dc ON dc.craftsman_name = src.craftsman_name AND dc.craftsman_email = src.craftsman_email 
JOIN dwh.d_customer dcust ON dcust.customer_name = src.customer_name AND dcust.customer_email = src.customer_email 
JOIN dwh.d_product dp ON dp.product_name = src.product_name AND dp.product_description = src.product_description AND dp.product_price = src.product_price;


MERGE INTO dwh.f_order f
USING tmp_source_fact t
ON f.product_id = t.product_id AND f.craftsman_id = t.craftsman_id AND f.customer_id = t.customer_id AND f.order_created_date = t.order_created_date 
WHEN MATCHED THEN
  UPDATE SET order_completion_date = t.order_completion_date, order_status = t.order_status, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm)
  VALUES (t.product_id, t.craftsman_id, t.customer_id, t.order_created_date, t.order_completion_date, t.order_status, current_timestamp);
