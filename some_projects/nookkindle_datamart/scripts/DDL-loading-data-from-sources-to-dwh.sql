/* создание таблицы tmp_sources с данными из всех источников */
DROP TABLE IF EXISTS tmp_sources;
CREATE TEMP TABLE tmp_sources AS 
SELECT  order_id,
        order_created_date,
        order_completion_date,
        order_status,
        provider_id,
        provider_name,
        provider_address,
        provider_birthday,
        provider_email,
        product_id,
        product_name,
        product_description,
        product_type,
        product_price,
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email 
  FROM source1.provider_market_wide
UNION
SELECT  t2.order_id,
        t2.order_created_date,
        t2.order_completion_date,
        t2.order_status,
        t1.provider_id,
        t1.provider_name,
        t1.provider_address,
        t1.provider_birthday,
        t1.provider_email,
        t1.product_id,
        t1.product_name,
        t1.product_description,
        t1.product_type,
        t1.product_price,
        t2.customer_id,
        t2.customer_name,
        t2.customer_address,
        t2.customer_birthday,
        t2.customer_email 
  FROM source2.provider_market_masters_products t1 
    JOIN source2.provider_market_orders_customers t2 ON t2.product_id = t1.product_id and t1.provider_id = t2.provider_id 
UNION
SELECT  t1.order_id,
        t1.order_created_date,
        t1.order_completion_date,
        t1.order_status,
        t2.provider_id,
        t2.provider_name,
        t2.provider_address,
        t2.provider_birthday,
        t2.provider_email,
        t1.product_id,
        t1.product_name,
        t1.product_description,
        t1.product_type,
        t1.product_price,
        t3.customer_id,
        t3.customer_name,
        t3.customer_address,
        t3.customer_birthday,
        t3.customer_email
  FROM source3.provider_market_orders t1
    JOIN source3.provider_market_providers t2 ON t1.provider_id = t2.provider_id 
    JOIN source3.provider_market_customers t3 ON t1.customer_id = t3.customer_id
union
select t1.order_id,
	   t1.order_created_date,
	   t1.order_completion_date,
	   t1.order_status,
	   t1.provider_id,
	   t1.provider_name,
	   t1.provider_address,
	   t1.provider_birthday,
	   t1.provider_email,
	   t1.product_id,
	   t1.product_name,
       t1.product_description,
       t1.product_type,
       t1.product_price,
       t2.customer_id,
       t2.customer_name,
       t2.customer_address,
       t2.customer_birthday,
       t2.customer_email  
  from external_source.provider_products_orders t1
    join external_source.customers t2 on t1.customer_id = t2.customer_id;

/* обновление существующих записей и добавление новых в dwh.d_providers */
MERGE INTO dwh.d_provider d
USING (SELECT DISTINCT provider_name, provider_address, provider_birthday, provider_email FROM tmp_sources) t
ON d.provider_name = t.provider_name AND d.provider_email = t.provider_email
WHEN MATCHED THEN
  UPDATE SET provider_address = t.provider_address, 
provider_birthday = t.provider_birthday, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (provider_name, provider_address, provider_birthday, provider_email, load_dttm)
  VALUES (t.provider_name, t.provider_address, t.provider_birthday, t.provider_email, current_timestamp);

/* обновление существующих записей и добавление новых в dwh.d_products */
MERGE INTO dwh.d_product d
USING (SELECT DISTINCT product_name, product_description, product_type, product_price from tmp_sources) t
ON d.product_name = t.product_name AND d.product_description = t.product_description AND d.product_price = t.product_price
WHEN MATCHED THEN
  UPDATE SET product_type= t.product_type, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_name, product_description, product_type, product_price, load_dttm)
  VALUES (t.product_name, t.product_description, t.product_type, t.product_price, current_timestamp);

/* обновление существующих записей и добавление новых в dwh.d_customer */
MERGE INTO dwh.d_customer d
USING (SELECT DISTINCT customer_name, customer_address, customer_birthday, customer_email from tmp_sources) t
ON d.customer_name = t.customer_name AND d.customer_email = t.customer_email
WHEN MATCHED THEN
  UPDATE SET customer_address= t.customer_address, 
customer_birthday= t.customer_birthday, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (customer_name, customer_address, customer_birthday, customer_email, load_dttm)
  VALUES (t.customer_name, t.customer_address, t.customer_birthday, t.customer_email, current_timestamp);

/* создание таблицы tmp_sources_fact */
DROP TABLE IF EXISTS tmp_sources_fact;
CREATE TEMP TABLE tmp_sources_fact AS 
SELECT  dp.product_id,
        dc.provider_id,
        dcust.customer_id,
        src.order_created_date,
        src.order_completion_date,
        src.order_status,
        current_timestamp 
FROM tmp_sources src
JOIN dwh.d_provider dc ON dc.provider_name = src.provider_name and dc.provider_email = src.provider_email 
JOIN dwh.d_customer dcust ON dcust.customer_name = src.customer_name and dcust.customer_email = src.customer_email 
JOIN dwh.d_product dp ON dp.product_name = src.product_name and dp.product_description = src.product_description and dp.product_price = src.product_price;

/* обновление существующих записей и добавление новых в dwh.f_order */
MERGE INTO dwh.f_order f
USING tmp_sources_fact t
ON f.product_id = t.product_id AND f.provider_id = t.provider_id AND f.customer_id = t.customer_id AND f.order_created_date = t.order_created_date 
WHEN MATCHED THEN
  UPDATE SET order_completion_date = t.order_completion_date, order_status = t.order_status, load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_id, provider_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm)
  VALUES (t.product_id, t.provider_id, t.customer_id, t.order_created_date, t.order_completion_date, t.order_status, current_timestamp);