DROP TABLE IF EXISTS dwh.customer_report_datamart;

CREATE TABLE IF NOT EXISTS dwh.customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор записи
    customer_id BIGINT NOT NULL, -- идентификатор покупателя
    customer_name VARCHAR NOT NULL, -- Ф. И. О. покупателя
    customer_address VARCHAR NOT NULL, -- адрес покупателя
    customer_birthday DATE NOT NULL, -- дата рождения покупателя
    customer_email VARCHAR NOT NULL, -- электронная почта покупателя
    customer_money NUMERIC(15,2) NOT NULL, -- сумма денег, которую потратил покупатель за месяц 
    shop_money BIGINT NOT NULL, -- сумма денег, которая заработал магазин от покупок покупателя за месяц
    count_order BIGINT NOT NULL, -- количество заказов у покупателя за месяц
    avg_price_order NUMERIC(10,2) NOT NULL, -- средняя стоимость одного заказа у покупателя за месяц
    median_time_order_completed NUMERIC(10,1), -- медианное время в днях от момента создания заказа до его завершения за месяц
    top_product_category VARCHAR NOT NULL, -- самая популярная категория товаров у этого покупателя за месяц
    top_provider_id BIGINT NOT NULL, -- идентификатор самого популярного поставщика у покупателя
    count_order_created BIGINT NOT NULL, -- количество созданных заказов за месяц
    count_order_in_progress BIGINT NOT NULL, -- количество заказов в процессе сборки за месяц
    count_order_delivery BIGINT NOT NULL, -- количество заказов в доставке за месяц
    count_order_done BIGINT NOT NULL, -- количество завершённых заказов за месяц
    count_order_not_done BIGINT NOT NULL, -- количество незавершённых заказов за месяц
    report_period VARCHAR NOT NULL, -- отчётный период (год и месяц)
    CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id)
);