CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.finance_order_payments_mart
(
    order_id String,
    customer_id String,
    customer_unique_id String,
    customer_city String,
    customer_state String,
    order_status String,
    order_purchase_date Date,
    order_purchase_month String,
    payment_sequential UInt32,
    payment_type String,
    payment_installments UInt32,
    installment_group String,
    payment_value Float64,
    price Float64,
    freight_value Float64,
    total_item_value Float64,
    total_freight_value Float64,
    total_order_value Float64,
    high_value_order_flag UInt8
)
ENGINE = MergeTree
ORDER BY (order_purchase_date, customer_state, payment_type, order_id);

CREATE TABLE IF NOT EXISTS analytics.finance_customer_value_mart
(
    customer_unique_id String,
    customer_city String,
    customer_state String,
    total_payment_value Float64,
    total_orders UInt32,
    avg_payment_value Float64,
    preferred_payment_type String,
    max_installments UInt32,
    customer_value_segment String
)
ENGINE = MergeTree
ORDER BY (customer_value_segment, customer_state, total_payment_value);

CREATE TABLE IF NOT EXISTS analytics.finance_payment_method_summary
(
    payment_type String,
    installment_group String,
    total_orders UInt32,
    total_payment_value Float64,
    avg_payment_value Float64,
    high_value_order_count UInt32
)
ENGINE = MergeTree
ORDER BY (payment_type, installment_group);

CREATE TABLE IF NOT EXISTS analytics.finance_geo_payment_summary
(
    customer_state String,
    customer_city String,
    total_customers UInt32,
    total_orders UInt32,
    total_payment_value Float64,
    avg_payment_value Float64,
    high_value_customer_count UInt32
)
ENGINE = MergeTree
ORDER BY (customer_state, customer_city);
