use traffic;
SET hivevar:do_date=2025-08-05;

-- 创建商品粒度页面浏览数据汇总表
drop table if exists dws_product_page_view_1d;
CREATE EXTERNAL TABLE dws_product_page_view_1d
(
    `sku_id`             STRING COMMENT '商品id',
    `view_count_1d`      BIGINT COMMENT '浏览次数',
    `view_user_count_1d` BIGINT COMMENT '浏览人数',
    `avg_stay_time_1d`   BIGINT COMMENT '平均停留时间(毫秒)'
) COMMENT '商品粒度页面浏览数据汇总表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/traffic/dws/dws_product_page_view_1d/';

-- 数据装载
INSERT into TABLE dws_product_page_view_1d PARTITION (dt = '${do_date}')
SELECT sku_id,
       COUNT(*)                AS view_count_1d,
       COUNT(DISTINCT user_id) AS view_user_count_1d,
       AVG(during_time)        AS avg_stay_time_1d
FROM dwd_page_log
WHERE dt = '${do_date}'
  AND page_id = 'good_detail'
GROUP BY sku_id;

-- 创建商品粒度加购数据汇总表
drop table if exists dws_product_cart_add_1d;
CREATE EXTERNAL TABLE dws_product_cart_add_1d
(
    `sku_id`                 STRING COMMENT '商品id',
    `cart_add_count_1d`      BIGINT COMMENT '加购次数',
    `cart_add_user_count_1d` BIGINT COMMENT '加购人数',
    `cart_add_num_1d`        BIGINT COMMENT '加购商品件数'
) COMMENT '商品粒度加购数据汇总表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/traffic/dws/dws_product_cart_add_1d/';

-- 数据装载
INSERT into TABLE dws_product_cart_add_1d PARTITION (dt = '${do_date}')
SELECT sku_id,
       COUNT(*)                AS cart_add_count_1d,
       COUNT(DISTINCT user_id) AS cart_add_user_count_1d,
       SUM(sku_num)            AS cart_add_num_1d
FROM dwd_trade_cart_add_inc
WHERE dt = '${do_date}'
GROUP BY sku_id;

-- 创建商品粒度下单数据汇总表
drop table if exists dws_product_order_1d;
CREATE EXTERNAL TABLE dws_product_order_1d
(
    `sku_id`              STRING COMMENT '商品id',
    `order_count_1d`      BIGINT COMMENT '下单次数',
    `order_user_count_1d` BIGINT COMMENT '下单人数',
    `order_num_1d`        BIGINT COMMENT '下单商品件数',
    `order_amount_1d`     DECIMAL(16, 2) COMMENT '下单金额'
) COMMENT '商品粒度下单数据汇总表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/traffic/dws/dws_product_order_1d/';

-- 数据装载
INSERT into TABLE dws_product_order_1d PARTITION (dt = '${do_date}')
SELECT sku_id,
       COUNT(*)                AS order_count_1d,
       COUNT(DISTINCT user_id) AS order_user_count_1d,
       SUM(sku_num)            AS order_num_1d,
       SUM(split_total_amount) AS order_amount_1d
FROM dwd_trade_order_detail_inc
WHERE dt = '${do_date}'
GROUP BY sku_id;

-- 创建商品粒度支付数据汇总表
drop table if exists dws_product_payment_1d;
CREATE EXTERNAL TABLE dws_product_payment_1d
(
    `sku_id`                STRING COMMENT '商品id',
    `payment_count_1d`      BIGINT COMMENT '支付次数',
    `payment_user_count_1d` BIGINT COMMENT '支付人数',
    `payment_num_1d`        BIGINT COMMENT '支付商品件数',
    `payment_amount_1d`     DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '商品粒度支付数据汇总表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/traffic/dws/dws_product_payment_1d/';

-- 数据装载
INSERT into TABLE dws_product_payment_1d PARTITION (dt = '${do_date}')
SELECT sku_id,
       COUNT(*)                  AS payment_count_1d,
       COUNT(DISTINCT user_id)   AS payment_user_count_1d,
       SUM(sku_num)              AS payment_num_1d,
       SUM(split_payment_amount) AS payment_amount_1d
FROM dwd_trade_pay_suc_detail_inc
WHERE dt = '${do_date}'
GROUP BY sku_id;
