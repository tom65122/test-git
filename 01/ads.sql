-- 创建商品效率统计表
CREATE EXTERNAL TABLE ads_product_efficiency_stat
(
    -- 统计维度
    `stat_date`               STRING COMMENT '统计日期',
    `stat_week`               STRING COMMENT '统计周',
    `stat_month`              STRING COMMENT '统计月',
    `stat_year`               STRING COMMENT '统计年',

    -- 访问相关指标
    `product_visitor_count`   BIGINT COMMENT '商品访客数',
    `product_view_count`      BIGINT COMMENT '商品浏览量',
    `visited_product_count`   BIGINT COMMENT '有访问商品数',
    `avg_stay_time`           DECIMAL(16, 2) COMMENT '商品平均停留时长(秒)',

    -- 加购相关指标
    `cart_add_num`            BIGINT COMMENT '商品加购件数',
    `cart_add_user_count`     BIGINT COMMENT '商品加购人数',
    `cart_conversion_rate`    DECIMAL(16, 4) COMMENT '访问加购转化率',

    -- 订单相关指标
    `order_user_count`        BIGINT COMMENT '下单买家数',
    `order_num`               BIGINT COMMENT '下单件数',
    `order_amount`            DECIMAL(16, 2) COMMENT '下单金额',
    `order_conversion_rate`   DECIMAL(16, 4) COMMENT '下单转化率',

    -- 支付相关指标
    `payment_user_count`      BIGINT COMMENT '支付买家数',
    `payment_num`             BIGINT COMMENT '支付件数',
    `payment_amount`          DECIMAL(16, 2) COMMENT '支付金额',
    `paid_product_count`      BIGINT COMMENT '有支付商品数',
    `payment_conversion_rate` DECIMAL(16, 4) COMMENT '支付转化率',
    `customer_price`          DECIMAL(16, 2) COMMENT '客单价'
) COMMENT '商品效率统计表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/电商数仓/ads/ads_product_efficiency_stat/';

-- 数据装载
INSERT OVERWRITE TABLE ads_product_efficiency_stat PARTITION (dt = '${do_date}')
SELECT
    -- 统计维度
    '${do_date}'                                                                                     AS stat_date,
    date_format('${do_date}', 'yyyy-ww')                                                             AS stat_week,
    date_format('${do_date}', 'yyyy-MM')                                                             AS stat_month,
    year('${do_date}')                                                                               AS stat_year,

    -- 访问相关指标
    SUM(pv.view_user_count_1d)                                                                       AS product_visitor_count,
    SUM(pv.view_count_1d)                                                                            AS product_view_count,
    COUNT(CASE WHEN pv.view_count_1d > 0 THEN 1 END)                                                 AS visited_product_count,
    CAST(AVG(pv.avg_stay_time_1d) / 1000.0 AS DECIMAL(16, 2))                                        AS avg_stay_time,

    -- 加购相关指标
    SUM(cart.cart_add_num_1d)                                                                        AS cart_add_num,
    SUM(cart.cart_add_user_count_1d)                                                                 AS cart_add_user_count,
    CAST(SUM(cart.cart_add_user_count_1d) /
         NULLIF(SUM(pv.view_user_count_1d), 0) AS DECIMAL(16, 4))                                    AS cart_conversion_rate,

    -- 订单相关指标
    SUM(ord.order_user_count_1d)                                                                     AS order_user_count,
    SUM(ord.order_num_1d)                                                                            AS order_num,
    SUM(ord.order_amount_1d)                                                                         AS order_amount,
    CAST(SUM(ord.order_user_count_1d) /
         NULLIF(SUM(pv.view_user_count_1d), 0) AS DECIMAL(16, 4))                                    AS order_conversion_rate,

    -- 支付相关指标
    SUM(pay.payment_user_count_1d)                                                                   AS payment_user_count,
    SUM(pay.payment_num_1d)                                                                          AS payment_num,
    SUM(pay.payment_amount_1d)                                                                       AS payment_amount,
    COUNT(CASE WHEN pay.payment_amount_1d > 0 THEN 1 END)                                            AS paid_product_count,
    CAST(SUM(pay.payment_user_count_1d) /
         NULLIF(SUM(pv.view_user_count_1d), 0) AS DECIMAL(16, 4))                                    AS payment_conversion_rate,
    CAST(SUM(pay.payment_amount_1d) / NULLIF(SUM(pay.payment_user_count_1d), 0) AS DECIMAL(16, 2))   AS customer_price

FROM (SELECT * FROM dws_product_page_view_1d WHERE dt = '${do_date}') pv
         FULL OUTER JOIN
         (SELECT * FROM dws_product_cart_add_1d WHERE dt = '${do_date}') cart ON pv.sku_id = cart.sku_id
         FULL OUTER JOIN
         (SELECT * FROM dws_product_order_1d WHERE dt = '${do_date}') ord ON pv.sku_id = ord.sku_id
         FULL OUTER JOIN
         (SELECT * FROM dws_product_payment_1d WHERE dt = '${do_date}') pay ON pv.sku_id = pay.sku_id
GROUP BY '${do_date}',
         date_format('${do_date}', 'yyyy-ww'),
         date_format('${do_date}', 'yyyy-MM'),
         year('${do_date}');


-- 创建商品区间分析表
CREATE EXTERNAL TABLE ads_product_range_analysis
(
    -- 统计维度
    `stat_date`            STRING COMMENT '统计日期',
    `stat_week`            STRING COMMENT '统计周',
    `stat_month`           STRING COMMENT '统计月',
    `range_type`           STRING COMMENT '区间类型(price_range:价格区间, payment_num_range:支付件数区间, payment_amount_range:支付金额区间)',
    `range_value`          STRING COMMENT '区间值',

    -- 分析指标
    `active_product_count` BIGINT COMMENT '动销商品数',
    `payment_amount`       DECIMAL(16, 2) COMMENT '支付金额',
    `payment_num`          BIGINT COMMENT '支付件数',
    `avg_price`            DECIMAL(16, 2) COMMENT '件单价'
) COMMENT '商品区间分析表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/电商数仓/ads/ads_product_range_analysis/';

-- 按价格区间分析数据装载
INSERT OVERWRITE TABLE ads_product_range_analysis PARTITION (dt = '${do_date}')
SELECT
    -- 统计维度
    '${do_date}'                                                                                                      AS stat_date,
    date_format('${do_date}', 'yyyy-ww')                                                                              AS stat_week,
    date_format('${do_date}', 'yyyy-MM')                                                                              AS stat_month,
    'price_range'                                                                                                     AS range_type,
    CASE
        WHEN sku.price <= 50 THEN '0-50'
        WHEN sku.price > 50 AND sku.price <= 100 THEN '51-100'
        WHEN sku.price > 100 AND sku.price <= 150 THEN '101-150'
        WHEN sku.price > 150 AND sku.price <= 300 THEN '151-300'
        WHEN sku.price > 300 THEN '300+'
        ELSE '其他'
        END                                                                                                           AS range_value,

    -- 分析指标
    COUNT(DISTINCT sku.id)                                                                                            AS active_product_count,
    SUM(COALESCE(pay.payment_amount_1d, 0))                                                                           AS payment_amount,
    SUM(COALESCE(pay.payment_num_1d, 0))                                                                              AS payment_num,
    CAST(SUM(COALESCE(pay.payment_amount_1d, 0)) /
         NULLIF(SUM(COALESCE(pay.payment_num_1d, 0)), 0) AS DECIMAL(16, 2))                                           AS avg_price
FROM (SELECT * FROM ods_sku_info_full WHERE dt = '${do_date}') sku
         LEFT JOIN
         (SELECT * FROM dws_product_payment_1d WHERE dt = '${do_date}') pay ON sku.id = pay.sku_id
GROUP BY CASE
             WHEN sku.price <= 50 THEN '0-50'
             WHEN sku.price > 50 AND sku.price <= 100 THEN '51-100'
             WHEN sku.price > 100 AND sku.price <= 150 THEN '101-150'
             WHEN sku.price > 150 AND sku.price <= 300 THEN '151-300'
             WHEN sku.price > 300 THEN '300+'
             ELSE '其他'
             END

UNION ALL

-- 按支付件数区间分析
SELECT
    -- 统计维度
    '${do_date}'                                                                            AS stat_date,
    date_format('${do_date}', 'yyyy-ww')                                                    AS stat_week,
    date_format('${do_date}', 'yyyy-MM')                                                    AS stat_month,
    'payment_num_range'                                                                     AS range_type,
    CASE
        WHEN pay.payment_num_1d <= 50 THEN '0-50'
        WHEN pay.payment_num_1d > 50 AND pay.payment_num_1d <= 100 THEN '51-100'
        WHEN pay.payment_num_1d > 100 AND pay.payment_num_1d <= 150 THEN '101-150'
        WHEN pay.payment_num_1d > 150 AND pay.payment_num_1d <= 300 THEN '151-300'
        WHEN pay.payment_num_1d > 300 THEN '300+'
        ELSE '其他'
        END                                                                                 AS range_value,

    -- 分析指标
    COUNT(DISTINCT pay.sku_id)                                                              AS active_product_count,
    SUM(pay.payment_amount_1d)                                                              AS payment_amount,
    SUM(pay.payment_num_1d)                                                                 AS payment_num,
    CAST(SUM(pay.payment_amount_1d) / NULLIF(SUM(pay.payment_num_1d), 0) AS DECIMAL(16, 2)) AS avg_price
FROM dws_product_payment_1d pay
WHERE pay.dt = '${do_date}'
GROUP BY CASE
             WHEN pay.payment_num_1d <= 50 THEN '0-50'
             WHEN pay.payment_num_1d > 50 AND pay.payment_num_1d <= 100 THEN '51-100'
             WHEN pay.payment_num_1d > 100 AND pay.payment_num_1d <= 150 THEN '101-150'
             WHEN pay.payment_num_1d > 150 AND pay.payment_num_1d <= 300 THEN '151-300'
             WHEN pay.payment_num_1d > 300 THEN '300+'
             ELSE '其他'
             END

UNION ALL

-- 按支付金额区间分析
SELECT
    -- 统计维度
    '${do_date}'                                                                            AS stat_date,
    date_format('${do_date}', 'yyyy-ww')                                                    AS stat_week,
    date_format('${do_date}', 'yyyy-MM')                                                    AS stat_month,
    'payment_amount_range'                                                                  AS range_type,
    CASE
        WHEN pay.payment_amount_1d <= 1000 THEN '0-1000'
        WHEN pay.payment_amount_1d > 1000 AND pay.payment_amount_1d <= 5000 THEN '1001-5000'
        WHEN pay.payment_amount_1d > 5000 AND pay.payment_amount_1d <= 10000 THEN '5001-10000'
        WHEN pay.payment_amount_1d > 10000 AND pay.payment_amount_1d <= 50000 THEN '10001-50000'
        WHEN pay.payment_amount_1d > 50000 THEN '50000+'
        ELSE '其他'
        END                                                                                 AS range_value,

    -- 分析指标
    COUNT(DISTINCT pay.sku_id)                                                              AS active_product_count,
    SUM(pay.payment_amount_1d)                                                              AS payment_amount,
    SUM(pay.payment_num_1d)                                                                 AS payment_num,
    CAST(SUM(pay.payment_amount_1d) / NULLIF(SUM(pay.payment_num_1d), 0) AS DECIMAL(16, 2)) AS avg_price
FROM dws_product_payment_1d pay
WHERE pay.dt = '${do_date}'
GROUP BY CASE
             WHEN pay.payment_amount_1d <= 1000 THEN '0-1000'
             WHEN pay.payment_amount_1d > 1000 AND pay.payment_amount_1d <= 5000 THEN '1001-5000'
             WHEN pay.payment_amount_1d > 5000 AND pay.payment_amount_1d <= 10000 THEN '5001-10000'
             WHEN pay.payment_amount_1d > 10000 AND pay.payment_amount_1d <= 50000 THEN '10001-50000'
             WHEN pay.payment_amount_1d > 50000 THEN '50000+'
             ELSE '其他'
             END;
