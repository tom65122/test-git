use traffic;
SET hivevar:do_date=2025-08-01;
-- 创建页面日志表
drop table if exists dwd_page_log;
CREATE EXTERNAL TABLE dwd_page_log
(
    `user_id`      STRING COMMENT '用户id',
    `sku_id`       STRING COMMENT '商品id',
    `page_id`      STRING COMMENT '页面id',
    `last_page_id` STRING COMMENT '上页id',
    `during_time`  BIGINT COMMENT '页面停留时间',
    `source_type`  STRING COMMENT '来源类型',
    `ts`           BIGINT COMMENT '时间戳'
) COMMENT '页面日志表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/traffic/dwd/dwd_page_log/';
-- 数据装载
INSERT OVERWRITE TABLE dwd_page_log PARTITION (dt = '${do_date}')
SELECT
    common.uid AS user_id,
    page.item AS sku_id,
    page.page_id AS page_id,
    page.last_page_id AS last_page_id,
    page.during_time AS during_time,
    page.source_type AS source_type,
    ts
FROM ods_page_log
WHERE dt = '${do_date}'
  AND page.page_id IS NOT NULL;


-- 创建购物车添加事实表
drop table if exists dwd_trade_cart_add_inc;
CREATE EXTERNAL TABLE dwd_trade_cart_add_inc
(
    `user_id`     STRING COMMENT '用户id',
    `sku_id`      STRING COMMENT '商品id',
    `date_id`     STRING COMMENT '日期id',
    `create_time` STRING COMMENT '创建时间',
    `source_type` STRING COMMENT '来源类型',
    `source_id`   STRING COMMENT '来源编号',
    `sku_num`     BIGINT COMMENT '加购件数',
    `ts`          BIGINT COMMENT '时间戳'
) COMMENT '加购事务事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/traffic/dwd/dwd_trade_cart_add_inc/';

-- 数据装载
INSERT OVERWRITE TABLE dwd_trade_cart_add_inc PARTITION (dt = '${do_date}')
SELECT
    data.user_id AS user_id,
    data.sku_id AS sku_id,
    date_format(data.create_time, 'yyyy-MM-dd') AS date_id,
    data.create_time AS create_time,
    data.source_type AS source_type,
    data.source_id AS source_id,
    data.sku_num AS sku_num,
    ts
FROM ods_cart_add_inc
WHERE dt = '${do_date}'
  AND type = 'insert';

-- 创建订单明细事实表
CREATE EXTERNAL TABLE dwd_trade_order_detail_inc
(
    `user_id`            STRING COMMENT '用户id',
    `order_id`           STRING COMMENT '订单id',
    `sku_id`             STRING COMMENT '商品id',
    `date_id`            STRING COMMENT '日期id',
    `create_time`        STRING COMMENT '创建时间',
    `source_type`        STRING COMMENT '来源类型',
    `source_id`          STRING COMMENT '来源编号',
    `sku_num`            BIGINT COMMENT '商品数量',
    `split_total_amount` DECIMAL(16, 2) COMMENT '应支付金额',
    `ts`                 BIGINT COMMENT '时间戳'
) COMMENT '订单明细事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/traffic/dwd/dwd_trade_order_detail_inc/';

-- 数据装载
INSERT OVERWRITE TABLE dwd_trade_order_detail_inc PARTITION (dt = '${do_date}')
SELECT
    data.user_id AS user_id,
    data.order_id AS order_id,
    data.sku_id AS sku_id,
    date_format(data.create_time, 'yyyy-MM-dd') AS date_id,
    data.create_time AS create_time,
    data.source_type AS source_type,
    data.source_id AS source_id,
    data.sku_num AS sku_num,
    data.split_total_amount AS split_total_amount,
    ts
FROM ods_order_detail_inc
WHERE dt = '${do_date}'
  AND type = 'insert';


-- 创建支付成功事实表
CREATE EXTERNAL TABLE dwd_trade_pay_suc_detail_inc
(
    `user_id`              STRING COMMENT '用户id',
    `order_id`             STRING COMMENT '订单id',
    `sku_id`               STRING COMMENT '商品id',
    `date_id`              STRING COMMENT '日期id',
    `payment_time`         STRING COMMENT '支付时间',
    `sku_num`              BIGINT COMMENT '商品数量',
    `split_payment_amount` DECIMAL(16, 2) COMMENT '支付金额',
    `ts`                   BIGINT COMMENT '时间戳'
) COMMENT '支付成功事务事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/warehouse/traffic/dwd/dwd_trade_pay_suc_detail_inc/';

-- 数据装载
-- 数据装载（关联版本）
-- 数据装载（兼容版本）
INSERT OVERWRITE TABLE dwd_trade_pay_suc_detail_inc PARTITION (dt = '${do_date}')
SELECT
    p.data.user_id AS user_id,
    p.data.order_id AS order_id,
    CASE WHEN od.data.sku_id IS NOT NULL THEN od.data.sku_id ELSE 'UNKNOWN' END AS sku_id,
    date_format(p.data.payment_time, 'yyyy-MM-dd') AS date_id,
    p.data.payment_time AS payment_time,
    CASE WHEN od.data.sku_num IS NOT NULL THEN od.data.sku_num ELSE 0 END AS sku_num,
    p.data.total_amount AS split_payment_amount,
    p.ts
FROM ods_payment_info_inc p
         LEFT JOIN ods_order_detail_inc od ON p.data.order_id = od.data.order_id AND od.dt = '${do_date}' AND od.type = 'insert'
WHERE p.dt = '${do_date}'
  AND p.type = 'insert';



