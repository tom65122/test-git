use traff;


-- 商品信息表（不分区）
CREATE TABLE IF NOT EXISTS dwd_product_info
(
    item_id     STRING COMMENT '商品ID',
    item_name   STRING COMMENT '商品名称',
    category_id STRING COMMENT '类目ID',
    brand_id    STRING COMMENT '品牌ID',
    create_time STRING COMMENT '创建时间',
    is_deleted  INT COMMENT '是否删除'
)
    COMMENT 'DWD：商品信息'
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dwd_product_info
SELECT item_id, item_name, category_id, brand_id, create_time, is_deleted
FROM ods_product_info
WHERE item_id IS NOT NULL;


-- SKU 信息表（不分区）
CREATE TABLE IF NOT EXISTS dwd_sku_info
(
    sku_id      STRING COMMENT 'SKU ID',
    item_id     STRING COMMENT '商品ID',
    spec        STRING COMMENT '规格',
    stock       INT COMMENT '库存数量',
    sku_price   DOUBLE COMMENT 'SKU价格',
    create_time STRING COMMENT '创建时间'
)
    COMMENT 'DWD：SKU信息'
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dwd_sku_info
SELECT sku_id, item_id, spec, stock, sku_price, create_time
FROM ods_sku_info
WHERE sku_id IS NOT NULL;


-- 用户信息表（不分区）
CREATE TABLE IF NOT EXISTS dwd_user_info
(
    user_id       STRING COMMENT '用户ID',
    gender        STRING COMMENT '性别',
    age           INT COMMENT '年龄',
    region        STRING COMMENT '地区',
    register_time STRING COMMENT '注册时间'
)
    COMMENT 'DWD：用户信息'
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dwd_user_info
SELECT user_id, gender, age, region, register_time
FROM ods_user_info
WHERE user_id IS NOT NULL;


-- 订单明细事实表（按月分区）
CREATE TABLE IF NOT EXISTS dwd_order_detail
(
    order_id     STRING COMMENT '订单ID',
    user_id      STRING COMMENT '用户ID',
    item_id      STRING COMMENT '商品ID',
    sku_id       STRING COMMENT 'SKU ID',
    order_price  DOUBLE COMMENT '订单价格',
    quantity     INT COMMENT '购买数量',
    order_status STRING COMMENT '订单状态'
) COMMENT 'DWD：订单明细'
    PARTITIONED BY (dt STRING) -- 月份分区 yyyy-MM

    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dwd_order_detail PARTITION (dt)
SELECT order_id,
       user_id,
       item_id,
       sku_id,
       order_price,
       quantity,
       order_status,
       substr(order_time, 1, 7) AS dt
FROM ods_order_detail
WHERE order_id IS NOT NULL;


-- 支付信息事实表（按月分区）
CREATE TABLE IF NOT EXISTS dwd_payment_info
(
    payment_id  STRING COMMENT '支付ID',
    order_id    STRING COMMENT '订单ID',
    pay_amount  DOUBLE COMMENT '支付金额',
    pay_channel STRING COMMENT '支付渠道'
) COMMENT 'DWD：支付信息'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dwd_payment_info PARTITION (dt)
SELECT payment_id,
       order_id,
       pay_amount,
       pay_channel,
       substr(pay_time, 1, 7) AS dt
FROM ods_payment_info
WHERE payment_id IS NOT NULL;


-- 搜索行为日志（月分区）
CREATE TABLE IF NOT EXISTS dwd_search_log
(
    search_id STRING COMMENT '搜索ID',
    user_id   STRING COMMENT '用户ID',
    keyword   STRING COMMENT '搜索关键词'
) COMMENT 'DWD：搜索日志'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dwd_search_log PARTITION (dt)
SELECT search_id,
       user_id,
       keyword,
       substr(search_time, 1, 7) AS dt
FROM ods_search_log
WHERE search_id IS NOT NULL;


-- 内容互动行为日志（月分区）
CREATE TABLE IF NOT EXISTS dwd_content_log
(
    content_id       STRING COMMENT '内容ID',
    user_id          STRING COMMENT '用户ID',
    item_id          STRING COMMENT '商品ID',
    content_type     STRING COMMENT '内容类型',
    interaction_type STRING COMMENT '互动类型'
) COMMENT 'DWD：内容互动日志'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dwd_content_log PARTITION (dt)
SELECT content_id,
       user_id,
       item_id,
       content_type,
       interaction_type,
       substr(interaction_time, 1, 7) AS dt
FROM ods_content_log
WHERE content_id IS NOT NULL;


-- 商品评价信息（月分区）
CREATE TABLE IF NOT EXISTS dwd_comment_info
(
    comment_id   STRING COMMENT '评价ID',
    user_id      STRING COMMENT '用户ID',
    item_id      STRING COMMENT '商品ID',
    sku_id       STRING COMMENT 'SKU ID',
    score        DOUBLE COMMENT '评分',
    comment_text STRING COMMENT '评价内容'
) COMMENT 'DWD：商品评论信息'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dwd_comment_info PARTITION (dt)
SELECT comment_id,
       user_id,
       item_id,
       sku_id,
       score,

       comment_text,
       substr(comment_time, 1, 7) AS dt
FROM ods_comment_info
WHERE comment_id IS NOT NULL;
