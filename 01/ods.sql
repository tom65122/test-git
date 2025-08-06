create database if not exists traffic;

use traffic;

-- 创建页面日志表
drop table if exists ods_page_log;
CREATE EXTERNAL TABLE ods_page_log
(
    `common` STRUCT<ar : STRING, ba : STRING, ch : STRING, is_new : STRING, md : STRING, mid : STRING, os : STRING, uid
                    : STRING, vc : STRING> COMMENT '公共信息',
    `page`   STRUCT<during_time : BIGINT, item : STRING, item_type : STRING, last_page_id : STRING, page_id : STRING,
                    source_type : STRING> COMMENT '页面信息',
    `ts`     BIGINT COMMENT '时间戳'
) COMMENT '页面日志表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/电商数仓/ods/ods_page_log/';

-- 生成500条页面日志数据
INSERT INTO TABLE ods_page_log PARTITION (dt = '2025-08-01')
SELECT named_struct(
               'ar', case cast(rand() * 10 as int)
                         when 0 then '北京'
                         when 1 then '上海'
                         when 2 then '广州'
                         when 3 then '深圳'
                         when 4 then '杭州'
                         when 5 then '成都'
                         when 6 then '武汉'
                         when 7 then '西安'
                         when 8 then '南京'
                         else '重庆'
            end,
               'ba', case cast(rand() * 5 as int)
                         when 0 then 'iPhone'
                         when 1 then 'android'
                         when 2 then 'web'
                         when 3 then 'iPad'
                         else 'Windows'
                   end,
               'ch', case cast(rand() * 5 as int)
                         when 0 then 'App Store'
                         when 1 then '应用宝'
                         when 2 then '官网'
                         when 3 then '华为应用市场'
                         else '小米应用商店'
                   end,
               'is_new', case cast(rand() * 2 as int)
                             when 0 then '0'
                             else '1'
                   end,
               'md', case cast(rand() * 10 as int)
                         when 0 then 'iPhone15,3'
                         when 1 then 'Xiaomi 13'
                         when 2 then 'HUAWEI P60'
                         when 3 then 'Samsung S23'
                         when 4 then 'MacBook Pro'
                         when 5 then 'iPad Pro'
                         when 6 then 'Windows PC'
                         when 7 then 'Surface Pro'
                         when 8 then '华为MateBook'
                         else '小米笔记本'
                   end,
               'mid', concat('MID_', lpad(cast((rand() * 90000 + 10000) as int), 5, '0')),
               'os', case cast(rand() * 8 as int)
                         when 0 then 'iOS 17.1'
                         when 1 then 'Android 14'
                         when 2 then 'Windows 11'
                         when 3 then 'macOS 14'
                         when 4 then 'iOS 16.5'
                         when 5 then 'Android 13'
                         when 6 then 'Windows 10'
                         else 'macOS 13'
                   end,
               'uid', concat('USER_', lpad(cast((rand() * 90000 + 10000) as int), 5, '0')),
               'vc', case cast(rand() * 5 as int)
                         when 0 then '1.0.0'
                         when 1 then '1.2.5'
                         when 2 then '2.0.1'
                         when 3 then '2.3.0'
                         else '3.1.2'
                   end
           ) as common,
       named_struct(
               'during_time', cast((rand() * 300000 + 10000) as bigint),
               'item', concat('SKU', lpad(cast((rand() * 90000 + 10000) as int), 5, '0')),
               'item_type', case cast(rand() * 3 as int)
                                when 0 then 'product'
                                when 1 then 'category'
                                else 'brand'
                   end,
               'last_page_id', case cast(rand() * 5 as int)
                                   when 0 then 'home'
                                   when 1 then 'search'
                                   when 2 then 'category'
                                   when 3 then 'recommend'
                                   else 'promotion'
                   end,
               'page_id', case cast(rand() * 5 as int)
                              when 0 then 'product_detail'
                              when 1 then 'product_list'
                              when 2 then 'cart'
                              when 3 then 'checkout'
                              else 'user_center'
                   end,
               'source_type', case cast(rand() * 5 as int)
                                  when 0 then 'banner'
                                  when 1 then 'recommend'
                                  when 2 then 'search'
                                  when 3 then 'category'
                                  else 'direct'
                   end
           ) as page,
       cast((rand() * 86400000 + 1704067200000) as bigint) as ts
FROM (SELECT 1) t1
         LATERAL VIEW explode(split(repeat('x', 500), 'x')) t2 as row_col;


-- 创建购物车添加增量表
drop table if exists ods_cart_add_inc;
CREATE EXTERNAL TABLE ods_cart_add_inc
(
    `type` STRING COMMENT '操作类型',
    `ts`   BIGINT COMMENT '时间戳',
    `data` STRUCT<id : STRING, user_id : STRING, sku_id : STRING, cart_price : DECIMAL(16, 2), sku_num : BIGINT, img_url
                  : STRING, sku_name : STRING, is_checked : STRING, create_time : STRING, operate_time : STRING,
                  is_ordered : STRING, order_time : STRING, source_type : STRING, source_id : STRING> COMMENT '数据'
) COMMENT '购物车添加增量表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/电商数仓/ods/ods_cart_add_inc/';

-- 生成300条购物车添加数据
INSERT INTO TABLE ods_cart_add_inc PARTITION (dt = '2025-08-01')
SELECT case cast(rand() * 3 as int)
           when 0 then 'insert'
           when 1 then 'update'
           else 'delete'
           end as type,
       cast((rand() * 86400000 + 1704067200000) as bigint) as ts,
       named_struct(
               'id', concat('CART_', lpad(cast((rand() * 90000 + 10000) as int), 5, '0')),
               'user_id', concat('USER_', lpad(cast((rand() * 90000 + 10000) as int), 5, '0')),
               'sku_id', concat('SKU', lpad(cast((rand() * 90000 + 10000) as int), 5, '0')),
               'cart_price', cast(round(rand() * 10000 + 100, 2) as decimal(16, 2)),
               'sku_num', cast((rand() * 10 + 1) as bigint),
               'img_url', concat('http://example.com/images/product_', cast(rand() * 1000 as int), '.jpg'),
               'sku_name', case cast(rand() * 20 as int)
                               when 0 then 'iPhone 15 Pro'
                               when 1 then 'MacBook Air M2'
                               when 2 then 'AirPods Pro'
                               when 3 then 'iPad Pro'
                               when 4 then 'Apple Watch Series 9'
                               when 5 then '华为P60'
                               when 6 then '华为MateBook'
                               when 7 then '华为FreeBuds'
                               when 8 then '小米13'
                               when 9 then '小米笔记本'
                               when 10 then '小米手环'
                               when 11 then '三星Galaxy S23'
                               when 12 then '三星平板'
                               when 13 then '三星耳机'
                               when 14 then '戴尔XPS笔记本'
                               when 15 then '索尼降噪耳机'
                               when 16 then '联想ThinkPad'
                               when 17 then 'OPPO Find'
                               when 18 then 'vivo X系列'
                               else '一加手机'
                   end,
               'is_checked', case cast(rand() * 2 as int)
                                 when 0 then '0'
                                 else '1'
                   end,
               'create_time', '2025-08-01 00:00:00',
               'operate_time', from_unixtime(unix_timestamp('2025-08-01 00:00:00') + cast(rand() * 86400 as int)),
               'is_ordered', case cast(rand() * 5 as int)
                                 when 0 then '0'
                                 else '1'
                   end,
               'order_time', case
                                 when cast(rand() * 5 as int) = 0 then null
                                 else from_unixtime(unix_timestamp('2025-08-01 00:00:00') + cast(rand() * 86400 as int))
                   end,
               'source_type', case cast(rand() * 5 as int)
                                  when 0 then 'product_detail'
                                  when 1 then 'recommend'
                                  when 2 then 'search'
                                  when 3 then 'category'
                                  else 'promotion'
                   end,
               'source_id', case
                                when cast(rand() * 5 as int) = 0 then null
                                else concat('SOURCE_', lpad(cast(rand() * 1000 as int), 3, '0'))
                   end
           ) as data
FROM (SELECT 1) t1
         LATERAL VIEW explode(split(repeat('x', 300), 'x')) t2 as row_col;

-- 创建订单明细增量表
drop table if exists ods_order_detail_inc;
CREATE EXTERNAL TABLE ods_order_detail_inc
(
    `type` STRING COMMENT '操作类型',
    `ts`   BIGINT COMMENT '时间戳',
    `data` STRUCT<id : STRING, order_id : STRING, user_id : STRING, sku_id : STRING, sku_name : STRING, sku_num
                  : BIGINT, create_time : STRING, source_type : STRING, source_id : STRING, split_total_amount
                  : DECIMAL(16, 2), split_activity_amount : DECIMAL(16, 2), split_coupon_amount
                  : DECIMAL(16, 2)> COMMENT '数据'
) COMMENT '订单明细增量表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/电商数仓/ods/ods_order_detail_inc/';

-- 生成200条订单明细数据
INSERT INTO TABLE ods_order_detail_inc PARTITION (dt = '2025-08-01')
SELECT case cast(rand() * 3 as int)
           when 0 then 'insert'
           when 1 then 'update'
           else 'delete'
           end as type,
       cast((rand() * 86400000 + 1704067200000) as bigint) as ts,
       named_struct(
               'id', concat('ORDER_DETAIL_', lpad(cast((rand() * 90000 + 10000) as int), 5, '0')),
               'order_id', concat('ORDER_', lpad(cast((rand() * 90000 + 10000) as int), 5, '0')),
               'user_id', concat('USER_', lpad(cast((rand() * 90000 + 10000) as int), 5, '0')),
               'sku_id', concat('SKU', lpad(cast((rand() * 90000 + 10000) as int), 5, '0')),
               'sku_name', case cast(rand() * 20 as int)
                               when 0 then 'iPhone 15 Pro'
                               when 1 then 'MacBook Air M2'
                               when 2 then 'AirPods Pro'
                               when 3 then 'iPad Pro'
                               when 4 then 'Apple Watch Series 9'
                               when 5 then '华为P60'
                               when 6 then '华为MateBook'
                               when 7 then '华为FreeBuds'
                               when 8 then '小米13'
                               when 9 then '小米笔记本'
                               when 10 then '小米手环'
                               when 11 then '三星Galaxy S23'
                               when 12 then '三星平板'
                               when 13 then '三星耳机'
                               when 14 then '戴尔XPS笔记本'
                               when 15 then '索尼降噪耳机'
                               when 16 then '联想ThinkPad'
                               when 17 then 'OPPO Find'
                               when 18 then 'vivo X系列'
                               else '一加手机'
                   end,
               'sku_num', cast((rand() * 5 + 1) as bigint),
               'create_time', from_unixtime(unix_timestamp('2025-08-01 00:00:00') + cast(rand() * 86400 as int)),
               'source_type', case cast(rand() * 5 as int)
                                  when 0 then 'product_detail'
                                  when 1 then 'recommend'
                                  when 2 then 'search'
                                  when 3 then 'category'
                                  else 'promotion'
                   end,
               'source_id', case
                                when cast(rand() * 5 as int) = 0 then null
                                else concat('SOURCE_', lpad(cast(rand() * 1000 as int), 3, '0'))
                   end,
               'split_total_amount', cast(round(rand() * 10000 + 100, 2) as decimal(16, 2)),
               'split_activity_amount', cast(round(rand() * 1000, 2) as decimal(16, 2)),
               'split_coupon_amount', cast(round(rand() * 500, 2) as decimal(16, 2))
           ) as data
FROM (SELECT 1) t1
         LATERAL VIEW explode(split(repeat('x', 200), 'x')) t2 as row_col;


-- 创建支付信息增量表
drop table if exists ods_payment_info_inc;
CREATE EXTERNAL TABLE ods_payment_info_inc
(
    `type` STRING COMMENT '操作类型',
    `ts`   BIGINT COMMENT '时间戳',
    `data` STRUCT<id : STRING, out_trade_no : STRING, order_id : STRING, user_id : STRING, payment_type : STRING,
                  trade_no : STRING, total_amount : DECIMAL(16, 2), subject : STRING, payment_time : STRING,
                  callback_content : STRING, callback_time : STRING> COMMENT '数据'
) COMMENT '支付信息增量表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/电商数仓/ods/ods_payment_info_inc/';

-- 生成150条支付信息数据
INSERT INTO TABLE ods_payment_info_inc PARTITION (dt = '2025-08-01')
SELECT case cast(rand() * 3 as int)
           when 0 then 'insert'
           when 1 then 'update'
           else 'delete'
           end as type,
       cast((rand() * 86400000 + 1704067200000) as bigint) as ts,
       named_struct(
               'id', concat('PAYMENT_', lpad(cast((rand() * 90000 + 10000) as int), 5, '0')),
               'out_trade_no', concat('OUT_TRADE_', lpad(cast((rand() * 9000000 + 1000000) as int), 7, '0')),
               'order_id', concat('ORDER_', lpad(cast((rand() * 90000 + 10000) as int), 5, '0')),
               'user_id', concat('USER_', lpad(cast((rand() * 90000 + 10000) as int), 5, '0')),
               'payment_type', case cast(rand() * 4 as int)
                                   when 0 then 'alipay'
                                   when 1 then 'wechat'
                                   when 2 then 'unionpay'
                                   else 'credit_card'
                   end,
               'trade_no', concat('TRADE_', lpad(cast((rand() * 9000000 + 1000000) as int), 7, '0')),
               'total_amount', cast(round(rand() * 10000 + 10, 2) as decimal(16, 2)),
               'subject', case cast(rand() * 5 as int)
                              when 0 then '商品购买'
                              when 1 then '订单支付'
                              when 2 then '购物付款'
                              when 3 then '订单结算'
                              else '商品结算'
                   end,
               'payment_time', from_unixtime(unix_timestamp('2025-08-01 00:00:00') + cast(rand() * 86400 as int)),
               'callback_content', case cast(rand() * 3 as int)
                                       when 0 then '{"status":"success","message":"支付成功"}'
                                       when 1 then '{"status":"success","message":"交易完成"}'
                                       else '{"status":"pending","message":"处理中"}'
                   end,
               'callback_time', from_unixtime(unix_timestamp('2025-08-01 00:00:00') + cast(rand() * 86400 as int) +
                                              cast(rand() * 3600 as int))
           ) as data
FROM (SELECT 1) t1
         LATERAL VIEW explode(split(repeat('x', 150), 'x')) t2 as row_col;
-- 创建商品维度全量表
drop table if exists ods_sku_info_full;
CREATE EXTERNAL TABLE ods_sku_info_full
(
    `id`              STRING COMMENT 'sku_id',
    `spu_id`          STRING COMMENT 'spu_id',
    `price`           DECIMAL(16, 2) COMMENT '价格',
    `sku_name`        STRING COMMENT '商品名称',
    `sku_desc`        STRING COMMENT '商品描述',
    `weight`          DECIMAL(16, 2) COMMENT '重量',
    `tm_id`           STRING COMMENT '品牌id',
    `category3_id`    STRING COMMENT '三级分类id',
    `category2_id`    STRING COMMENT '二级分类id',
    `category1_id`    STRING COMMENT '一级分类id',
    `sku_default_img` STRING COMMENT '默认图片',
    `is_sale`         STRING COMMENT '是否在售',
    `create_time`     STRING COMMENT '创建时间'
) COMMENT '商品维度表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/电商数仓/ods/ods_sku_info_full/';

-- 生成100条商品数据

-- 创建商品维度全量表
drop table if exists ods_sku_info_full;
CREATE EXTERNAL TABLE ods_sku_info_full
(
    `id`              STRING COMMENT 'sku_id',
    `spu_id`          STRING COMMENT 'spu_id',
    `price`           DECIMAL(16, 2) COMMENT '价格',
    `sku_name`        STRING COMMENT '商品名称',
    `sku_desc`        STRING COMMENT '商品描述',
    `weight`          DECIMAL(16, 2) COMMENT '重量',
    `tm_id`           STRING COMMENT '品牌id',
    `category3_id`    STRING COMMENT '三级分类id',
    `category2_id`    STRING COMMENT '二级分类id',
    `category1_id`    STRING COMMENT '一级分类id',
    `sku_default_img` STRING COMMENT '默认图片',
    `is_sale`         STRING COMMENT '是否在售',
    `create_time`     STRING COMMENT '创建时间'
) COMMENT '商品维度表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/电商数仓/ods/ods_sku_info_full/';

-- 生成100条商品数据
INSERT INTO TABLE ods_sku_info_full PARTITION (dt = '2025-08-01')
SELECT concat('SKU', lpad(cast((rand() * 90000 + 10000) as int), 5, '0')) as id,
       concat('SPU', lpad(cast((rand() * 9000 + 1000) as int), 4, '0'))   as spu_id,
       cast(round(rand() * 10000 + 100, 2) as decimal(16, 2))             as price,
       case cast(rand() * 20 as int)
           when 0 then 'iPhone 15 Pro'
           when 1 then 'MacBook Air M2'
           when 2 then 'AirPods Pro'
           when 3 then 'iPad Pro'
           when 4 then 'Apple Watch Series 9'
           when 5 then '华为P60'
           when 6 then '华为MateBook'
           when 7 then '华为FreeBuds'
           when 8 then '小米13'
           when 9 then '小米笔记本'
           when 10 then '小米手环'
           when 11 then '三星Galaxy S23'
           when 12 then '三星平板'
           when 13 then '三星耳机'
           when 14 then '戴尔XPS笔记本'
           when 15 then '索尼降噪耳机'
           when 16 then '联想ThinkPad'
           when 17 then 'OPPO Find'
           when 18 then 'vivo X系列'
           else '一加手机'
           end as sku_name,
       case cast(rand() * 10 as int)
           when 0 then '最新款电子产品'
           when 1 then '高性能数码设备'
           when 2 then '智能穿戴设备'
           when 3 then '移动通讯设备'
           when 4 then '办公必备工具'
           when 5 then '家庭娱乐设备'
           when 6 then '专业摄影设备'
           when 7 then '健康监测设备'
           when 8 then '游戏娱乐设备'
           else '生活辅助工具'
           end as sku_desc,
       cast(round(rand() * 5 + 0.1, 2) as decimal(16, 2)) as weight,
       case cast(rand() * 10 as int)
           when 0 then 'APPLE'
           when 1 then 'HUAWEI'
           when 2 then 'XIAOMI'
           when 3 then 'SAMSUNG'
           when 4 then 'DELL'
           when 5 then 'SONY'
           when 6 then 'LENOVO'
           when 7 then 'OPPO'
           when 8 then 'VIVO'
           else 'ONEPLUS'
           end as tm_id,
       concat('CAT3_', lpad(cast(rand() * 50 as int), 2, '0')) as category3_id,
       concat('CAT2_', lpad(cast(rand() * 20 as int), 2, '0')) as category2_id,
       concat('CAT1_', lpad(cast(rand() * 5 + 1 as int), 1, '0')) as category1_id,
       concat('http://example.com/images/',
              case cast(rand() * 20 as int)
                  when 0 then 'iphone'
                  when 1 then 'macbook'
                  when 2 then 'airpods'
                  when 3 then 'ipad'
                  when 4 then 'watch'
                  when 5 then 'p60'
                  when 6 then 'matebook'
                  when 7 then 'freebuds'
                  when 8 then 'mi13'
                  when 9 then 'milaptop'
                  when 10 then 'miband'
                  when 11 then 's23'
                  when 12 then 'samsungtab'
                  when 13 then 'samsungear'
                  when 14 then 'xps'
                  when 15 then 'sonyheadphone'
                  when 16 then 'thinkpad'
                  when 17 then 'oppo'
                  when 18 then 'vivo'
                  else 'oneplus'
                  end, cast(rand() * 100 as int), '.jpg') as sku_default_img,
       '1' as is_sale,
       '2025-08-01 00:00:00' as create_time
FROM (SELECT 1) t1
         LATERAL VIEW explode(split(repeat('x', 100), 'x')) t2 as row_col;
