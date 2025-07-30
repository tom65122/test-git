create database Trafficanalysis;
use Trafficanalysis;
drop table if exists ods_user_click_log;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_user_click_log (
          `log_id` STRING COMMENT '点击日志唯一ID',
          `user_id` STRING COMMENT '用户ID',
          `session_id` STRING COMMENT '会话ID（同一会话内的连续操作）',
          `page_id` STRING COMMENT '页面ID',
          `module_id` STRING COMMENT '页面内模块ID（如轮播图、商品列表）',
          `click_time` TIMESTAMP COMMENT '点击时间',
          `is_bounce` TINYINT COMMENT '是否跳出（1:是，0:否）'
)
    COMMENT 'ODS层-用户点击行为日志表（每日增量>100万条）'
    PARTITIONED BY (`dt` STRING COMMENT '2025-07-30')
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'  -- 适配JSON格式日志
    LOCATION '/warehouse/tablespace/ods_user_click_log/';  -- HDFS存储路径


load data inpath '/warehouse/ods_user_click_log.json' overwrite into table ods_user_click_log PARTITION (dt='2025-07-30');



select * from ods_user_click_log;



drop table if exists ods_page_view_log;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_page_view_log (
          `log_id` STRING COMMENT '访问日志唯一ID',
          `user_id` STRING COMMENT '用户ID',
          `session_id` STRING COMMENT '会话ID',
          `page_id` STRING COMMENT '页面ID（如首页、商品详情页）',
          `view_time` TIMESTAMP COMMENT '页面访问时间',
          `stay_time` INT COMMENT '页面停留时间（秒）'
)
    COMMENT 'ODS层-页面访问日志表'
    PARTITIONED BY (`dt` STRING COMMENT '2025-07-30')
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/ods_page_view_log/';


load data inpath '/warehouse/ods_page_view_log.json' overwrite into table ods_page_view_log PARTITION (dt='2025-07-30');

select * from ods_page_view_log;



drop table if exists ods_trade_order;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_trade_order (
        `order_id` STRING COMMENT '订单唯一ID',
        `user_id` STRING COMMENT '下单用户ID',
        `order_time` TIMESTAMP COMMENT '下单时间',
        `payment_amount` DECIMAL(16, 2) COMMENT '支付金额（元）',
        `source_page_id` STRING COMMENT '订单来源页面ID（如商品详情页、活动页）'
)
    COMMENT 'ODS层-交易订单表'
    PARTITIONED BY (`dt` STRING COMMENT '2025-07-30')
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/ods_trade_order/';

load data inpath '/warehouse/ods_trade_order.json' overwrite into table ods_trade_order PARTITION (dt='2025-07-30');


select * from ods_trade_order;
