use trafficanalysis;


-- 创建用户维度表
CREATE TABLE dim_user (
                          user_id STRING COMMENT '用户ID',
                          register_date STRING COMMENT '注册日期',
                          user_level STRING COMMENT '用户等级',
                          gender STRING COMMENT '性别',
                          age_group STRING COMMENT '年龄段'
) COMMENT '用户维度表'
    PARTITIONED BY (dt STRING COMMENT '分区日期');

-- 插入用户维度数据
INSERT OVERWRITE TABLE dim_user PARTITION (dt='2025-07-30')
SELECT DISTINCT
    user_id,
    '2025-01-01' AS register_date,
    CASE
        WHEN RAND() > 0.8 THEN 'VIP'
        WHEN RAND() > 0.5 THEN '高级'
        ELSE '普通'
        END AS user_level,
    CASE
        WHEN RAND() > 0.5 THEN '男'
        ELSE '女'
        END AS gender,
    CASE
        WHEN RAND() > 0.7 THEN '18-25'
        WHEN RAND() > 0.4 THEN '26-35'
        WHEN RAND() > 0.2 THEN '36-45'
        ELSE '46+'
        END AS age_group
FROM (
         SELECT user_id FROM ods_user_click_log WHERE dt = '2025-07-30'
         UNION
         SELECT user_id FROM ods_page_view_log WHERE dt = '2025-07-30'
         UNION
         SELECT user_id FROM ods_trade_order WHERE dt = '2025-07-30'
     ) t;

-- 创建页面维度表
CREATE TABLE dim_page (
                          page_id STRING COMMENT '页面ID',
                          page_name STRING COMMENT '页面名称',
                          page_type STRING COMMENT '页面类型',
                          page_level INT COMMENT '页面层级'
) COMMENT '页面维度表'
    PARTITIONED BY (dt STRING COMMENT '分区日期');

-- 插入页面维度数据
INSERT OVERWRITE TABLE dim_page PARTITION (dt='2025-07-30')
SELECT DISTINCT
    page_id,
    CASE
        WHEN page_id LIKE 'home%' THEN '首页'
        WHEN page_id LIKE 'product%' THEN '商品详情页'
        WHEN page_id LIKE 'cart%' THEN '购物车页'
        WHEN page_id LIKE 'payment%' THEN '支付页'
        WHEN page_id LIKE 'category%' THEN '分类页'
        ELSE '其他页面'
        END AS page_name,
    CASE
        WHEN page_id LIKE 'home%' THEN '首页'
        WHEN page_id LIKE 'product%' THEN '商品页'
        WHEN page_id LIKE 'cart%' THEN '购物车页'
        WHEN page_id LIKE 'payment%' THEN '支付页'
        WHEN page_id LIKE 'category%' THEN '分类页'
        ELSE '其他页'
        END AS page_type,
    CASE
        WHEN page_id LIKE 'home%' THEN 1
        WHEN page_id LIKE 'category%' THEN 2
        WHEN page_id LIKE 'product%' THEN 3
        WHEN page_id LIKE 'cart%' THEN 4
        WHEN page_id LIKE 'payment%' THEN 5
        ELSE 6
        END AS page_level
FROM (
         SELECT page_id FROM ods_user_click_log WHERE dt = '2025-07-30'
         UNION
         SELECT page_id FROM ods_page_view_log WHERE dt = '2025-07-30'
         UNION
         SELECT source_page_id AS page_id FROM ods_trade_order WHERE dt = '2025-07-30'
     ) t;

-- 创建模块维度表
CREATE TABLE dim_module (
                            module_id STRING COMMENT '模块ID',
                            module_name STRING COMMENT '模块名称',
                            module_type STRING COMMENT '模块类型',
                            position STRING COMMENT '页面位置'
) COMMENT '模块维度表'
    PARTITIONED BY (dt STRING COMMENT '分区日期');

-- 插入模块维度数据
INSERT OVERWRITE TABLE dim_module PARTITION (dt='2025-07-30')
SELECT DISTINCT
    module_id,
    CASE
        WHEN module_id LIKE 'banner%' THEN '轮播图'
        WHEN module_id LIKE 'recommend%' THEN '推荐商品'
        WHEN module_id LIKE 'category%' THEN '分类导航'
        WHEN module_id LIKE 'search%' THEN '搜索框'
        WHEN module_id LIKE 'cart%' THEN '购物车'
        ELSE '其他模块'
        END AS module_name,
    CASE
        WHEN module_id LIKE 'banner%' THEN '展示类'
        WHEN module_id LIKE 'recommend%' THEN '商品类'
        WHEN module_id LIKE 'category%' THEN '导航类'
        WHEN module_id LIKE 'search%' THEN '功能类'
        WHEN module_id LIKE 'cart%' THEN '功能类'
        ELSE '其他类'
        END AS module_type,
    CASE
        WHEN module_id LIKE '%top' THEN '顶部'
        WHEN module_id LIKE '%middle' THEN '中部'
        WHEN module_id LIKE '%bottom' THEN '底部'
        ELSE '未知'
        END AS position
FROM ods_user_click_log
WHERE dt = '2025-07-30' AND module_id IS NOT NULL;


-- 创建会话维度表
CREATE TABLE dim_session (
                             session_id STRING COMMENT '会话ID',
                             user_id STRING COMMENT '用户ID',
                             start_time TIMESTAMP COMMENT '会话开始时间',
                             end_time TIMESTAMP COMMENT '会话结束时间',
                             session_duration INT COMMENT '会话时长(秒)',
                             entry_page_id STRING COMMENT '入口页面',
                             exit_page_id STRING COMMENT '退出页面',
                             page_count INT COMMENT '访问页面数',
                             is_bounced TINYINT COMMENT '是否跳出(1:是,0:否)'
) COMMENT '会话维度表'
    PARTITIONED BY (dt STRING COMMENT '分区日期');

-- 插入会话维度数据(修正版)
INSERT OVERWRITE TABLE dim_session PARTITION (dt='2025-07-30')
SELECT
    session_id,
    user_id,
    MIN(view_time) AS start_time,
    MAX(view_time) AS end_time,
    SUM(stay_time) AS session_duration,
    CONCAT('', MIN(CASE WHEN view_time IS NOT NULL THEN page_id ELSE NULL END)) AS entry_page_id,
    CONCAT('', MAX(CASE WHEN view_time IS NOT NULL THEN page_id ELSE NULL END)) AS exit_page_id,
    COUNT(DISTINCT page_id) AS page_count,
    MAX(is_bounce_flag) AS is_bounced
FROM (
         SELECT
             session_id,
             user_id,
             page_id,
             view_time,
             stay_time,
             0 AS is_bounce_flag
         FROM ods_page_view_log
         WHERE dt = '2025-07-30'

         UNION ALL

         SELECT
             session_id,
             user_id,
             page_id,
             click_time AS view_time,
             5 AS stay_time,
             is_bounce AS is_bounce_flag
         FROM ods_user_click_log
         WHERE dt = '2025-07-30'
     ) t
GROUP BY session_id, user_id;



-- 创建时间维度表
CREATE TABLE dim_date (
                          date_key STRING COMMENT '日期键(yyyyMMdd)',
                          date_value STRING COMMENT '日期值(yyyy-MM-dd)',
                          year INT COMMENT '年',
                          quarter INT COMMENT '季度',
                          month INT COMMENT '月',
                          day_of_month INT COMMENT '月中第几天',
                          day_of_week INT COMMENT '周中第几天',
                          is_weekend TINYINT COMMENT '是否周末(1:是,0:否)'
) COMMENT '时间维度表';

-- 插入时间维度数据
INSERT OVERWRITE TABLE dim_date
SELECT
    '20250730' AS date_key,
    '2025-07-30' AS date_value,
    2025 AS year,
    3 AS quarter,
    7 AS month,
    30 AS day_of_month,
    3 AS day_of_week,  -- 假设2025-07-30是周三
    0 AS is_weekend;


-- 创建订单来源页面维度表
CREATE TABLE dim_order_source_page (
                                       source_page_id STRING COMMENT '来源页面ID',
                                       source_page_name STRING COMMENT '来源页面名称',
                                       source_page_type STRING COMMENT '来源页面类型',
                                       conversion_rate DECIMAL(5,4) COMMENT '转化率'
) COMMENT '订单来源页面维度表'
    PARTITIONED BY (dt STRING COMMENT '分区日期');

-- 插入订单来源页面维度数据
INSERT OVERWRITE TABLE dim_order_source_page PARTITION (dt='2025-07-30')
SELECT DISTINCT
    source_page_id,
    CASE
        WHEN source_page_id LIKE 'product%' THEN '商品详情页'
        WHEN source_page_id LIKE 'home%' THEN '首页'
        WHEN source_page_id LIKE 'category%' THEN '分类页'
        WHEN source_page_id LIKE 'cart%' THEN '购物车页'
        ELSE '其他页面'
        END AS source_page_name,
    CASE
        WHEN source_page_id LIKE 'product%' THEN '商品页'
        WHEN source_page_id LIKE 'home%' THEN '首页'
        WHEN source_page_id LIKE 'category%' THEN '分类页'
        WHEN source_page_id LIKE 'cart%' THEN '购物车页'
        ELSE '其他页'
        END AS source_page_type,
    ROUND(RAND(), 4) AS conversion_rate
FROM ods_trade_order
WHERE dt = '2025-07-30';
