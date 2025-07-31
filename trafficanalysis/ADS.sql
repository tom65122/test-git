use trafficanalysis;

-- 创建页面板块分析表
DROP TABLE IF EXISTS ads_page_module_analysis;

CREATE TABLE ads_page_module_analysis
(
    page_id              STRING COMMENT '页面ID',
    page_name            STRING COMMENT '页面名称',
    module_id            STRING COMMENT '模块ID',
    module_name          STRING COMMENT '模块名称',
    module_type          STRING COMMENT '模块类型',
    date_id              STRING COMMENT ' 日期维度ID',
    -- 点击指标
    click_count          BIGINT COMMENT '点击次数',
    click_user_count     BIGINT COMMENT '点击用户数',
    -- 订单指标
    order_count          BIGINT COMMENT '订单数',
    order_user_count     BIGINT COMMENT '下单用户数',
    order_amount         DECIMAL(16, 2) COMMENT '引导支付金额',
    -- 转化率指标
    click_conversion_rate DECIMAL(5, 4) COMMENT '点击转化率',
    create_time          TIMESTAMP COMMENT '记录创建时间'
) COMMENT 'ADS层-页面板块分析表'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ORC;

-- 插入页面板块分析数据
INSERT OVERWRITE TABLE ads_page_module_analysis PARTITION (dt = '20250730')
SELECT
    m.page_id,
    p.page_name,
    m.module_id,
    m.module_name,
    dm.module_type,
    '20250730' AS date_id,
    m.click_count,
    m.click_user_count,
    COALESCE(ord.order_count, 0) AS order_count,
    COALESCE(ord.order_user_count, 0) AS order_user_count,
    COALESCE(ord.order_amount, 0.00) AS order_amount,
    CASE
        WHEN m.click_user_count > 0 THEN
                CAST(COALESCE(ord.order_user_count, 0) AS DECIMAL(10,4)) / CAST(m.click_user_count AS DECIMAL(10,4))
        ELSE 0
        END AS click_conversion_rate,
    CURRENT_TIMESTAMP() AS create_time
FROM dws_module_stats_1d m
         JOIN dim_page p ON m.page_id = p.page_id
         JOIN dim_module dm ON m.module_id = dm.module_id
         LEFT JOIN (
    SELECT
        c.page_id,
        c.module_id,
        COUNT(DISTINCT o.user_id) AS order_user_count,
        COUNT(*) AS order_count,
        SUM(o.payment_amount) AS order_amount
    FROM dwd_click_fact c
             JOIN dwd_order_fact o ON c.user_id = o.user_id
    WHERE c.dt = '20250730' AND o.dt = '20250730'
    GROUP BY c.page_id, c.module_id
) ord ON m.page_id = ord.page_id AND m.module_id = ord.module_id
WHERE m.dt = '20250730' AND p.dt = '20250730' AND dm.dt = '20250730';



-- 创建页面访问趋势分析表
DROP TABLE IF EXISTS ads_page_trend_analysis;

CREATE TABLE ads_page_trend_analysis
(
    page_id              STRING COMMENT '页面ID',
    page_name            STRING COMMENT '页面名称',
    date_id              STRING COMMENT '日期维度ID',
    -- 流量指标
    pv_count             BIGINT COMMENT '页面浏览次数',
    visitor_count        BIGINT COMMENT '访客数',
    click_count          BIGINT COMMENT '点击次数',
    click_user_count     BIGINT COMMENT '点击用户数',
    -- 跳出指标
    bounce_count         BIGINT COMMENT '跳出次数',
    bounce_rate          DECIMAL(5, 4) COMMENT '跳出率',
    -- 订单指标
    order_count          BIGINT COMMENT '订单数',
    order_user_count     BIGINT COMMENT '下单用户数',
    order_amount         DECIMAL(16, 2) COMMENT '订单金额',
    -- 转化率指标
    visitor_conversion_rate DECIMAL(5, 4) COMMENT '访客转化率',
    create_time          TIMESTAMP COMMENT '记录创建时间'
) COMMENT 'ADS层-页面访问趋势分析表'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ORC;

-- 插入页面访问趋势分析数据
INSERT OVERWRITE TABLE ads_page_trend_analysis PARTITION (dt = '20250730')
SELECT
    p.page_id,
    p.page_name,
    '20250730' AS date_id,
    ps.pv_count,
    ps.visitor_count,
    ps.click_count,
    ps.click_count AS click_user_count, -- 简化处理，实际应统计独立点击用户数
    ps.bounce_count,
    CASE
        WHEN ps.session_count > 0 THEN
                CAST(ps.bounce_count AS DECIMAL(10,4)) / CAST(ps.session_count AS DECIMAL(10,4))
        ELSE 0
        END AS bounce_rate,
    ps.order_count,
    ps.order_user_count,
    ps.order_amount,
    CASE
        WHEN ps.visitor_count > 0 THEN
                CAST(ps.order_user_count AS DECIMAL(10,4)) / CAST(ps.visitor_count AS DECIMAL(10,4))
        ELSE 0
        END AS visitor_conversion_rate,
    CURRENT_TIMESTAMP() AS create_time
FROM dws_page_stats_1d ps
         JOIN dim_page p ON ps.page_id = p.page_id
WHERE ps.dt = '20250730' AND p.dt = '20250730';


-- 创建时间段访问分析表
DROP TABLE IF EXISTS ads_hourly_visit_analysis;

CREATE TABLE ads_hourly_visit_analysis
(
    hour_id              INT COMMENT '小时ID',
    date_id              STRING COMMENT '日期维度ID',
    -- 流量指标
    pv_count             BIGINT COMMENT '页面浏览次数',
    visitor_count        BIGINT COMMENT '访客数',
    click_count          BIGINT COMMENT '点击次数',
    -- 订单指标
    order_count          BIGINT COMMENT '订单数',
    order_amount         DECIMAL(16, 2) COMMENT '订单金额',
    -- 转化率指标
    conversion_rate      DECIMAL(5, 4) COMMENT '转化率',
    create_time          TIMESTAMP COMMENT '记录创建时间'
) COMMENT 'ADS层-时间段访问分析表'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ORC;

-- 插入时间段访问分析数据
INSERT OVERWRITE TABLE ads_hourly_visit_analysis PARTITION (dt = '20250730')
SELECT
    pv.hour_id,
    '20250730' AS date_id,
    COUNT(*) AS pv_count,
    COUNT(DISTINCT pv.user_id) AS visitor_count,
    COALESCE(click.click_count, 0) AS click_count,
    COALESCE(ord.order_count, 0) AS order_count,
    COALESCE(ord.order_amount, 0.00) AS order_amount,
    CASE
        WHEN COUNT(DISTINCT pv.user_id) > 0 THEN
                CAST(COALESCE(ord.order_count, 0) AS DECIMAL(10,4)) / CAST(COUNT(DISTINCT pv.user_id) AS DECIMAL(10,4))
        ELSE 0
        END AS conversion_rate,
    CURRENT_TIMESTAMP() AS create_time
FROM dwd_page_view_fact pv
         LEFT JOIN (
    SELECT
        hour_id,
        COUNT(*) AS click_count
    FROM dwd_click_fact
    WHERE dt = '20250730'
    GROUP BY hour_id
) click ON pv.hour_id = click.hour_id
         LEFT JOIN (
    SELECT
        HOUR(order_time) AS hour_id,
        COUNT(*) AS order_count,
        SUM(payment_amount) AS order_amount
    FROM dwd_order_fact
    WHERE dt = '20250730'
    GROUP BY HOUR(order_time)
) ord ON pv.hour_id = ord.hour_id
WHERE pv.dt = '20250730'
GROUP BY pv.hour_id, click.click_count, ord.order_count, ord.order_amount;


-- 创建活动效果分析表
DROP TABLE IF EXISTS ads_campaign_effect_analysis;

CREATE TABLE ads_campaign_effect_analysis
(
    page_id              STRING COMMENT '页面ID',
    page_name            STRING COMMENT '页面名称',
    date_id              STRING COMMENT '日期维度ID',
    -- 流量指标
    pv_count             BIGINT COMMENT '页面浏览次数',
    visitor_count        BIGINT COMMENT '访客数',
    click_count          BIGINT COMMENT '点击次数',
    -- 订单指标
    order_count          BIGINT COMMENT '订单数',
    order_user_count     BIGINT COMMENT '下单用户数',
    order_amount         DECIMAL(16, 2) COMMENT '订单金额',
    -- 转化率指标
    visitor_conversion_rate DECIMAL(5, 4) COMMENT '访客转化率',
    click_conversion_rate   DECIMAL(5, 4) COMMENT '点击转化率',
    create_time          TIMESTAMP COMMENT '记录创建时间'
) COMMENT 'ADS层-活动效果分析表'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ORC;

-- 插入活动效果分析数据
INSERT OVERWRITE TABLE ads_campaign_effect_analysis PARTITION (dt = '20250730')
SELECT
    p.page_id,
    p.page_name,
    '20250730' AS date_id,
    ps.pv_count,
    ps.visitor_count,
    ps.click_count,
    ps.order_count,
    ps.order_user_count,
    ps.order_amount,
    CASE
        WHEN ps.visitor_count > 0 THEN
                CAST(ps.order_user_count AS DECIMAL(10,4)) / CAST(ps.visitor_count AS DECIMAL(10,4))
        ELSE 0
        END AS visitor_conversion_rate,
    CASE
        WHEN ps.click_count > 0 THEN
                CAST(ps.order_count AS DECIMAL(10,4)) / CAST(ps.click_count AS DECIMAL(10,4))
        ELSE 0
        END AS click_conversion_rate,
    CURRENT_TIMESTAMP() AS create_time
FROM dws_page_stats_1d ps
         JOIN dim_page p ON ps.page_id = p.page_id
WHERE ps.dt = '20250730' AND p.dt = '20250730';


-- 创建用户行为偏好分析表
DROP TABLE IF EXISTS ads_user_behavior_preference;

CREATE TABLE ads_user_behavior_preference
(
    user_id              STRING COMMENT '用户ID',
    date_id              STRING COMMENT '日期维度ID',
    -- 偏好指标
    preferred_page_type  STRING COMMENT '偏好的页面类型',
    preferred_module_type STRING COMMENT '偏好的模块类型',
    total_pv_count       BIGINT COMMENT '总浏览次数',
    total_click_count    BIGINT COMMENT '总点击次数',
    total_order_amount   DECIMAL(16, 2) COMMENT '总订单金额',
    create_time          TIMESTAMP COMMENT '记录创建时间'
) COMMENT 'ADS层-用户行为偏好分析表'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ORC;

-- 插入用户行为偏好分析数据
INSERT OVERWRITE TABLE ads_user_behavior_preference PARTITION (dt = '20250730')
SELECT
    ub.user_id,
    '20250730' AS date_id,
    -- 简化处理，实际应根据用户行为数据计算偏好
    FIRST_VALUE(p.page_type) OVER (PARTITION BY ub.user_id ORDER BY ub.pv_count DESC) AS preferred_page_type,
        FIRST_VALUE(m.module_type) OVER (PARTITION BY ub.user_id ORDER BY click.click_count DESC) AS preferred_module_type,
        ub.pv_count AS total_pv_count,
    ub.click_count AS total_click_count,
    ub.order_amount AS total_order_amount,
    CURRENT_TIMESTAMP() AS create_time
FROM dws_user_stats_1d ub
         LEFT JOIN dim_page p ON ub.user_id = p.page_id -- 简化关联
         LEFT JOIN dim_module m ON ub.user_id = m.module_id -- 简化关联
         LEFT JOIN (
    SELECT user_id, COUNT(*) AS click_count
    FROM dwd_click_fact
    WHERE dt = '20250730'
    GROUP BY user_id
) click ON ub.user_id = click.user_id
WHERE ub.dt = '20250730';
