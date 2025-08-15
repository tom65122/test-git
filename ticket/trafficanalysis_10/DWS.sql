use trafficanalysis;

-- 删除原表并重新创建（添加create_time字段）
DROP TABLE IF EXISTS dws_page_stats_1d;

CREATE TABLE dws_page_stats_1d
(
    page_id          STRING COMMENT '页面ID',
    page_name        STRING COMMENT '页面名称',
    date_id          STRING COMMENT '日期维度ID',
    -- 流量指标
    pv_count         BIGINT COMMENT '页面浏览次数',
    click_count      BIGINT COMMENT '页面点击次数',
    visitor_count    BIGINT COMMENT '访客数',
    session_count    BIGINT COMMENT '会话数',
    -- 跳出指标
    bounce_count     BIGINT COMMENT '跳出次数',
    -- 订单指标
    order_count      BIGINT COMMENT '订单数',
    order_amount     DECIMAL(16, 2) COMMENT '订单金额',
    order_user_count BIGINT COMMENT '下单用户数',
    create_time      TIMESTAMP COMMENT '记录创建时间'
) COMMENT 'DWS层-页面主题宽表(按日统计)'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ORC;

-- 插入数据（包含create_time字段）
INSERT OVERWRITE TABLE dws_page_stats_1d PARTITION (dt = '20250730')
SELECT p.page_id,
       p.page_name,
       '20250730'                         AS date_id,
       -- 流量指标
       COALESCE(pv.pv_count, 0)           AS pv_count,
       COALESCE(click.click_count, 0)     AS click_count,
       COALESCE(visitor.visitor_count, 0) AS visitor_count,
       COALESCE(session.session_count, 0) AS session_count,
       -- 跳出指标
       COALESCE(bounce.bounce_count, 0)   AS bounce_count,
       -- 订单指标
       COALESCE(ord.order_count, 0)       AS order_count,
       COALESCE(ord.order_amount, 0.00)   AS order_amount,
       COALESCE(ord.order_user_count, 0)  AS order_user_count,
       CURRENT_TIMESTAMP()                AS create_time
FROM dim_page p
         LEFT JOIN (SELECT page_id,
                           COUNT(*) AS pv_count
                    FROM dwd_page_view_fact
                    WHERE dt = '20250730'
                    GROUP BY page_id) pv ON p.page_id = pv.page_id
         LEFT JOIN (SELECT page_id,
                           COUNT(*) AS click_count
                    FROM dwd_click_fact
                    WHERE dt = '20250730'
                    GROUP BY page_id) click ON p.page_id = click.page_id
         LEFT JOIN (SELECT page_id,
                           COUNT(DISTINCT user_id) AS visitor_count
                    FROM dwd_page_view_fact
                    WHERE dt = '20250730'
                    GROUP BY page_id) visitor ON p.page_id = visitor.page_id
         LEFT JOIN (SELECT entry_page_id AS page_id,
                           COUNT(*)      AS session_count
                    FROM dwd_session_fact
                    WHERE dt = '20250730'
                    GROUP BY entry_page_id) session ON p.page_id = session.page_id
    LEFT JOIN (SELECT exit_page_id AS page_id,
    COUNT(*)     AS bounce_count
    FROM dwd_session_fact
    WHERE dt = '20250730'
    AND is_bounced = 1
    GROUP BY exit_page_id) bounce ON p.page_id = bounce.page_id
    LEFT JOIN (SELECT source_page_id          AS page_id,
    COUNT(*)                AS order_count,
    SUM(payment_amount)     AS order_amount,
    COUNT(DISTINCT user_id) AS order_user_count
    FROM dwd_order_fact
    WHERE dt = '20250730'
    GROUP BY source_page_id) ord ON p.page_id = ord.page_id
WHERE p.dt = '20250730';


-- 删除原表并重新创建（添加create_time字段）
DROP TABLE IF EXISTS dws_user_stats_1d;

CREATE TABLE dws_user_stats_1d
(
    user_id         STRING COMMENT '用户ID',
    date_id         STRING COMMENT '日期维度ID',
    -- 流量指标
    pv_count        BIGINT COMMENT '页面浏览次数',
    click_count     BIGINT COMMENT '点击次数',
    session_count   BIGINT COMMENT '会话次数',
    total_stay_time BIGINT COMMENT '总停留时间(秒)',
    -- 跳出指标
    bounce_count    BIGINT COMMENT '跳出次数',
    -- 订单指标
    order_count     BIGINT COMMENT '下单次数',
    order_amount    DECIMAL(16, 2) COMMENT '下单金额',
    create_time     TIMESTAMP COMMENT '记录创建时间'
) COMMENT 'DWS层-用户主题宽表(按日统计)'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ORC;

-- 插入数据（包含create_time字段）
INSERT OVERWRITE TABLE dws_user_stats_1d PARTITION (dt = '20250730')
SELECT user_id,
       '20250730' AS date_id,
       pv_count,
       click_count,
       session_count,
       total_stay_time,
       bounce_count,
       order_count,
       order_amount,
       CURRENT_TIMESTAMP() AS create_time
FROM dwd_user_behavior_fact
WHERE dt = '20250730';


-- 删除原表并重新创建（添加create_time字段）
DROP TABLE IF EXISTS dws_module_stats_1d;

CREATE TABLE dws_module_stats_1d
(
    module_id        STRING COMMENT '模块ID',
    module_name      STRING COMMENT '模块名称',
    page_id          STRING COMMENT '页面ID',
    date_id          STRING COMMENT '日期维度ID',
    -- 点击指标
    click_count      BIGINT COMMENT '点击次数',
    click_user_count BIGINT COMMENT '点击用户数',
    create_time      TIMESTAMP COMMENT '记录创建时间'
) COMMENT 'DWS层-模块主题宽表(按日统计)'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ORC;

-- 插入数据（包含create_time字段）
INSERT OVERWRITE TABLE dws_module_stats_1d PARTITION (dt = '20250730')
SELECT c.module_id,
       MAX(m.module_name) AS module_name,
       c.page_id,
       '20250730' AS date_id,
       COUNT(*) AS click_count,
       COUNT(DISTINCT c.user_id) AS click_user_count,
       CURRENT_TIMESTAMP() AS create_time
FROM dwd_click_fact c
         JOIN dim_module m ON c.module_id = m.module_id
WHERE c.dt = '20250730'
  AND m.dt = '20250730'
GROUP BY c.module_id, c.page_id;


-- 删除原表并重新创建（添加create_time字段）
DROP TABLE IF EXISTS dws_session_stats_1d;

CREATE TABLE dws_session_stats_1d
(
    session_id       STRING COMMENT '会话ID',
    user_id          STRING COMMENT '用户ID',
    date_id          STRING COMMENT '日期维度ID',
    -- 会话指标
    session_duration INT COMMENT '会话时长(秒)',
    page_count       INT COMMENT '访问页面数',
    click_count      INT COMMENT '点击次数',
    is_bounced       TINYINT COMMENT '是否跳出',
    create_time      TIMESTAMP COMMENT '记录创建时间'
) COMMENT 'DWS层-会话主题宽表(按日统计)'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ORC;

-- 插入数据（包含create_time字段）
INSERT OVERWRITE TABLE dws_session_stats_1d PARTITION (dt = '20250730')
SELECT session_id,
       user_id,
       date_id,
       session_duration,
       page_count,
       click_count,
       is_bounced,
       CURRENT_TIMESTAMP() AS create_time
FROM dwd_session_fact
WHERE dt = '20250730';


-- 删除原表并重新创建（添加create_time字段）
DROP TABLE IF EXISTS dws_page_path_1d;

CREATE TABLE dws_page_path_1d
(
    source_page_id STRING COMMENT '来源页面ID',
    target_page_id STRING COMMENT '目标页面ID',
    date_id        STRING COMMENT '日期维度ID',
    path_count     BIGINT COMMENT '路径次数',
    create_time    TIMESTAMP COMMENT '记录创建时间'
) COMMENT 'DWS层-页面路径分析表(按日统计)'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ORC;

-- 插入数据（包含create_time字段）
INSERT OVERWRITE TABLE dws_page_path_1d PARTITION (dt = '20250730')
SELECT prev_page.page_id AS source_page_id,
       curr_page.page_id AS target_page_id,
       '20250730'        AS date_id,
       COUNT(*)          AS path_count,
       CURRENT_TIMESTAMP() AS create_time
FROM (SELECT session_id,
             page_id,
             view_time,
             ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY view_time) AS page_seq
      FROM dwd_page_view_fact
      WHERE dt = '20250730') curr_page
         JOIN (SELECT session_id,
                      page_id,
                      view_time,
                      ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY view_time) AS page_seq
               FROM dwd_page_view_fact
               WHERE dt = '20250730') prev_page
              ON curr_page.session_id = prev_page.session_id
                  AND curr_page.page_seq = prev_page.page_seq + 1
GROUP BY prev_page.page_id, curr_page.page_id;


