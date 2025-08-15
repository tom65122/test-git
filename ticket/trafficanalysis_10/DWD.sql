use trafficanalysis;


-- 删除原表并重新创建（添加create_time字段）
DROP TABLE IF EXISTS dwd_click_fact;

CREATE TABLE dwd_click_fact
(
    log_id      STRING COMMENT '点击日志唯一ID',
    user_id     STRING COMMENT '用户ID',
    session_id  STRING COMMENT '会话ID',
    page_id     STRING COMMENT '页面ID',
    module_id   STRING COMMENT '模块ID',
    click_time  TIMESTAMP COMMENT '点击时间',
    is_bounce   TINYINT COMMENT '是否跳出',
    date_id     STRING COMMENT '日期维度ID',
    hour_id     INT COMMENT '小时ID',
    create_time TIMESTAMP COMMENT '记录创建时间'
) COMMENT 'DWD层-用户点击事实表'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ORC;

-- 插入数据（包含create_time字段）
INSERT OVERWRITE TABLE dwd_click_fact PARTITION (dt = '20250730')
SELECT o.log_id,
       o.user_id,
       o.session_id,
       o.page_id,
       o.module_id,
       o.click_time,
       o.is_bounce,
       '20250730'         AS date_id,
    HOUR(o.click_time) AS hour_id,
    o.create_time      AS create_time
FROM ods_user_click_log o
WHERE o.dt = '20250730';

select * from dwd_click_fact;

-- 删除原表并重新创建（添加create_time字段）
DROP TABLE IF EXISTS dwd_page_view_fact;

CREATE TABLE dwd_page_view_fact
(
    log_id      STRING COMMENT '访问日志唯一ID',
    user_id     STRING COMMENT '用户ID',
    session_id  STRING COMMENT '会话ID',
    page_id     STRING COMMENT '页面ID',
    view_time   TIMESTAMP COMMENT '页面访问时间',
    stay_time   INT COMMENT '页面停留时间',
    date_id     STRING COMMENT '日期维度ID',
    hour_id     INT COMMENT '小时ID',
    create_time TIMESTAMP COMMENT '记录创建时间'
) COMMENT 'DWD层-页面浏览事实表'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ORC;

-- 插入数据（包含create_time字段）
INSERT OVERWRITE TABLE dwd_page_view_fact PARTITION (dt = '20250730')
SELECT o.log_id,
       o.user_id,
       o.session_id,
       o.page_id,
       o.view_time,
       o.stay_time,
       '20250730'        AS date_id,
    HOUR(o.view_time) AS hour_id,
    o.create_time     AS create_time
FROM ods_page_view_log o
WHERE o.dt = '20250730';

select * from dwd_page_view_fact;

-- 删除原表并重新创建（添加create_time字段）
DROP TABLE IF EXISTS dwd_order_fact;

CREATE TABLE dwd_order_fact
(
    order_id        STRING COMMENT '订单唯一ID',
    user_id         STRING COMMENT '下单用户ID',
    order_time      TIMESTAMP COMMENT '下单时间',
    payment_amount  DECIMAL(16, 2) COMMENT '支付金额',
    source_page_id  STRING COMMENT '订单来源页面ID',
    date_id         STRING COMMENT '日期维度ID',
    hour_id         INT COMMENT '小时ID',
    create_time     TIMESTAMP COMMENT '记录创建时间'
) COMMENT 'DWD层-订单事实表'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ORC;

-- 插入数据（包含create_time字段）
INSERT OVERWRITE TABLE dwd_order_fact PARTITION (dt = '20250730')
SELECT o.order_id,
       o.user_id,
       o.order_time,
       o.payment_amount,
       o.source_page_id,
       '20250730'         AS date_id,
    HOUR(o.order_time) AS hour_id,
    o.create_time      AS create_time
FROM ods_trade_order o
WHERE o.dt = '20250730';

select * from dwd_order_fact;

-- 删除原表并重新创建（添加create_time字段）
DROP TABLE IF EXISTS dwd_session_fact;

CREATE TABLE dwd_session_fact
(
    session_id       STRING COMMENT '会话ID',
    user_id          STRING COMMENT '用户ID',
    start_time       TIMESTAMP COMMENT '会话开始时间',
    end_time         TIMESTAMP COMMENT '会话结束时间',
    session_duration INT COMMENT '会话时长(秒)',
    entry_page_id    STRING COMMENT '入口页面ID',
    exit_page_id     STRING COMMENT '退出页面ID',
    page_count       INT COMMENT '访问页面数',
    click_count      INT COMMENT '点击次数',
    is_bounced       TINYINT COMMENT '是否跳出',
    date_id          STRING COMMENT '日期维度ID',
    create_time      TIMESTAMP COMMENT '记录创建时间'
) COMMENT 'DWD层-会话事实表'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ORC;

-- 插入数据（包含create_time字段）
INSERT OVERWRITE TABLE dwd_session_fact PARTITION (dt = '20250730')
SELECT s.session_id,
       s.user_id,
       s.start_time,
       s.end_time,
       s.session_duration,
       s.entry_page_id,
       s.exit_page_id,
       s.page_count,
       COALESCE(c.click_count, 0) AS click_count,
       s.is_bounced,
       '20250730'                 AS date_id,
       s.create_time              AS create_time
FROM dim_session s
         LEFT JOIN (SELECT session_id,
                           COUNT(*) AS click_count
                    FROM ods_user_click_log
                    WHERE dt = '20250730'
                    GROUP BY session_id) c ON s.session_id = c.session_id
WHERE s.dt = '20250730';

select * from dwd_session_fact;


-- 删除原表并重新创建（添加create_time字段）
DROP TABLE IF EXISTS dwd_user_behavior_fact;

CREATE TABLE dwd_user_behavior_fact
(
    user_id         STRING COMMENT '用户ID',
    date_id         STRING COMMENT '日期维度ID',
    pv_count        INT COMMENT '页面浏览次数',
    click_count     INT COMMENT '点击次数',
    session_count   INT COMMENT '会话次数',
    total_stay_time INT COMMENT '总停留时间(秒)',
    bounce_count    INT COMMENT '跳出次数',
    order_count     INT COMMENT '下单次数',
    order_amount    DECIMAL(16, 2) COMMENT '下单金额',
    create_time     TIMESTAMP COMMENT '记录创建时间'
) COMMENT 'DWD层-用户行为汇总事实表'
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    STORED AS ORC;

-- 插入数据（优化版本）
INSERT OVERWRITE TABLE dwd_user_behavior_fact PARTITION (dt = '20250730')
SELECT u.user_id,
       '20250730' AS date_id,
       COALESCE(pv.pv_count, 0) AS pv_count,
       COALESCE(click.click_count, 0) AS click_count,
       COALESCE(session.session_count, 0) AS session_count,
       COALESCE(pv.total_stay_time, 0) AS total_stay_time,
       COALESCE(bounce.bounce_count, 0) AS bounce_count,
       COALESCE(ord.order_count, 0) AS order_count,
       COALESCE(ord.order_amount, 0.00) AS order_amount,
       GREATEST(
               COALESCE(pv.create_time, '1970-01-01 00:00:00'),
               COALESCE(click.create_time, '1970-01-01 00:00:00'),
               COALESCE(ord.create_time, '1970-01-01 00:00:00')
           ) AS create_time
FROM (
         SELECT DISTINCT user_id
         FROM (
                  SELECT user_id FROM ods_user_click_log WHERE dt = '20250730'
                  UNION
                  SELECT user_id FROM ods_page_view_log WHERE dt = '20250730'
                  UNION
                  SELECT user_id FROM ods_trade_order WHERE dt = '20250730'
              ) t
     ) u
         LEFT JOIN (
    SELECT user_id,
           COUNT(*) AS pv_count,
           SUM(stay_time) AS total_stay_time,
           MAX(create_time) AS create_time
    FROM ods_page_view_log
    WHERE dt = '20250730'
    GROUP BY user_id
) pv ON u.user_id = pv.user_id
         LEFT JOIN (
    SELECT user_id,
           COUNT(*) AS click_count,
           MAX(create_time) AS create_time
    FROM ods_user_click_log
    WHERE dt = '20250730'
    GROUP BY user_id
) click ON u.user_id = click.user_id
         LEFT JOIN (
    SELECT user_id,
           COUNT(*) AS session_count
    FROM dim_session
    WHERE dt = '20250730'
    GROUP BY user_id
) session ON u.user_id = session.user_id
    LEFT JOIN (
    SELECT user_id,
    COUNT(*) AS bounce_count
    FROM dim_session
    WHERE dt = '20250730'
    AND is_bounced = 1
    GROUP BY user_id
    ) bounce ON u.user_id = bounce.user_id
    LEFT JOIN (
    SELECT user_id,
    COUNT(*) AS order_count,
    SUM(payment_amount) AS order_amount,
    MAX(create_time) AS create_time
    FROM ods_trade_order
    WHERE dt = '20250730'
    GROUP BY user_id
    ) ord ON u.user_id = ord.user_id;



select  * from dwd_user_behavior_fact;