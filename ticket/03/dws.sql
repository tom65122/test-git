use traff;


CREATE TABLE IF NOT EXISTS dws_item_order_month
(
    item_id        STRING COMMENT '商品ID',
    order_user_cnt INT COMMENT '下单人数',
    order_cnt      INT COMMENT '下单次数',
    order_amt      DOUBLE COMMENT '下单总额'
)
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 清洗语句
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE dws_item_order_month PARTITION (dt)
SELECT item_id,
       COUNT(DISTINCT user_id)     AS order_user_cnt,
       COUNT(DISTINCT order_id)    AS order_cnt,
       SUM(order_price * quantity) AS order_amt,
       dt
FROM dwd_order_detail
GROUP BY item_id, dt;

CREATE TABLE IF NOT EXISTS dws_item_behavior_month
(
    item_id     STRING COMMENT '商品ID',
    view_cnt    INT COMMENT '浏览次数',
    cart_cnt    INT COMMENT '加购次数',
    collect_cnt INT COMMENT '收藏次数',
    comment_cnt INT COMMENT '评价数量',
    good_cnt    INT COMMENT '好评数',
    bad_cnt     INT COMMENT '差评数',
    avg_score   DOUBLE COMMENT '平均评分'
)
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dws_item_behavior_month PARTITION (dt)
SELECT log.item_id,
       COUNT(IF(interaction_type = 'view', 1, NULL))    AS view_cnt,
       COUNT(IF(interaction_type = 'cart', 1, NULL))    AS cart_cnt,
       COUNT(IF(interaction_type = 'collect', 1, NULL)) AS collect_cnt,
       COUNT(comment.comment_id)                        AS comment_cnt,
       COUNT(IF(comment.score >= 4, 1, NULL))           AS good_cnt,
       COUNT(IF(comment.score <= 2, 1, NULL))           AS bad_cnt,
       ROUND(AVG(comment.score), 2)                     AS avg_score,
       log.dt
FROM dwd_content_log log
         LEFT JOIN dwd_comment_info comment
                   ON log.item_id = comment.item_id AND log.dt = comment.dt
GROUP BY log.item_id, log.dt;

CREATE TABLE IF NOT EXISTS dws_item_payment_month
(
    item_id      STRING COMMENT '商品ID',
    pay_user_cnt INT COMMENT '支付人数',
    pay_cnt      INT COMMENT '支付次数',
    pay_amt      DOUBLE COMMENT '支付金额'
)
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dws_item_payment_month PARTITION (dt)
SELECT detail.item_id,
       COUNT(DISTINCT detail.user_id)     AS pay_user_cnt,
       COUNT(DISTINCT payment.payment_id) AS pay_cnt,
       SUM(payment.pay_amount)            AS pay_amt,
       payment.dt
FROM dwd_payment_info payment
         JOIN dwd_order_detail detail
              ON payment.order_id = detail.order_id
                  AND payment.dt = detail.dt
GROUP BY detail.item_id, payment.dt;


CREATE TABLE IF NOT EXISTS dws_item_conversion_month
(
    item_id         STRING COMMENT '商品ID',
    view_cnt        INT COMMENT '浏览数',
    pay_user_cnt    INT COMMENT '支付人数',
    order_user_cnt  INT COMMENT '下单人数',
    conversion_rate DOUBLE COMMENT '支付转化率 = 支付人数 / 浏览',
    repurchase_rate DOUBLE COMMENT '复购率 = 多次支付人数 / 总支付人数'
)
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 清洗插入逻辑（示例）
INSERT OVERWRITE TABLE dws_item_conversion_month PARTITION (dt)
SELECT a.item_id,
       a.view_cnt,
       b.pay_user_cnt,
       c.order_user_cnt,
       ROUND(b.pay_user_cnt / a.view_cnt, 4) AS conversion_rate,
       0.0                                   AS repurchase_rate, -- 可后续从用户维度补全
       a.dt
FROM dws_item_behavior_month a
         LEFT JOIN dws_item_payment_month b ON a.item_id = b.item_id AND a.dt = b.dt
         LEFT JOIN dws_item_order_month c ON a.item_id = c.item_id AND a.dt = c.dt;


CREATE TABLE IF NOT EXISTS dws_item_comment_month (
                                                      item_id STRING COMMENT '商品ID',
                                                      comment_cnt INT COMMENT '评论数',
                                                      good_cnt INT COMMENT '好评数',
                                                      bad_cnt INT COMMENT '差评数',
                                                      avg_score DOUBLE COMMENT '平均评分'
)
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');
INSERT OVERWRITE TABLE dws_item_comment_month PARTITION (dt)
SELECT
    item_id,
    COUNT(*) AS comment_cnt,
    COUNT(IF(score >= 4, 1, NULL)) AS good_cnt,
    COUNT(IF(score <= 2, 1, NULL)) AS bad_cnt,
    ROUND(AVG(score), 2) AS avg_score,
    dt
FROM dwd_comment_info
GROUP BY item_id, dt;

