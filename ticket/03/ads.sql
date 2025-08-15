use traff;

CREATE TABLE IF NOT EXISTS ads_item_360
(
    -- 基本信息
    item_id           STRING COMMENT '商品ID',
    dt                STRING COMMENT '统计周期（月）',

    -- 1. 销售表现
    order_cnt         BIGINT COMMENT '下单次数',
    order_user_cnt    BIGINT COMMENT '下单人数',
    order_amt         DOUBLE COMMENT '下单总金额',
    pay_cnt           BIGINT COMMENT '支付次数',
    pay_user_cnt      BIGINT COMMENT '支付人数',
    pay_amt           DOUBLE COMMENT '支付总金额',

    -- 2. 用户行为
    view_cnt          BIGINT COMMENT '浏览次数',
    cart_cnt          BIGINT COMMENT '加购次数',
    collect_cnt       BIGINT COMMENT '收藏次数',
    conversion_rate   DOUBLE COMMENT '转化率（支付用户数 / 浏览）',
    repurchase_rate   DOUBLE COMMENT '复购率（多次支付用户数 / 支付用户数）',

    -- 3. 评价表现
    comment_cnt       BIGINT COMMENT '评价总数',
    avg_score         DOUBLE COMMENT '平均评分',

    -- 4. 内容互动
    content_exp_cnt   BIGINT COMMENT '内容曝光次数（view）',
    content_click_cnt BIGINT COMMENT '内容点击次数（click）',

    -- 5. 商品信息
    sku_cnt           INT COMMENT 'SKU 数量',
    avg_price         DOUBLE COMMENT '平均 SKU 价格'
)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE ads_item_360
SELECT a.item_id,
       a.dt,

       -- 销售指标
       a.order_cnt,
       a.order_user_cnt,
       a.order_amt,
       COALESCE(b.pay_cnt, 0)                                                                                     AS pay_cnt,
       COALESCE(b.pay_user_cnt, 0)                                                                                AS pay_user_cnt,
       COALESCE(b.pay_amt, 0.0)                                                                                   AS pay_amt,

       -- 用户行为
       COALESCE(c.view_cnt, 0)                                                                                    AS view_cnt,
       COALESCE(c.cart_cnt, 0)                                                                                    AS cart_cnt,
       COALESCE(c.collect_cnt, 0)                                                                                 AS collect_cnt,
       CASE
           WHEN COALESCE(c.view_cnt, 0) > 0 THEN ROUND(COALESCE(b.pay_user_cnt, 0) / c.view_cnt, 4)
           ELSE 0.0 END                                                                                           AS conversion_rate,
       0.0                                                                                                        AS repurchase_rate,

       -- 用户反馈
       COALESCE(e.comment_cnt, 0)                                                                                 AS comment_cnt,
       COALESCE(e.avg_score, 0.0)                                                                                 AS avg_score,

       -- 内容表现
       COALESCE(d.view_cnt, 0)                                                                                    AS content_exp_cnt,
       COALESCE(d.click_cnt, 0)                                                                                   AS content_click_cnt,

       -- 商品属性
       COALESCE(s.sku_cnt, 0)                                                                                     AS sku_cnt,
       COALESCE(s.avg_price, 0.0)                                                                                 AS avg_price

FROM dws_item_order_month a
         LEFT JOIN dws_item_payment_month b
                   ON a.item_id = b.item_id AND a.dt = b.dt
         LEFT JOIN dws_item_behavior_month c
                   ON a.item_id = c.item_id AND a.dt = c.dt
         LEFT JOIN (SELECT item_id,
                           COUNT(CASE WHEN interaction_type = 'view' THEN 1 END)  AS view_cnt,
                           COUNT(CASE WHEN interaction_type = 'click' THEN 1 END) AS click_cnt,
                           dt
                    FROM dwd_content_log
                    GROUP BY item_id, dt) d ON a.item_id = d.item_id AND a.dt = d.dt
         LEFT JOIN dws_item_comment_month e
                   ON a.item_id = e.item_id AND a.dt = e.dt
         LEFT JOIN (SELECT item_id,
                           COUNT(*)                 AS sku_cnt,
                           ROUND(AVG(sku_price), 2) AS avg_price
                    FROM dwd_sku_info
                    GROUP BY item_id) s ON a.item_id = s.item_id;
