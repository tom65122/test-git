create database if not exists traff;

use traff;


--商品信息表（ods_product_info）
CREATE TABLE IF NOT EXISTS ods_product_info
(
    item_id     STRING,
    item_name   STRING,
    category_id STRING,
    brand_id    STRING,
    create_time STRING,
    is_deleted  INT
)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE TABLE IF NOT EXISTS tmp_ods_product_info
(
    item_id     STRING,
    item_name   STRING,
    category_id STRING,
    brand_id    STRING,
    create_time STRING,
    is_deleted  INT
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;

LOAD DATA INPATH '/data/ods_product_info.txt' INTO TABLE tmp_ods_product_info;

INSERT OVERWRITE TABLE ods_product_info
SELECT *
FROM tmp_ods_product_info;


--SKU 信息表（ods_sku_info）
CREATE TABLE IF NOT EXISTS ods_sku_info
(
    sku_id      STRING,
    item_id     STRING,
    spec        STRING,
    stock       INT,
    sku_price   DOUBLE,
    create_time STRING
)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE TABLE IF NOT EXISTS tmp_ods_sku_info
(
    sku_id      STRING,
    item_id     STRING,
    spec        STRING,
    stock       INT,
    sku_price   DOUBLE,
    create_time STRING
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;

LOAD DATA INPATH '/data/ods_sku_info.txt' INTO TABLE tmp_ods_sku_info;

INSERT OVERWRITE TABLE ods_sku_info
SELECT *
FROM tmp_ods_sku_info;
--订单明细表（ods_order_detail）
CREATE TABLE IF NOT EXISTS ods_order_detail
(
    order_id     STRING,
    user_id      STRING,
    item_id      STRING,
    sku_id       STRING,
    order_price  DOUBLE,
    quantity     INT,
    order_time   STRING,
    order_status STRING
)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE TABLE IF NOT EXISTS tmp_ods_order_detail
(
    order_id     STRING,
    user_id      STRING,
    item_id      STRING,
    sku_id       STRING,
    order_price  DOUBLE,
    quantity     INT,
    order_time   STRING,
    order_status STRING
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;

LOAD DATA INPATH '/data/ods_order_detail.txt' INTO TABLE tmp_ods_order_detail;

INSERT OVERWRITE TABLE ods_order_detail
SELECT *
FROM tmp_ods_order_detail;
--支付信息表（ods_payment_info）
CREATE TABLE IF NOT EXISTS ods_payment_info
(
    payment_id  STRING,
    order_id    STRING,
    pay_amount  DOUBLE,
    pay_time    STRING,
    pay_channel STRING
)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE TABLE IF NOT EXISTS tmp_ods_payment_info
(
    payment_id  STRING,
    order_id    STRING,
    pay_amount  DOUBLE,
    pay_time    STRING,
    pay_channel STRING
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;

LOAD DATA INPATH '/data/ods_payment_info.txt' INTO TABLE tmp_ods_payment_info;

INSERT OVERWRITE TABLE ods_payment_info
SELECT *
FROM tmp_ods_payment_info;
--用户信息表（ods_user_info）
CREATE TABLE IF NOT EXISTS ods_user_info
(
    user_id       STRING,
    gender        STRING,
    age           INT,
    region        STRING,
    register_time STRING
)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE TABLE IF NOT EXISTS tmp_ods_user_info
(
    user_id       STRING,
    gender        STRING,
    age           INT,
    region        STRING,
    register_time STRING
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;

LOAD DATA INPATH '/data/ods_user_info.txt' INTO TABLE tmp_ods_user_info;

INSERT OVERWRITE TABLE ods_user_info
SELECT *
FROM tmp_ods_user_info;
--搜索日志表（ods_search_log）
CREATE TABLE IF NOT EXISTS ods_search_log
(
    search_id   STRING,
    user_id     STRING,
    keyword     STRING,
    search_time STRING
)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE TABLE IF NOT EXISTS tmp_ods_search_log
(
    search_id   STRING,
    user_id     STRING,
    keyword     STRING,
    search_time STRING
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;

LOAD DATA INPATH '/data/ods_search_log.txt' INTO TABLE tmp_ods_search_log;

INSERT OVERWRITE TABLE ods_search_log
SELECT *
FROM tmp_ods_search_log;
--内容互动日志表（ods_content_log
CREATE TABLE IF NOT EXISTS ods_content_log
(
    content_id       STRING,
    user_id          STRING,
    item_id          STRING,
    content_type     STRING,
    interaction_type STRING,
    interaction_time STRING
)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE TABLE IF NOT EXISTS tmp_ods_content_log
(
    content_id       STRING,
    user_id          STRING,
    item_id          STRING,
    content_type     STRING,
    interaction_type STRING,
    interaction_time STRING
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;

LOAD DATA INPATH '/data/ods_content_log.txt' INTO TABLE tmp_ods_content_log;

INSERT OVERWRITE TABLE ods_content_log
SELECT *
FROM tmp_ods_content_log;
--商品评价信息表（ods_comment_info
CREATE TABLE IF NOT EXISTS ods_comment_info
(
    comment_id   STRING,
    user_id      STRING,
    item_id      STRING,
    sku_id       STRING,
    comment_time STRING,
    score        DOUBLE,
    comment_text STRING
)
    STORED AS ORC
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

CREATE TABLE IF NOT EXISTS tmp_ods_comment_info
(
    comment_id   STRING,
    user_id      STRING,
    item_id      STRING,
    sku_id       STRING,
    comment_time STRING,
    score        DOUBLE,
    comment_text STRING
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;

LOAD DATA INPATH '/data/ods_comment_info.txt' INTO TABLE tmp_ods_comment_info;

INSERT OVERWRITE TABLE ods_comment_info
SELECT *
FROM tmp_ods_comment_info;

