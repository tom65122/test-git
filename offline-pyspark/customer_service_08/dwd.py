# 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板
# DWD层实现代码

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("CustomerServiceCouponDWD") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .enableHiveSupport() \
    .getOrCreate()

java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.Path")
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
fs = spark.sparkContext._jvm.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())

spark.sql("USE customer")  # 使用实际数据库名

def create_hdfs_dir(path):
    jvm_path = spark.sparkContext._jvm.Path(path)
    if not fs.exists(jvm_path):
        fs.mkdirs(jvm_path)
        print(f"HDFS目录创建成功：{path}")
    else:
        print(f"HDFS目录已存在：{path}")

def repair_hive_table(table_name):
    spark.sql(f"MSCK REPAIR TABLE {table_name}")
    print(f"修复分区完成：{table_name}")

def print_data_count(df, table_name):
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count

# ====================== 1. 优惠发送事实表 dwd_coupon_send_fact ======================
print("=" * 50)
print("处理优惠发送事实表...")

create_hdfs_dir("/warehouse/customer/dwd/dwd_coupon_send_fact")
spark.sql("DROP TABLE IF EXISTS dwd_coupon_send_fact")
spark.sql("""
CREATE EXTERNAL TABLE dwd_coupon_send_fact (
    send_id STRING COMMENT '发送ID',
    activity_id STRING COMMENT '活动ID',
    product_id STRING COMMENT '商品ID',
    sku_id STRING COMMENT 'SKU ID',
    customer_service_id STRING COMMENT '客服ID',
    customer_id STRING COMMENT '客户ID',
    coupon_amount DECIMAL(10,2) COMMENT '发送的优惠金额',
    send_time STRING COMMENT '发送时间',
    expire_time STRING COMMENT '过期时间',
    usage_valid_hours INT COMMENT '使用有效期(小时)',
    send_channel STRING COMMENT '发送渠道(PC/无线)',
    remark STRING COMMENT '备注',
    send_status STRING COMMENT '发送状态(已发送/已使用/已过期)',
    send_date STRING COMMENT '发送日期',
    send_hour STRING COMMENT '发送小时'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/customer/dwd/dwd_coupon_send_fact'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 从ODS层读取优惠发送记录
ods_send_df = spark.table("ods_coupon_send_record").filter(
    F.col("dt") == "20250801"
)

# 数据清洗和转换
dwd_send_df = ods_send_df \
    .filter(F.col("send_id").isNotNull()) \
    .withColumn("send_date", F.to_date(F.col("send_time"))) \
    .withColumn("send_hour", F.hour(F.to_timestamp(F.col("send_time")))) \
    .withColumn("coupon_amount", F.col("coupon_amount").cast(T.DecimalType(10, 2))) \
    .select(
    "send_id", "activity_id", "product_id", "sku_id",
    "customer_service_id", "customer_id", "coupon_amount",
    "send_time", "expire_time", "usage_valid_hours",
    "send_channel", "remark", "send_status",
    "send_date", "send_hour"
)

print_data_count(dwd_send_df, "dwd_coupon_send_fact")

dwd_send_df.withColumn("dt", F.lit("20250801")) \
    .write.mode("append") \
    .partitionBy("dt") \
    .orc("/warehouse/customer/dwd/dwd_coupon_send_fact")

repair_hive_table("dwd_coupon_send_fact")

# ====================== 2. 优惠使用事实表 dwd_coupon_usage_fact ======================
print("=" * 50)
print("处理优惠使用事实表...")

create_hdfs_dir("/warehouse/customer/dwd/dwd_coupon_usage_fact")
spark.sql("DROP TABLE IF EXISTS dwd_coupon_usage_fact")
spark.sql("""
CREATE EXTERNAL TABLE dwd_coupon_usage_fact (
    usage_id STRING COMMENT '使用ID',
    send_id STRING COMMENT '发送ID',
    order_id STRING COMMENT '订单ID',
    activity_id STRING COMMENT '活动ID',
    product_id STRING COMMENT '商品ID',
    sku_id STRING COMMENT 'SKU ID',
    customer_service_id STRING COMMENT '客服ID',
    customer_id STRING COMMENT '客户ID',
    coupon_amount DECIMAL(10,2) COMMENT '使用的优惠金额',
    usage_time STRING COMMENT '使用时间',
    order_amount DECIMAL(10,2) COMMENT '订单金额',
    actual_payment DECIMAL(10,2) COMMENT '实际支付金额',
    order_status STRING COMMENT '订单状态',
    payment_time STRING COMMENT '支付时间',
    usage_date STRING COMMENT '使用日期',
    usage_hour STRING COMMENT '使用小时',
    save_amount DECIMAL(10,2) COMMENT '节省金额'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/customer/dwd/dwd_coupon_usage_fact'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 从ODS层读取优惠使用记录
ods_usage_df = spark.table("ods_coupon_usage_record").filter(
    F.col("dt") == "20250801"
)

# 数据清洗和转换
dwd_usage_df = ods_usage_df \
    .filter(F.col("usage_id").isNotNull()) \
    .withColumn("usage_date", F.to_date(F.col("usage_time"))) \
    .withColumn("usage_hour", F.hour(F.to_timestamp(F.col("usage_time")))) \
    .withColumn("save_amount",
                F.col("order_amount") - F.col("actual_payment")) \
    .withColumn("coupon_amount", F.col("coupon_amount").cast(T.DecimalType(10, 2))) \
    .withColumn("order_amount", F.col("order_amount").cast(T.DecimalType(10, 2))) \
    .withColumn("actual_payment", F.col("actual_payment").cast(T.DecimalType(10, 2))) \
    .select(
    "usage_id", "send_id", "order_id", "activity_id",
    "product_id", "sku_id", "customer_service_id", "customer_id",
    "coupon_amount", "usage_time", "order_amount",
    "actual_payment", "order_status", "payment_time",
    "usage_date", "usage_hour", "save_amount"
)

print_data_count(dwd_usage_df, "dwd_coupon_usage_fact")

dwd_usage_df.withColumn("dt", F.lit("20250801")) \
    .write.mode("append") \
    .partitionBy("dt") \
    .orc("/warehouse/customer/dwd/dwd_coupon_usage_fact")

repair_hive_table("dwd_coupon_usage_fact")

# ====================== 3. 活动商品关联事实表 dwd_activity_product_fact ======================
print("=" * 50)
print("处理活动商品关联事实表...")

create_hdfs_dir("/warehouse/customer/dwd/dwd_activity_product_fact")
spark.sql("DROP TABLE IF EXISTS dwd_activity_product_fact")
spark.sql("""
CREATE EXTERNAL TABLE dwd_activity_product_fact (
    activity_id STRING COMMENT '活动ID',
    product_id STRING COMMENT '商品ID',
    sku_id STRING COMMENT 'SKU ID',
    coupon_amount DECIMAL(10,2) COMMENT '优惠金额',
    max_usage_count INT COMMENT '每人限购次数',
    product_status STRING COMMENT '商品状态(有效/已移出)',
    add_time STRING COMMENT '添加时间',
    remove_time STRING COMMENT '移出时间',
    min_price DECIMAL(10,2) COMMENT '商品最低价',
    estimated_price DECIMAL(10,2) COMMENT '预估到手价',
    is_valid BOOLEAN COMMENT '是否有效'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/customer/dwd/dwd_activity_product_fact'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 从ODS层读取活动商品关联信息
ods_product_df = spark.table("ods_coupon_product").filter(
    F.col("dt") == "20250801"
)

# 数据清洗和转换
dwd_product_df = ods_product_df \
    .filter(F.col("activity_id").isNotNull() & F.col("product_id").isNotNull()) \
    .withColumn("is_valid",
                F.when((F.col("product_status") == "有效") & F.col("remove_time").isNull(), True)
                .otherwise(False)) \
    .withColumn("coupon_amount", F.col("coupon_amount").cast(T.DecimalType(10, 2))) \
    .withColumn("min_price", F.col("min_price").cast(T.DecimalType(10, 2))) \
    .withColumn("estimated_price", F.col("estimated_price").cast(T.DecimalType(10, 2))) \
    .select(
    "activity_id", "product_id", "sku_id", "coupon_amount",
    "max_usage_count", "product_status", "add_time", "remove_time",
    "min_price", "estimated_price", "is_valid"
)

print_data_count(dwd_product_df, "dwd_activity_product_fact")

dwd_product_df.withColumn("dt", F.lit("20250801")) \
    .write.mode("append") \
    .partitionBy("dt") \
    .orc("/warehouse/customer/dwd/dwd_activity_product_fact")

repair_hive_table("dwd_activity_product_fact")

# ====================== 4. 客服业绩宽表 dwd_customer_service_performance ======================
print("=" * 50)
print("处理客服业绩宽表...")

create_hdfs_dir("/warehouse/customer/dwd/dwd_customer_service_performance")
spark.sql("DROP TABLE IF EXISTS dwd_customer_service_performance")
spark.sql("""
CREATE EXTERNAL TABLE dwd_customer_service_performance (
    customer_service_id STRING COMMENT '客服ID',
    customer_service_name STRING COMMENT '客服姓名',
    department STRING COMMENT '所属部门',
    position STRING COMMENT '职位',
    send_count BIGINT COMMENT '发送优惠次数',
    usage_count BIGINT COMMENT '优惠被使用次数',
    total_coupon_amount DECIMAL(16,2) COMMENT '发送优惠总金额',
    total_save_amount DECIMAL(16,2) COMMENT '为客户节省总金额',
    conversion_rate DECIMAL(5,4) COMMENT '转化率',
    avg_coupon_amount DECIMAL(10,2) COMMENT '平均优惠金额'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/customer/dwd/dwd_customer_service_performance'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 关联发送表和使用表统计客服业绩
send_stats_df = spark.table("dwd_coupon_send_fact").filter(
    F.col("dt") == "20250801"
).groupBy("customer_service_id").agg(
    F.count("*").alias("send_count"),
    F.sum("coupon_amount").alias("total_coupon_amount")
)

usage_stats_df = spark.table("dwd_coupon_usage_fact").filter(
    F.col("dt") == "20250801"
).groupBy("customer_service_id").agg(
    F.count("*").alias("usage_count"),
    F.sum("save_amount").alias("total_save_amount")
)

# 关联客服维度信息
customer_service_dim = spark.table("dim_customer_service_full").filter(
    (F.col("dt") == "20250801") & (F.col("is_current") == True)
).select(
    "customer_service_id", "customer_service_name",
    "department", "position"
)

# 构建客服业绩宽表
dwd_performance_df = customer_service_dim \
    .join(send_stats_df, "customer_service_id", "left") \
    .join(usage_stats_df, "customer_service_id", "left") \
    .withColumn("send_count", F.coalesce(F.col("send_count"), F.lit(0))) \
    .withColumn("usage_count", F.coalesce(F.col("usage_count"), F.lit(0))) \
    .withColumn("total_coupon_amount", F.coalesce(F.col("total_coupon_amount"), F.lit(0.0))) \
    .withColumn("total_save_amount", F.coalesce(F.col("total_save_amount"), F.lit(0.0))) \
    .withColumn("conversion_rate",
                F.when(F.col("send_count") > 0,
                       F.col("usage_count").cast(T.DoubleType()) / F.col("send_count").cast(T.DoubleType()))
                .otherwise(0.0)) \
    .withColumn("avg_coupon_amount",
                F.when(F.col("send_count") > 0,
                       F.col("total_coupon_amount") / F.col("send_count"))
                .otherwise(0.0)) \
    .select(
    "customer_service_id", "customer_service_name",
    "department", "position",
    "send_count", "usage_count",
    "total_coupon_amount", "total_save_amount",
    "conversion_rate", "avg_coupon_amount"
)

print_data_count(dwd_performance_df, "dwd_customer_service_performance")

dwd_performance_df.withColumn("dt", F.lit("20250801")) \
    .write.mode("append") \
    .partitionBy("dt") \
    .orc("/warehouse/customer/dwd/dwd_customer_service_performance")

repair_hive_table("dwd_customer_service_performance")

spark.stop()
