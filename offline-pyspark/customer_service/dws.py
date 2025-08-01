# dws.py
# 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板
# DWS层实现代码

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("CustomerServiceCouponDWS") \
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

# ====================== 1. 客服业绩汇总表 dws_customer_service_performance_summary ======================
print("=" * 50)
print("处理客服业绩汇总表...")

create_hdfs_dir("/warehouse/customer/dws/dws_customer_service_performance_summary")
spark.sql("DROP TABLE IF EXISTS dws_customer_service_performance_summary")
spark.sql("""
CREATE EXTERNAL TABLE dws_customer_service_performance_summary (
    customer_service_id STRING COMMENT '客服ID',
    customer_service_name STRING COMMENT '客服姓名',
    department STRING COMMENT '所属部门',
    position STRING COMMENT '职位',
    send_count BIGINT COMMENT '发送优惠次数',
    usage_count BIGINT COMMENT '优惠被使用次数',
    total_coupon_amount DECIMAL(16,2) COMMENT '发送优惠总金额',
    total_save_amount DECIMAL(16,2) COMMENT '为客户节省总金额',
    conversion_rate DECIMAL(5,4) COMMENT '转化率',
    avg_coupon_amount DECIMAL(10,2) COMMENT '平均优惠金额',
    customer_count BIGINT COMMENT '服务客户数',
    order_count BIGINT COMMENT '促成订单数',
    total_order_amount DECIMAL(16,2) COMMENT '促成订单总额',
    avg_order_amount DECIMAL(10,2) COMMENT '平均订单金额'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/customer/dws/dws_customer_service_performance_summary'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 从DWD层读取客服业绩数据
dwd_performance_df = spark.table("dwd_customer_service_performance").filter(
    F.col("dt") == "20250801"
)

# 从DWD层读取优惠使用数据以获取订单相关信息
dwd_usage_df = spark.table("dwd_coupon_usage_fact").filter(
    F.col("dt") == "20250801"
)

# 统计客服相关订单信息
order_stats_df = dwd_usage_df.groupBy("customer_service_id").agg(
    F.countDistinct("customer_id").alias("customer_count"),
    F.count("order_id").alias("order_count"),
    F.sum("order_amount").alias("total_order_amount"),
    F.avg("order_amount").alias("avg_order_amount")
)

# 构建客服业绩汇总表
dws_performance_summary_df = dwd_performance_df.join(
    order_stats_df,
    "customer_service_id",
    "left"
).withColumn("customer_count", F.coalesce(F.col("customer_count"), F.lit(0))) \
    .withColumn("order_count", F.coalesce(F.col("order_count"), F.lit(0))) \
    .withColumn("total_order_amount", F.coalesce(F.col("total_order_amount"), F.lit(0.0))) \
    .withColumn("avg_order_amount", F.coalesce(F.col("avg_order_amount"), F.lit(0.0)))

print_data_count(dws_performance_summary_df, "dws_customer_service_performance_summary")

dws_performance_summary_df.withColumn("dt", F.lit("20250801")) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/customer/dws/dws_customer_service_performance_summary")

repair_hive_table("dws_customer_service_performance_summary")

# ====================== 2. 优惠活动效果汇总表 dws_coupon_activity_effect_summary ======================
print("=" * 50)
print("处理优惠活动效果汇总表...")

create_hdfs_dir("/warehouse/customer/dws/dws_coupon_activity_effect_summary")
spark.sql("DROP TABLE IF EXISTS dws_coupon_activity_effect_summary")
spark.sql("""
CREATE EXTERNAL TABLE dws_coupon_activity_effect_summary (
    activity_id STRING COMMENT '活动ID',
    activity_name STRING COMMENT '活动名称',
    activity_level STRING COMMENT '活动级别',
    activity_status STRING COMMENT '活动状态',
    send_count BIGINT COMMENT '发送优惠次数',
    usage_count BIGINT COMMENT '优惠被使用次数',
    total_coupon_amount DECIMAL(16,2) COMMENT '发送优惠总金额',
    total_save_amount DECIMAL(16,2) COMMENT '为客户节省总金额',
    conversion_rate DECIMAL(5,4) COMMENT '转化率',
    customer_count BIGINT COMMENT '触达客户数',
    order_count BIGINT COMMENT '促成订单数',
    total_order_amount DECIMAL(16,2) COMMENT '促成订单总额',
    avg_order_amount DECIMAL(10,2) COMMENT '平均订单金额',
    roi DECIMAL(10,4) COMMENT '投资回报率'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/customer/dws/dws_coupon_activity_effect_summary'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 从DWD层读取优惠发送数据
dwd_send_df = spark.table("dwd_coupon_send_fact").filter(
    F.col("dt") == "20250801"
)

# 从DWD层读取优惠使用数据
dwd_usage_df = spark.table("dwd_coupon_usage_fact").filter(
    F.col("dt") == "20250801"
)

# 从DIM层读取活动维度数据
dim_activity_df = spark.table("dim_activity_full").filter(
    F.col("dt") == "20250801"
)

# 统计活动发送情况
send_stats_df = dwd_send_df.groupBy("activity_id").agg(
    F.count("*").alias("send_count"),
    F.sum("coupon_amount").alias("total_coupon_amount"),
    F.countDistinct("customer_id").alias("customer_count")
)

# 统计活动使用情况
usage_stats_df = dwd_usage_df.groupBy("activity_id").agg(
    F.count("*").alias("usage_count"),
    F.sum("save_amount").alias("total_save_amount"),
    F.countDistinct("order_id").alias("order_count"),
    F.sum("order_amount").alias("total_order_amount"),
    F.avg("order_amount").alias("avg_order_amount")
)

# 构建活动效果汇总表
dws_activity_summary_df = dim_activity_df.select(
    "activity_id", "activity_name", "activity_level", "activity_status"
).join(send_stats_df, "activity_id", "left") \
    .join(usage_stats_df, "activity_id", "left") \
    .withColumn("send_count", F.coalesce(F.col("send_count"), F.lit(0))) \
    .withColumn("usage_count", F.coalesce(F.col("usage_count"), F.lit(0))) \
    .withColumn("total_coupon_amount", F.coalesce(F.col("total_coupon_amount"), F.lit(0.0))) \
    .withColumn("total_save_amount", F.coalesce(F.col("total_save_amount"), F.lit(0.0))) \
    .withColumn("customer_count", F.coalesce(F.col("customer_count"), F.lit(0))) \
    .withColumn("order_count", F.coalesce(F.col("order_count"), F.lit(0))) \
    .withColumn("total_order_amount", F.coalesce(F.col("total_order_amount"), F.lit(0.0))) \
    .withColumn("avg_order_amount", F.coalesce(F.col("avg_order_amount"), F.lit(0.0))) \
    .withColumn("conversion_rate",
                F.when(F.col("send_count") > 0,
                       F.col("usage_count").cast(T.DoubleType()) / F.col("send_count").cast(T.DoubleType()))
                .otherwise(0.0)) \
    .withColumn("roi",
                F.when(F.col("total_save_amount") > 0,
                       F.col("total_order_amount") / F.col("total_save_amount"))
                .otherwise(0.0)) \
    .select(
    "activity_id", "activity_name", "activity_level", "activity_status",
    "send_count", "usage_count", "total_coupon_amount", "total_save_amount",
    "conversion_rate", "customer_count", "order_count", "total_order_amount",
    "avg_order_amount", "roi"
)

print_data_count(dws_activity_summary_df, "dws_coupon_activity_effect_summary")

dws_activity_summary_df.withColumn("dt", F.lit("20250801")) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/customer/dws/dws_coupon_activity_effect_summary")

repair_hive_table("dws_coupon_activity_effect_summary")

# ====================== 3. 商品优惠效果分析表 dws_product_coupon_analysis ======================
print("=" * 50)
print("处理商品优惠效果分析表...")

create_hdfs_dir("/warehouse/customer/dws/dws_product_coupon_analysis")
spark.sql("DROP TABLE IF EXISTS dws_product_coupon_analysis")
spark.sql("""
CREATE EXTERNAL TABLE dws_product_coupon_analysis (
    product_id STRING COMMENT '商品ID',
    sku_id STRING COMMENT 'SKU ID',
    send_count BIGINT COMMENT '发送优惠次数',
    usage_count BIGINT COMMENT '优惠被使用次数',
    total_coupon_amount DECIMAL(16,2) COMMENT '发送优惠总金额',
    total_save_amount DECIMAL(16,2) COMMENT '为客户节省总金额',
    conversion_rate DECIMAL(5,4) COMMENT '转化率',
    customer_count BIGINT COMMENT '触达客户数',
    order_count BIGINT COMMENT '促成订单数',
    total_order_amount DECIMAL(16,2) COMMENT '促成订单总额',
    avg_order_amount DECIMAL(10,2) COMMENT '平均订单金额',
    avg_discount_rate DECIMAL(5,4) COMMENT '平均折扣率'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/customer/dws/dws_product_coupon_analysis'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 从DWD层读取优惠发送数据
dwd_send_df = spark.table("dwd_coupon_send_fact").filter(
    F.col("dt") == "20250801"
)

# 从DWD层读取优惠使用数据
dwd_usage_df = spark.table("dwd_coupon_usage_fact").filter(
    F.col("dt") == "20250801"
)

# 从DWD层读取活动商品关联数据
dwd_product_df = spark.table("dwd_activity_product_fact").filter(
    F.col("dt") == "20250801"
)

# 统计商品发送情况
product_send_stats_df = dwd_send_df.groupBy("product_id", "sku_id").agg(
    F.count("*").alias("send_count"),
    F.sum("coupon_amount").alias("total_coupon_amount"),
    F.countDistinct("customer_id").alias("customer_count")
)

# 统计商品使用情况
product_usage_stats_df = dwd_usage_df.groupBy("product_id", "sku_id").agg(
    F.count("*").alias("usage_count"),
    F.sum("save_amount").alias("total_save_amount"),
    F.countDistinct("order_id").alias("order_count"),
    F.sum("order_amount").alias("total_order_amount"),
    F.avg("order_amount").alias("avg_order_amount")
)

# 构建商品优惠效果分析表
dws_product_analysis_df = dwd_product_df.select(
    "product_id", "sku_id"
).join(product_send_stats_df, ["product_id", "sku_id"], "left") \
    .join(product_usage_stats_df, ["product_id", "sku_id"], "left") \
    .withColumn("send_count", F.coalesce(F.col("send_count"), F.lit(0))) \
    .withColumn("usage_count", F.coalesce(F.col("usage_count"), F.lit(0))) \
    .withColumn("total_coupon_amount", F.coalesce(F.col("total_coupon_amount"), F.lit(0.0))) \
    .withColumn("total_save_amount", F.coalesce(F.col("total_save_amount"), F.lit(0.0))) \
    .withColumn("customer_count", F.coalesce(F.col("customer_count"), F.lit(0))) \
    .withColumn("order_count", F.coalesce(F.col("order_count"), F.lit(0))) \
    .withColumn("total_order_amount", F.coalesce(F.col("total_order_amount"), F.lit(0.0))) \
    .withColumn("avg_order_amount", F.coalesce(F.col("avg_order_amount"), F.lit(0.0))) \
    .withColumn("conversion_rate",
                F.when(F.col("send_count") > 0,
                       F.col("usage_count").cast(T.DoubleType()) / F.col("send_count").cast(T.DoubleType()))
                .otherwise(0.0)) \
    .withColumn("avg_discount_rate",
                F.when(F.col("total_order_amount") > 0,
                       F.col("total_save_amount") / (F.col("total_order_amount") + F.col("total_save_amount")))
                .otherwise(0.0)) \
    .select(
    "product_id", "sku_id",
    "send_count", "usage_count", "total_coupon_amount", "total_save_amount",
    "conversion_rate", "customer_count", "order_count", "total_order_amount",
    "avg_order_amount", "avg_discount_rate"
)

print_data_count(dws_product_analysis_df, "dws_product_coupon_analysis")

dws_product_analysis_df.withColumn("dt", F.lit("20250801")) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/customer/dws/dws_product_coupon_analysis")

repair_hive_table("dws_product_coupon_analysis")

spark.stop()
