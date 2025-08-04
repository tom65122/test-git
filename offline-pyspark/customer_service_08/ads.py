# ads.py
# 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板
# ADS层实现代码

import os
# 设置Python环境变量，解决python3找不到的问题
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("CustomerServiceCouponADS") \
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
    try:
        spark.sql(f"MSCK REPAIR TABLE {table_name}")
        print(f"修复分区完成：{table_name}")
    except Exception as e:
        # 如果表不是分区表，则跳过修复操作
        if "NOT_A_PARTITIONED_TABLE" in str(e):
            print(f"表 {table_name} 不是分区表，跳过修复操作")
        else:
            raise e

def print_data_count(df, table_name):
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count

# ====================== 1. 优惠活动每日统计表 ads_activity_daily_stats ======================
print("=" * 50)
print("处理优惠活动每日统计表...")

create_hdfs_dir("/warehouse/customer/ads/ads_activity_daily_stats")
spark.sql("DROP TABLE IF EXISTS ads_activity_daily_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_activity_daily_stats (
    stat_date STRING COMMENT '统计日期',
    activity_id STRING COMMENT '活动ID',
    activity_name STRING COMMENT '活动名称',
    send_count_7d BIGINT COMMENT '近7天发送优惠次数',
    usage_count_7d BIGINT COMMENT '近7天优惠被使用次数',
    total_coupon_amount_7d DECIMAL(16,2) COMMENT '近7天发送优惠总金额',
    total_save_amount_7d DECIMAL(16,2) COMMENT '近7天为客户节省总金额',
    send_count_30d BIGINT COMMENT '近30天发送优惠次数',
    usage_count_30d BIGINT COMMENT '近30天优惠被使用次数',
    total_coupon_amount_30d DECIMAL(16,2) COMMENT '近30天发送优惠总金额',
    total_save_amount_30d DECIMAL(16,2) COMMENT '近30天为客户节省总金额'
) 
STORED AS ORC
LOCATION '/warehouse/customer/ads/ads_activity_daily_stats'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 从DWS层读取活动效果汇总数据
dws_activity_summary_df = spark.table("dws_coupon_activity_effect_summary").filter(
    F.col("dt") == "20250801"
)

# 构建活动每日统计表（这里简化处理，实际应用中需要处理多天数据）
ads_activity_daily_stats_df = dws_activity_summary_df.select(
    F.lit("2025-08-01").alias("stat_date"),
    "activity_id",
    "activity_name",
    F.col("send_count").alias("send_count_7d"),
    F.col("usage_count").alias("usage_count_7d"),
    F.col("total_coupon_amount").alias("total_coupon_amount_7d"),
    F.col("total_save_amount").alias("total_save_amount_7d"),
    F.col("send_count").alias("send_count_30d"),
    F.col("usage_count").alias("usage_count_30d"),
    F.col("total_coupon_amount").alias("total_coupon_amount_30d"),
    F.col("total_save_amount").alias("total_save_amount_30d")
)

print_data_count(ads_activity_daily_stats_df, "ads_activity_daily_stats")

ads_activity_daily_stats_df.write.mode("append").orc("/warehouse/customer/ads/ads_activity_daily_stats")
repair_hive_table("ads_activity_daily_stats")

# ====================== 2. 客服优惠发送与核销统计表 ads_customer_service_coupon_stats ======================
print("=" * 50)
print("处理客服优惠发送与核销统计表...")

create_hdfs_dir("/warehouse/customer/ads/ads_customer_service_coupon_stats")
spark.sql("DROP TABLE IF EXISTS ads_customer_service_coupon_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_customer_service_coupon_stats (
    stat_date STRING COMMENT '统计日期',
    customer_service_id STRING COMMENT '客服ID',
    customer_service_name STRING COMMENT '客服姓名',
    department STRING COMMENT '所属部门',
    position STRING COMMENT '职位',
    send_count_7d BIGINT COMMENT '近7天发送优惠次数',
    usage_count_7d BIGINT COMMENT '近7天优惠被使用次数',
    conversion_rate_7d DECIMAL(5,4) COMMENT '近7天转化率',
    total_coupon_amount_7d DECIMAL(16,2) COMMENT '近7天发送优惠总金额',
    total_save_amount_7d DECIMAL(16,2) COMMENT '近7天为客户节省总金额',
    send_count_30d BIGINT COMMENT '近30天发送优惠次数',
    usage_count_30d BIGINT COMMENT '近30天优惠被使用次数',
    conversion_rate_30d DECIMAL(5,4) COMMENT '近30天转化率',
    total_coupon_amount_30d DECIMAL(16,2) COMMENT '近30天发送优惠总金额',
    total_save_amount_30d DECIMAL(16,2) COMMENT '近30天为客户节省总金额'
) 
STORED AS ORC
LOCATION '/warehouse/customer/ads/ads_customer_service_coupon_stats'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 从DWS层读取客服业绩汇总数据
dws_performance_summary_df = spark.table("dws_customer_service_performance_summary").filter(
    F.col("dt") == "20250801"
)

# 构建客服优惠发送与核销统计表（这里简化处理，实际应用中需要处理多天数据）
ads_customer_service_stats_df = dws_performance_summary_df.select(
    F.lit("2025-08-01").alias("stat_date"),
    "customer_service_id",
    "customer_service_name",
    "department",
    "position",
    F.col("send_count").alias("send_count_7d"),
    F.col("usage_count").alias("usage_count_7d"),
    F.col("conversion_rate").alias("conversion_rate_7d"),
    F.col("total_coupon_amount").alias("total_coupon_amount_7d"),
    F.col("total_save_amount").alias("total_save_amount_7d"),
    F.col("send_count").alias("send_count_30d"),
    F.col("usage_count").alias("usage_count_30d"),
    F.col("conversion_rate").alias("conversion_rate_30d"),
    F.col("total_coupon_amount").alias("total_coupon_amount_30d"),
    F.col("total_save_amount").alias("total_save_amount_30d")
)

print_data_count(ads_customer_service_stats_df, "ads_customer_service_coupon_stats")

ads_customer_service_stats_df.write.mode("append").orc("/warehouse/customer/ads/ads_customer_service_coupon_stats")
repair_hive_table("ads_customer_service_coupon_stats")

# ====================== 3. 优惠发送与核销明细表 ads_coupon_send_usage_detail ======================
print("=" * 50)
print("处理优惠发送与核销明细表...")

create_hdfs_dir("/warehouse/customer/ads/ads_coupon_send_usage_detail")
spark.sql("DROP TABLE IF EXISTS ads_coupon_send_usage_detail")
spark.sql("""
CREATE EXTERNAL TABLE ads_coupon_send_usage_detail (
    stat_date STRING COMMENT '统计日期',
    send_id STRING COMMENT '发送ID',
    usage_id STRING COMMENT '使用ID',
    activity_id STRING COMMENT '活动ID',
    product_id STRING COMMENT '商品ID',
    sku_id STRING COMMENT 'SKU ID',
    customer_service_id STRING COMMENT '客服ID',
    customer_id STRING COMMENT '客户ID',
    coupon_amount DECIMAL(10,2) COMMENT '优惠金额',
    send_time STRING COMMENT '发送时间',
    usage_time STRING COMMENT '使用时间',
    order_id STRING COMMENT '订单ID',
    order_amount DECIMAL(10,2) COMMENT '订单金额',
    actual_payment DECIMAL(10,2) COMMENT '实际支付金额',
    save_amount DECIMAL(10,2) COMMENT '节省金额',
    send_status STRING COMMENT '发送状态',
    order_status STRING COMMENT '订单状态'
) 
STORED AS ORC
LOCATION '/warehouse/customer/ads/ads_coupon_send_usage_detail'
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

# 关联发送和使用数据，构建优惠发送与核销明细表
ads_coupon_detail_df = dwd_send_df.join(
    dwd_usage_df.select(
        "send_id", "usage_id", "order_id", "usage_time",
        "order_amount", "actual_payment", "save_amount", "order_status"
    ),
    "send_id",
    "left"
).select(
    F.lit("2025-08-01").alias("stat_date"),
    dwd_send_df["send_id"],
    F.col("usage_id"),
    dwd_send_df["activity_id"],
    dwd_send_df["product_id"],
    dwd_send_df["sku_id"],
    dwd_send_df["customer_service_id"],
    dwd_send_df["customer_id"],
    dwd_send_df["coupon_amount"],
    dwd_send_df["send_time"],
    F.col("usage_time"),
    F.col("order_id"),
    F.col("order_amount"),
    F.col("actual_payment"),
    F.col("save_amount"),
    dwd_send_df["send_status"],
    F.col("order_status")
)

print_data_count(ads_coupon_detail_df, "ads_coupon_send_usage_detail")

ads_coupon_detail_df.write.mode("append").orc("/warehouse/customer/ads/ads_coupon_send_usage_detail")
repair_hive_table("ads_coupon_send_usage_detail")

# ====================== 4. 时间周期汇总统计表 ads_time_period_summary ======================
print("=" * 50)
print("处理时间周期汇总统计表...")

create_hdfs_dir("/warehouse/customer/ads/ads_time_period_summary")
spark.sql("DROP TABLE IF EXISTS ads_time_period_summary")
spark.sql("""
CREATE EXTERNAL TABLE ads_time_period_summary (
    stat_date STRING COMMENT '统计日期',
    total_send_count_7d BIGINT COMMENT '近7天发送优惠总数',
    total_usage_count_7d BIGINT COMMENT '近7天核销优惠总数',
    total_order_count_7d BIGINT COMMENT '近7天通过优惠完成支付的订单数',
    total_send_count_30d BIGINT COMMENT '近30天发送优惠总数',
    total_usage_count_30d BIGINT COMMENT '近30天核销优惠总数',
    total_order_count_30d BIGINT COMMENT '近30天通过优惠完成支付的订单数'
) 
STORED AS ORC
LOCATION '/warehouse/customer/ads/ads_time_period_summary'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 从DWD层读取统计数据
dwd_send_df = spark.table("dwd_coupon_send_fact").filter(
    F.col("dt") == "20250801"
)

dwd_usage_df = spark.table("dwd_coupon_usage_fact").filter(
    F.col("dt") == "20250801"
)

# 计算发送统计
send_stats = dwd_send_df.agg(
    F.count("*").alias("send_count")
).collect()[0]

# 计算使用统计
usage_stats = dwd_usage_df.agg(
    F.count("*").alias("usage_count"),
    F.countDistinct("order_id").alias("order_count")
).collect()[0]

# 构建时间周期汇总统计表（这里简化处理，实际应用中需要处理多天数据）
ads_time_period_summary_df = spark.createDataFrame([
    (
        "2025-08-01",
        send_stats["send_count"],
        usage_stats["usage_count"],
        usage_stats["order_count"],
        send_stats["send_count"],
        usage_stats["usage_count"],
        usage_stats["order_count"]
    )
], schema=["stat_date", "total_send_count_7d", "total_usage_count_7d", "total_order_count_7d",
           "total_send_count_30d", "total_usage_count_30d", "total_order_count_30d"])

print_data_count(ads_time_period_summary_df, "ads_time_period_summary")

ads_time_period_summary_df.write.mode("append").orc("/warehouse/customer/ads/ads_time_period_summary")
repair_hive_table("ads_time_period_summary")

# ====================== 5. 各客服发送优惠数量表 ads_customer_service_send_stats ======================
print("=" * 50)
print("处理各客服发送优惠数量表...")

create_hdfs_dir("/warehouse/customer/ads/ads_customer_service_send_stats")
spark.sql("DROP TABLE IF EXISTS ads_customer_service_send_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_customer_service_send_stats (
    stat_date STRING COMMENT '统计日期',
    customer_service_id STRING COMMENT '客服ID',
    customer_service_name STRING COMMENT '客服姓名',
    department STRING COMMENT '所属部门',
    position STRING COMMENT '职位',
    send_count_7d BIGINT COMMENT '近7天发送优惠次数',
    send_count_30d BIGINT COMMENT '近30天发送优惠次数'
) 
STORED AS ORC
LOCATION '/warehouse/customer/ads/ads_customer_service_send_stats'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 从DWS层读取客服发送统计数据
dws_performance_summary_df = spark.table("dws_customer_service_performance_summary").filter(
    F.col("dt") == "20250801"
)

# 构建客服发送优惠数量表
ads_customer_service_send_stats_df = dws_performance_summary_df.select(
    F.lit("2025-08-01").alias("stat_date"),
    "customer_service_id",
    "customer_service_name",
    "department",
    "position",
    F.col("send_count").alias("send_count_7d"),
    F.col("send_count").alias("send_count_30d")
)

print_data_count(ads_customer_service_send_stats_df, "ads_customer_service_send_stats")

ads_customer_service_send_stats_df.write.mode("append").orc("/warehouse/customer/ads/ads_customer_service_send_stats")
repair_hive_table("ads_customer_service_send_stats")

# ====================== 6. 各客服发送优惠被核销数量表 ads_customer_service_usage_stats ======================
print("=" * 50)
print("处理各客服发送优惠被核销数量表...")

create_hdfs_dir("/warehouse/customer/ads/ads_customer_service_usage_stats")
spark.sql("DROP TABLE IF EXISTS ads_customer_service_usage_stats")
spark.sql("""
CREATE EXTERNAL TABLE ads_customer_service_usage_stats (
    stat_date STRING COMMENT '统计日期',
    customer_service_id STRING COMMENT '客服ID',
    customer_service_name STRING COMMENT '客服姓名',
    department STRING COMMENT '所属部门',
    position STRING COMMENT '职位',
    usage_count_7d BIGINT COMMENT '近7天发送优惠被核销次数',
    usage_count_30d BIGINT COMMENT '近30天发送优惠被核销次数'
) 
STORED AS ORC
LOCATION '/warehouse/customer/ads/ads_customer_service_usage_stats'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 从DWS层读取客服使用统计数据
dws_performance_summary_df = spark.table("dws_customer_service_performance_summary").filter(
    F.col("dt") == "20250801"
)

# 构建客服发送优惠被核销数量表
ads_customer_service_usage_stats_df = dws_performance_summary_df.select(
    F.lit("2025-08-01").alias("stat_date"),
    "customer_service_id",
    "customer_service_name",
    "department",
    "position",
    F.col("usage_count").alias("usage_count_7d"),
    F.col("usage_count").alias("usage_count_30d")
)

print_data_count(ads_customer_service_usage_stats_df, "ads_customer_service_usage_stats")

ads_customer_service_usage_stats_df.write.mode("append").orc("/warehouse/customer/ads/ads_customer_service_usage_stats")
repair_hive_table("ads_customer_service_usage_stats")

spark.stop()
