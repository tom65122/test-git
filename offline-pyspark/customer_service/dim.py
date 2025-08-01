# 工单编号：大数据-电商数仓-08-商品主题营销工具客服专属优惠看板

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("CustomerServiceCouponDIM") \
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

spark.sql("USE customer")  # 根据实际数据库名修改

def create_hdfs_dir(path):
    jvm_path = spark.sparkContext._jvm.Path(path)
    if not fs.exists(jvm_path):
        fs.mkdirs(jvm_path)
        print(f"HDFS目录创建成功：{path}")
    else:
        print(f"HDFS目录已存在：{path}")

# 新增：修复Hive表分区的函数（关键）
def repair_hive_table(table_name):
    spark.sql(f"MSCK REPAIR TABLE {table_name}")
    print(f"修复分区完成：{table_name}")

# 新增：打印数据量的函数（验证数据是否存在）
def print_data_count(df, table_name):
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count


# ====================== 1. 客服维度表 dim_customer_service_full ======================
create_hdfs_dir("/warehouse/default/dim/dim_customer_service_full")
spark.sql("DROP TABLE IF EXISTS dim_customer_service_full")
spark.sql("""
CREATE EXTERNAL TABLE dim_customer_service_full (
    customer_service_sk BIGINT COMMENT '客服代理键',
    customer_service_id STRING COMMENT '客服ID',
    customer_service_name STRING COMMENT '客服姓名',
    department STRING COMMENT '所属部门',
    position STRING COMMENT '职位',
    hire_date STRING COMMENT '入职日期',
    status STRING COMMENT '状态(在职/离职)',
    create_time STRING COMMENT '创建时间',
    update_time STRING COMMENT '更新时间',
    effective_date STRING COMMENT '生效日期',
    expiry_date STRING COMMENT '失效日期',
    is_current BOOLEAN COMMENT '是否当前版本',
    version INT COMMENT '版本号'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/default/dim/dim_customer_service_full'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

customer_service_info = spark.table("ods_customer_service_info").filter(
    (F.col("dt") == "20250801") & (F.col("status").isNotNull())
).select("customer_service_id", "customer_service_name", "department", "position",
         "hire_date", "status", "create_time", "update_time")

dim_customer_service_df = customer_service_info \
    .withColumn("customer_service_sk", F.monotonically_increasing_id()) \
    .withColumn("effective_date", F.current_date()) \
    .withColumn("expiry_date", F.date_add(F.current_date(), 365*10)) \
    .withColumn("is_current", F.lit(True)) \
    .withColumn("version", F.lit(1))

print_data_count(dim_customer_service_df, "dim_customer_service_full")

dim_customer_service_df.withColumn("dt", F.lit("20250801")) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/default/dim/dim_customer_service_full")

repair_hive_table("dim_customer_service_full")



# ====================== 3. 活动维度表 dim_activity_full ======================
create_hdfs_dir("/warehouse/default/dim/dim_activity_full")
spark.sql("DROP TABLE IF EXISTS dim_activity_full")
spark.sql("""
CREATE EXTERNAL TABLE dim_activity_full (
    activity_sk BIGINT COMMENT '活动代理键',
    activity_id STRING COMMENT '活动ID',
    activity_name STRING COMMENT '活动名称',
    activity_level STRING COMMENT '活动级别',
    activity_level_desc STRING COMMENT '活动级别描述',
    activity_status STRING COMMENT '活动状态',
    activity_status_desc STRING COMMENT '活动状态描述',
    activity_start_time STRING COMMENT '活动开始时间',
    activity_end_time STRING COMMENT '活动结束时间',
    activity_duration INT COMMENT '活动持续天数',
    coupon_type STRING COMMENT '优惠类型',
    coupon_type_desc STRING COMMENT '优惠类型描述',
    creator_id STRING COMMENT '创建人ID',
    create_time STRING COMMENT '创建时间',
    update_time STRING COMMENT '更新时间',
    max_valid_days INT COMMENT '最大有效天数',
    max_products INT COMMENT '最大商品数'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/default/dim/dim_activity_full'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

# 从ODS层读取活动信息
ods_activity_df = spark.table("ods_coupon_activity").filter(
    F.col("dt") == "20250801"
).select("activity_id", "activity_name", "activity_level", "activity_status",
         "activity_start_time", "activity_end_time", "coupon_type", "creator_id",
         "create_time", "update_time", "max_valid_days", "max_products")

# 添加维度描述字段
dim_activity_df = ods_activity_df \
    .withColumn("activity_sk", F.monotonically_increasing_id()) \
    .withColumn("activity_level_desc",
                F.when(F.col("activity_level") == "商品级", "Product Level")
                .when(F.col("activity_level") == "SKU级", "SKU Level")
                .otherwise("Unknown")) \
    .withColumn("activity_status_desc",
                F.when(F.col("activity_status") == "进行中", "Active")
                .when(F.col("activity_status") == "已结束", "Ended")
                .when(F.col("activity_status") == "已暂停", "Paused")
                .otherwise("Unknown")) \
    .withColumn("coupon_type_desc",
                F.when(F.col("coupon_type") == "固定优惠", "Fixed Discount")
                .when(F.col("coupon_type") == "自定义优惠", "Custom Discount")
                .otherwise("Unknown")) \
    .withColumn("activity_duration",
                F.datediff(F.to_date(F.col("activity_end_time")), F.to_date(F.col("activity_start_time"))))

print_data_count(dim_activity_df, "dim_activity_full")

dim_activity_df.withColumn("dt", F.lit("20250801")) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/default/dim/dim_activity_full")

repair_hive_table("dim_activity_full")


# ====================== 4. 客户维度表 dim_customer_full ======================
create_hdfs_dir("/warehouse/default/dim/dim_customer_full")
spark.sql("DROP TABLE IF EXISTS dim_customer_full")
spark.sql("""
CREATE EXTERNAL TABLE dim_customer_full (
    customer_id STRING COMMENT '客户ID',
    customer_name STRING COMMENT '客户姓名',
    gender STRING COMMENT '性别',
    age INT COMMENT '年龄',
    level STRING COMMENT '客户等级',
    register_time STRING COMMENT '注册时间',
    last_login_time STRING COMMENT '最后登录时间'
) 
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
LOCATION '/warehouse/default/dim/dim_customer_full'
TBLPROPERTIES ('orc.compress' = 'snappy');
""")

dim_customer_df = spark.table("ods_customer_info").filter(
    (F.col("dt") == "20250801")
).select("customer_id", "customer_name", "gender", "age", "level", "register_time", "last_login_time")

print_data_count(dim_customer_df, "dim_customer_full")

dim_customer_df.withColumn("dt", F.lit("20250801")) \
    .write.mode("overwrite") \
    .partitionBy("dt") \
    .orc("/warehouse/default/dim/dim_customer_full")

repair_hive_table("dim_customer_full")

spark.stop()
