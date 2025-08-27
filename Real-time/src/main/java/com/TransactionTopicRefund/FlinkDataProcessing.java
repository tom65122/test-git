package com.TransactionTopicRefund;

import common.utils.EnvironmentSettingUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink实时数据处理流程
 * 功能：从MySQL读取ODS层数据，进行清洗、转换和关联，构建DWD层
 */
public class FlinkDataProcessing {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // ==================== 1. 从MySQL读取ODS层数据 ====================

        // 1.1 读取订单表
        tEnv.executeSql("CREATE TABLE ods_order_source (\n" +
                "    order_id STRING,\n" +
                "    user_id STRING,\n" +
                "    sku_id STRING,\n" +
                "    order_amount DECIMAL(18,2),\n" +
                "    pay_time TIMESTAMP(3),\n" +
                "    commit_ship_time TIMESTAMP(3),\n" +
                "    order_status STRING,\n" +
                "    create_time TIMESTAMP(3),\n" +
                "    data_source STRING,\n" +
                "    sync_time TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://cdh03:3306/refund',\n" +
                "    'table-name' = 'ods_order',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root',\n" +
                "    'scan.incremental.initial' = 'true',\n" +
                "    'scan.incremental.mode' = 'timestamp',\n" +
                "    'scan.incremental.timestamp.column' = 'sync_time'\n" +
                ");");

        // 1.2 读取退款表
        tEnv.executeSql("CREATE TABLE ods_refund_source (\n" +
                "    refund_id STRING,\n" +
                "    order_id STRING,\n" +
                "    user_id STRING,\n" +
                "    refund_amount DECIMAL(18,2),\n" +
                "    refund_status STRING,\n" +
                "    refund_apply_time TIMESTAMP(3),\n" +
                "    user_input_reason STRING,\n" +
                "    refund_finish_time TIMESTAMP(3),\n" +
                "    data_source STRING,\n" +
                "    sync_time TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://cdh03:3306/refund',\n" +
                "    'table-name' = 'ods_refund',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root',\n" +
                "    'scan.incremental.initial' = 'true',\n" +
                "    'scan.incremental.mode' = 'timestamp',\n" +
                "    'scan.incremental.timestamp.column' = 'sync_time'\n" +
                ");");

        // 1.3 读取物流表
        tEnv.executeSql("CREATE TABLE ods_logistics_source (\n" +
                "    logistics_id STRING,\n" +
                "    order_id STRING,\n" +
                "    logistics_node STRING,\n" +
                "    node_time TIMESTAMP(3),\n" +
                "    express_company STRING,\n" +
                "    logistics_desc STRING,\n" +
                "    data_source STRING,\n" +
                "    sync_time TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://cdh03:3306/refund',\n" +
                "    'table-name' = 'ods_logistics',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root',\n" +
                "    'scan.incremental.initial' = 'true',\n" +
                "    'scan.incremental.mode' = 'timestamp',\n" +
                "    'scan.incremental.timestamp.column' = 'sync_time'\n" +
                ");");

        // 1.4 读取用户行为表
        tEnv.executeSql("CREATE TABLE ods_user_behavior_source (\n" +
                "    behavior_id STRING,\n" +
                "    user_id STRING,\n" +
                "    behavior_type STRING,\n" +
                "    search_keywords ARRAY<STRING>,\n" +
                "    behavior_time TIMESTAMP(3),\n" +
                "    page_url STRING,\n" +
                "    data_source STRING,\n" +
                "    sync_time TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://cdh03:3306/refund',\n" +
                "    'table-name' = 'ods_user_behavior',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'rootv',\n" +
                "    'scan.incremental.initial' = 'true',\n" +
                "    'scan.incremental.mode' = 'timestamp',\n" +
                "    'scan.incremental.timestamp.column' = 'sync_time'\n" +
                ");");

        // 1.5 读取异常数据表
        tEnv.executeSql("CREATE TABLE ods_error_data_source (\n" +
                "    error_id STRING,\n" +
                "    table_name STRING,\n" +
                "    error_field STRING,\n" +
                "    error_reason STRING,\n" +
                "    raw_data STRING,\n" +
                "    create_time TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://cdh03:3306/refund',\n" +
                "    'table-name' = 'ods_error_data',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root',\n" +
                "    'scan.incremental.initial' = 'true',\n" +
                "    'scan.incremental.mode' = 'timestamp',\n" +
                "    'scan.incremental.timestamp.column' = 'create_time'\n" +
                ");");

        // ==================== 2. 创建DWD层宽表 ====================

        // 2.1 订单-退款关联宽表
        tEnv.executeSql("CREATE TABLE dwd_order_refund_detail (\n" +
                "    order_id STRING COMMENT '订单ID',\n" +
                "    user_id STRING COMMENT '用户ID',\n" +
                "    sku_id STRING COMMENT '商品ID',\n" +
                "    order_amount DECIMAL(18,2) COMMENT '订单金额',\n" +
                "    pay_time TIMESTAMP(3) COMMENT '支付时间',\n" +
                "    commit_ship_time TIMESTAMP(3) COMMENT '承诺发货时间',\n" +
                "    order_status STRING COMMENT '订单状态',\n" +
                "    create_time TIMESTAMP(3) COMMENT '订单创建时间',\n" +
                "    refund_id STRING COMMENT '退款ID',\n" +
                "    refund_amount DECIMAL(18,2) COMMENT '退款金额',\n" +
                "    refund_status STRING COMMENT '退款状态',\n" +
                "    refund_apply_time TIMESTAMP(3) COMMENT '退款申请时间',\n" +
                "    user_input_reason STRING COMMENT '退款原因',\n" +
                "    refund_finish_time TIMESTAMP(3) COMMENT '退款完结时间',\n" +
                "    data_source STRING COMMENT '数据来源',\n" +
                "    sync_time TIMESTAMP(3) COMMENT '同步时间',\n" +
                "    dt STRING COMMENT '分区字段'\n" +
                ") PARTITIONED BY (dt)\n" +
                "WITH (\n" +
                "    'connector' = 'paimon',\n" +
                "    'path' = 'hdfs:///refund_data/dwd/dwd_order_refund_detail',\n" +
                "    'file.format' = 'parquet',\n" +
                "    'sink.parallelism' = '1',\n" +
                "    'computed-column.expression.dt' = 'DATE_FORMAT(sync_time, ''yyyy-MM-dd'')'\n" +
                ");");

        // 2.2 订单-物流关联宽表
        tEnv.executeSql("CREATE TABLE dwd_order_logistics_detail (\n" +
                "    order_id STRING COMMENT '订单ID',\n" +
                "    user_id STRING COMMENT '用户ID',\n" +
                "    sku_id STRING COMMENT '商品ID',\n" +
                "    order_amount DECIMAL(18,2) COMMENT '订单金额',\n" +
                "    pay_time TIMESTAMP(3) COMMENT '支付时间',\n" +
                "    commit_ship_time TIMESTAMP(3) COMMENT '承诺发货时间',\n" +
                "    order_status STRING COMMENT '订单状态',\n" +
                "    create_time TIMESTAMP(3) COMMENT '订单创建时间',\n" +
                "    logistics_id STRING COMMENT '物流单ID',\n" +
                "    logistics_node STRING COMMENT '物流节点',\n" +
                "    node_time TIMESTAMP(3) COMMENT '节点时间',\n" +
                "    express_company STRING COMMENT '快递公司',\n" +
                "    logistics_desc STRING COMMENT '物流描述',\n" +
                "    data_source STRING COMMENT '数据来源',\n" +
                "    sync_time TIMESTAMP(3) COMMENT '同步时间',\n" +
                "    dt STRING COMMENT '分区字段'\n" +
                ") PARTITIONED BY (dt)\n" +
                "WITH (\n" +
                "    'connector' = 'paimon',\n" +
                "    'path' = 'hdfs:///refund_data/dwd/dwd_order_logistics_detail',\n" +
                "    'file.format' = 'parquet',\n" +
                "    'sink.parallelism' = '1',\n" +
                "    'computed-column.expression.dt' = 'DATE_FORMAT(sync_time, ''yyyy-MM-dd'')'\n" +
                ");");

        // ==================== 3. 数据处理逻辑 ====================
        // 3.1 数据清洗和转换
        tEnv.executeSql("CREATE VIEW cleaned_order AS " +
                "SELECT " +
                "    order_id," +
                "    user_id," +
                "    sku_id," +
                "    order_amount," +
                "    pay_time," +
                "    commit_ship_time," +
                "    order_status," +
                "    create_time," +
                "    data_source," +
                "    sync_time," +
                "    DATE_FORMAT(sync_time, 'yyyy-MM-dd') as dt " +
                "FROM ods_order_source " +
                "WHERE order_amount > 0 AND order_id IS NOT NULL");

// 3.2 多表关联处理 - 修正后的插入语句
        tEnv.executeSql("INSERT INTO dwd_order_refund_detail " +
                "SELECT " +
                "    o.order_id," +
                "    o.user_id," +
                "    o.sku_id," +
                "    o.order_amount," +
                "    o.pay_time," +
                "    o.commit_ship_time," +
                "    o.order_status," +
                "    o.create_time," +
                "    r.refund_id," +
                "    r.refund_amount," +
                "    r.refund_status," +
                "    r.refund_apply_time," +
                "    r.user_input_reason," +
                "    r.refund_finish_time," +
                "    o.data_source," +
                "    o.sync_time," +
                "    DATE_FORMAT(o.sync_time, 'yyyy-MM-dd') as dt " +
                "FROM ods_order_source o " +
                "LEFT JOIN ods_refund_source r ON o.order_id = r.order_id " +
                "WHERE o.sync_time >= CURRENT_DATE - INTERVAL '7' DAY");

        tEnv.executeSql("INSERT INTO dwd_order_logistics_detail " +
                "SELECT " +
                "    o.order_id," +
                "    o.user_id," +
                "    o.sku_id," +
                "    o.order_amount," +
                "    o.pay_time," +
                "    o.commit_ship_time," +
                "    o.order_status," +
                "    o.create_time," +
                "    l.logistics_id," +
                "    l.logistics_node," +
                "    l.node_time," +
                "    l.express_company," +
                "    l.logistics_desc," +
                "    o.data_source," +
                "    o.sync_time," +
                "    DATE_FORMAT(o.sync_time, 'yyyy-MM-dd') as dt " +
                "FROM ods_order_source o " +
                "LEFT JOIN ods_logistics_source l ON o.order_id = l.order_id " +
                "WHERE o.sync_time >= CURRENT_DATE - INTERVAL '7' DAY");

    }
}
