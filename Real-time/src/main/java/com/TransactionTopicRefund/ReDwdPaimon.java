package com.TransactionTopicRefund;

import common.utils.EnvironmentSettingUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink实时数据处理流程
 * 功能：从MySQL读取ODS层数据，进行清洗、转换和关联，构建DWD层
 */
public class ReDwdPaimon {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 创建 Paimon Catalog
        tEnv.executeSql("CREATE CATALOG paimon_cat WITH (" +
                "'type' = 'paimon'," +
                "'warehouse' = 'hdfs://cdh01:8020/refund_data/dwd'" +
                ")");

        // ==================== 1. 从MySQL读取ODS层数据 ====================
        // 注意：JDBC表需要在默认catalog中创建，不能在Paimon catalog中创建

        // 1.1 读取订单表
        tEnv.executeSql("CREATE TABLE `default_catalog`.`default_database`.`ods_order_source` (\n" +
                "    order_id STRING,\n" +
                "    user_id STRING,\n" +
                "    sku_id STRING,\n" +
                "    order_amount DECIMAL(18,2),\n" +
                "    pay_time TIMESTAMP(3),\n" +
                "    commit_ship_time TIMESTAMP(3),\n" +
                "    order_status STRING,\n" +
                "    create_time TIMESTAMP(3),\n" +
                "    data_source STRING,\n" +
                "    sync_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR sync_time AS sync_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://192.168.142.137:3306/refund',\n" +
                "    'table-name' = 'ods_order',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root'\n" +
                ");");

        // 1.2 读取退款表
        tEnv.executeSql("CREATE TABLE `default_catalog`.`default_database`.`ods_refund_source` (\n" +
                "    refund_id STRING,\n" +
                "    order_id STRING,\n" +
                "    user_id STRING,\n" +
                "    refund_amount DECIMAL(18,2),\n" +
                "    refund_status STRING,\n" +
                "    refund_apply_time TIMESTAMP(3),\n" +
                "    user_input_reason STRING,\n" +
                "    refund_finish_time TIMESTAMP(3),\n" +
                "    data_source STRING,\n" +
                "    sync_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR sync_time AS sync_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://192.168.142.137:3306/refund',\n" +
                "    'table-name' = 'ods_refund',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root'\n" +
                ");");

        // 1.3 读取物流表
        tEnv.executeSql("CREATE TABLE `default_catalog`.`default_database`.`ods_logistics_source` (\n" +
                "    logistics_id STRING,\n" +
                "    order_id STRING,\n" +
                "    logistics_node STRING,\n" +
                "    node_time TIMESTAMP(3),\n" +
                "    express_company STRING,\n" +
                "    logistics_desc STRING,\n" +
                "    data_source STRING,\n" +
                "    sync_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR sync_time AS sync_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://192.168.142.137:3306/refund',\n" +
                "    'table-name' = 'ods_logistics',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root'\n" +
                ");");

        // 1.4 读取用户行为表
        tEnv.executeSql("CREATE TABLE `default_catalog`.`default_database`.`ods_user_behavior_source` (\n" +
                "    behavior_id STRING,\n" +
                "    user_id STRING,\n" +
                "    behavior_type STRING,\n" +
                "    search_keywords ARRAY<STRING>,\n" +
                "    behavior_time TIMESTAMP(3),\n" +
                "    page_url STRING,\n" +
                "    data_source STRING,\n" +
                "    sync_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR sync_time AS sync_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://192.168.142.137:3306/refund',\n" +
                "    'table-name' = 'ods_user_behavior',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root'\n" +
                ");");

        // 1.5 读取异常数据表
        tEnv.executeSql("CREATE TABLE `default_catalog`.`default_database`.`ods_error_data_source` (\n" +
                "    error_id STRING,\n" +
                "    table_name STRING,\n" +
                "    error_field STRING,\n" +
                "    error_reason STRING,\n" +
                "    raw_data STRING,\n" +
                "    create_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR create_time AS create_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://192.168.142.137:3306/refund',\n" +
                "    'table-name' = 'ods_error_data',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'root'\n" +
                ");");

        // 切换到 Paimon Catalog 创建 Paimon 表
        tEnv.executeSql("USE CATALOG `paimon_cat`");

        // 删除已存在的表（如果存在）
        try {
            tEnv.executeSql("DROP TABLE IF EXISTS `dwd_order_refund_detail`");
        } catch (Exception e) {
            System.out.println("表 dwd_order_refund_detail 不存在或删除失败: " + e.getMessage());
        }

        try {
            tEnv.executeSql("DROP TABLE IF EXISTS `dwd_order_logistics_detail`");
        } catch (Exception e) {
            System.out.println("表 dwd_order_logistics_detail 不存在或删除失败: " + e.getMessage());
        }

        // ==================== 2. 创建DWD层宽表 ====================

        // 2.1 订单-退款关联宽表
        tEnv.executeSql("CREATE TABLE `dwd_order_refund_detail` (\n" +
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
                "    'file.format' = 'parquet',\n" +
                "    'sink.parallelism' = '1'\n" +
                ");");

        // 2.2 订单-物流关联宽表
        tEnv.executeSql("CREATE TABLE `dwd_order_logistics_detail` (\n" +
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
                "    'file.format' = 'parquet',\n" +
                "    'sink.parallelism' = '1'\n" +
                ");");

        // 切换回默认 Catalog 进行数据处理
        tEnv.executeSql("USE CATALOG `default_catalog`");

        // ==================== 3. 数据处理逻辑 ====================
        // 3.1 数据清洗和转换
        tEnv.executeSql("CREATE VIEW `cleaned_order` AS " +
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
                "FROM `default_catalog`.`default_database`.`ods_order_source` " +
                "WHERE order_amount > 0 AND order_id IS NOT NULL");

        // 3.2 多表关联处理 - 修正后的插入语句
        // 注意：Paimon表的路径应该是 paimon_cat.default 而不是 paimon_cat.default_database
        tEnv.executeSql("INSERT INTO `paimon_cat`.`default`.`dwd_order_refund_detail` " +
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
                "FROM `default_catalog`.`default_database`.`ods_order_source` o " +
                "LEFT JOIN `default_catalog`.`default_database`.`ods_refund_source` r ON o.order_id = r.order_id " +
                "WHERE o.sync_time >= CAST(CURRENT_DATE - INTERVAL '7' DAY AS TIMESTAMP)");

        tEnv.executeSql("INSERT INTO `paimon_cat`.`default`.`dwd_order_logistics_detail` " +
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
                "FROM `default_catalog`.`default_database`.`ods_order_source` o " +
                "LEFT JOIN `default_catalog`.`default_database`.`ods_logistics_source` l ON o.order_id = l.order_id " +
                "WHERE o.sync_time >= CAST(CURRENT_DATE - INTERVAL '7' DAY AS TIMESTAMP)");



        // ==================== 4. 输出到控制台 ====================
// ==================== 4. 输出到控制台 ====================
// 输出订单-退款关联宽表数据到控制台
        tEnv.executeSql("CREATE TABLE `default_catalog`.`default_database`.`print_order_refund` (\n" +
                "    order_id STRING,\n" +
                "    user_id STRING,\n" +
                "    sku_id STRING,\n" +
                "    order_amount DECIMAL(18,2),\n" +
                "    pay_time TIMESTAMP(3),\n" +
                "    commit_ship_time TIMESTAMP(3),\n" +
                "    order_status STRING,\n" +
                "    create_time TIMESTAMP(3),\n" +
                "    refund_id STRING,\n" +
                "    refund_amount DECIMAL(18,2),\n" +
                "    refund_status STRING,\n" +
                "    refund_apply_time TIMESTAMP(3),\n" +
                "    user_input_reason STRING,\n" +
                "    refund_finish_time TIMESTAMP(3),\n" +
                "    data_source STRING,\n" +
                "    sync_time TIMESTAMP(3),\n" +
                "    dt STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ");");

// 输出订单-物流关联宽表数据到控制台
        tEnv.executeSql("CREATE TABLE `default_catalog`.`default_database`.`print_order_logistics` (\n" +
                "    order_id STRING,\n" +
                "    user_id STRING,\n" +
                "    sku_id STRING,\n" +
                "    order_amount DECIMAL(18,2),\n" +
                "    pay_time TIMESTAMP(3),\n" +
                "    commit_ship_time TIMESTAMP(3),\n" +
                "    order_status STRING,\n" +
                "    create_time TIMESTAMP(3),\n" +
                "    logistics_id STRING,\n" +
                "    logistics_node STRING,\n" +
                "    node_time TIMESTAMP(3),\n" +
                "    express_company STRING,\n" +
                "    logistics_desc STRING,\n" +
                "    data_source STRING,\n" +
                "    sync_time TIMESTAMP(3),\n" +
                "    dt STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ");");

// 创建ODS数据调试输出表
        tEnv.executeSql("CREATE TABLE `default_catalog`.`default_database`.`print_ods_order` (\n" +
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
                "    'connector' = 'print'\n" +
                ");");

        System.out.println("开始执行数据插入操作...");

// 将DWD层数据插入到控制台输出表
        tEnv.executeSql("INSERT INTO `default_catalog`.`default_database`.`print_order_refund` " +
                "SELECT * FROM `paimon_cat`.`default`.`dwd_order_refund_detail`");

        tEnv.executeSql("INSERT INTO `default_catalog`.`default_database`.`print_order_logistics` " +
                "SELECT * FROM `paimon_cat`.`default`.`dwd_order_logistics_detail`");

// 直接从ODS层输出数据进行调试
        tEnv.executeSql("INSERT INTO `default_catalog`.`default_database`.`print_ods_order` " +
                "SELECT * FROM `default_catalog`.`default_database`.`ods_order_source`");

// 使用Statement Set方式执行所有INSERT语句
        try {
            org.apache.flink.table.api.StatementSet statementSet = tEnv.createStatementSet();

            statementSet.addInsertSql("INSERT INTO `default_catalog`.`default_database`.`print_order_refund` " +
                    "SELECT * FROM `paimon_cat`.`default`.`dwd_order_refund_detail`");

            statementSet.addInsertSql("INSERT INTO `default_catalog`.`default_database`.`print_order_logistics` " +
                    "SELECT * FROM `paimon_cat`.`default`.`dwd_order_logistics_detail`");

            statementSet.addInsertSql("INSERT INTO `default_catalog`.`default_database`.`print_ods_order` " +
                    "SELECT * FROM `default_catalog`.`default_database`.`ods_order_source`");

            org.apache.flink.table.api.TableResult result = statementSet.execute();
            System.out.println("作业执行成功，作业ID: " + result.getJobClient().get().getJobID());
            System.out.println("请等待数据处理和输出...");

        } catch (Exception e) {
            System.err.println("作业执行失败:");
            e.printStackTrace();
        }

    }
}
