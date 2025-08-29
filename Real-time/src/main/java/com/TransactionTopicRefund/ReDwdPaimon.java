package com.TransactionTopicRefund;

import common.utils.ConfigUtils;
import common.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * DWD层：直接基于ODS层数据进行扩展处理
 * 不使用维度表，仅使用已有的ODS表数据
 */
public class ReDwdPaimon {

    private static final String PAIMON_WAREHOUSE_PATH = "hdfs://cdh01:8020/refund_data/dwd";

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // 创建 TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建 Paimon Catalog
        tableEnv.executeSql("CREATE CATALOG paimon_cat WITH (" +
                "'type' = 'paimon'," +
                "'warehouse' = '" + PAIMON_WAREHOUSE_PATH + "'" +
                ")");

        // ==================== 1. 确保ODS层Paimon表存在 ====================
        // 切换到Paimon Catalog
        tableEnv.executeSql("USE CATALOG paimon_cat");

        // 确保ODS表存在（这些表已经在ReCdcDimPaimon中创建）
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `paimon_cat`.`default`.`ods_order_paimon` (" +
                "    order_id STRING," +
                "    user_id STRING," +
                "    sku_id STRING," +
                "    order_amount DECIMAL(18,2)," +
                "    pay_time TIMESTAMP(3)," +
                "    commit_ship_time TIMESTAMP(3)," +
                "    order_status STRING," +
                "    create_time TIMESTAMP(3)," +
                "    data_source STRING," +
                "    sync_time TIMESTAMP(3)," +
                "    db_name STRING," +
                "    table_name STRING" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `paimon_cat`.`default`.`ods_refund_paimon` (" +
                "    refund_id STRING," +
                "    order_id STRING," +
                "    user_id STRING," +
                "    refund_amount DECIMAL(18,2)," +
                "    refund_status STRING," +
                "    refund_apply_time TIMESTAMP(3)," +
                "    user_input_reason STRING," +
                "    refund_finish_time TIMESTAMP(3)," +
                "    data_source STRING," +
                "    sync_time TIMESTAMP(3)," +
                "    db_name STRING," +
                "    table_name STRING" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `paimon_cat`.`default`.`ods_logistics_paimon` (" +
                "    logistics_id STRING," +
                "    order_id STRING," +
                "    logistics_node STRING," +
                "    node_time TIMESTAMP(3)," +
                "    express_company STRING," +
                "    logistics_desc STRING," +
                "    data_source STRING," +
                "    sync_time TIMESTAMP(3)," +
                "    db_name STRING," +
                "    table_name STRING" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `paimon_cat`.`default`.`ods_user_behavior_paimon` (" +
                "    behavior_id STRING," +
                "    user_id STRING," +
                "    behavior_type STRING," +
                "    search_keywords ARRAY<STRING>," +
                "    behavior_time TIMESTAMP(3)," +
                "    page_url STRING," +
                "    data_source STRING," +
                "    sync_time TIMESTAMP(3)," +
                "    db_name STRING," +
                "    table_name STRING" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // ==================== 2. 删除已存在的表并重新创建DWD层宽表 ====================
        // 2.1 删除已存在的订单宽表并重新创建
        tableEnv.executeSql("DROP TABLE IF EXISTS `paimon_cat`.`default`.`dwd_order_wide`");
        tableEnv.executeSql("CREATE TABLE `paimon_cat`.`default`.`dwd_order_wide` (" +
                "    order_id STRING," +
                "    user_id STRING," +
                "    sku_id STRING," +
                "    order_amount DECIMAL(18,2)," +
                "    pay_time TIMESTAMP(3)," +
                "    commit_ship_time TIMESTAMP(3)," +
                "    order_status STRING," +
                "    create_time TIMESTAMP(3)," +
                "    data_source STRING," +
                "    sync_time TIMESTAMP(3)," +
                "    refund_amount DECIMAL(18,2)," +
                "    refund_status STRING," +
                "    logistics_node STRING," +
                "    node_time TIMESTAMP(3)," +
                "    express_company STRING," +
                "    behavior_type STRING," +
                "    behavior_time TIMESTAMP(3)," +
                "    page_url STRING" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // 2.2 删除已存在的退款宽表并重新创建
        tableEnv.executeSql("DROP TABLE IF EXISTS `paimon_cat`.`default`.`dwd_refund_wide`");
        tableEnv.executeSql("CREATE TABLE `paimon_cat`.`default`.`dwd_refund_wide` (" +
                "    refund_id STRING," +
                "    order_id STRING," +
                "    user_id STRING," +
                "    refund_amount DECIMAL(18,2)," +
                "    refund_status STRING," +
                "    refund_apply_time TIMESTAMP(3)," +
                "    user_input_reason STRING," +
                "    refund_finish_time TIMESTAMP(3)," +
                "    data_source STRING," +
                "    sync_time TIMESTAMP(3)," +
                "    order_amount DECIMAL(18,2)," +
                "    order_status STRING," +
                "    pay_time TIMESTAMP(3)," +
                "    logistics_node STRING," +
                "    node_time TIMESTAMP(3)," +
                "    express_company STRING" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // 2.3 删除已存在的用户行为宽表并重新创建
        tableEnv.executeSql("DROP TABLE IF EXISTS `paimon_cat`.`default`.`dwd_user_behavior_wide`");
        tableEnv.executeSql("CREATE TABLE `paimon_cat`.`default`.`dwd_user_behavior_wide` (" +
                "    behavior_id STRING," +
                "    user_id STRING," +
                "    behavior_type STRING," +
                "    search_keywords ARRAY<STRING>," +
                "    behavior_time TIMESTAMP(3)," +
                "    page_url STRING," +
                "    data_source STRING," +
                "    sync_time TIMESTAMP(3)," +
                "    order_id STRING," +
                "    order_status STRING," +
                "    order_amount DECIMAL(18,2)" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // ==================== 3. 数据关联处理（仅使用ODS表）====================
        // 3.1 订单宽表数据处理
        tableEnv.executeSql(
                "INSERT INTO `paimon_cat`.`default`.`dwd_order_wide` " +
                        "SELECT " +
                        "    o.order_id," +
                        "    o.user_id," +
                        "    o.sku_id," +
                        "    o.order_amount," +
                        "    o.pay_time," +
                        "    o.commit_ship_time," +
                        "    o.order_status," +
                        "    o.create_time," +
                        "    o.data_source," +
                        "    o.sync_time," +
                        "    r.refund_amount," +
                        "    r.refund_status," +
                        "    l.logistics_node," +
                        "    l.node_time," +
                        "    l.express_company," +
                        "    b.behavior_type," +
                        "    b.behavior_time," +
                        "    b.page_url " +
                        "FROM `paimon_cat`.`default`.`ods_order_paimon` o " +
                        "LEFT JOIN `paimon_cat`.`default`.`ods_refund_paimon` r " +
                        "    ON o.order_id = r.order_id " +
                        "LEFT JOIN `paimon_cat`.`default`.`ods_logistics_paimon` l " +
                        "    ON o.order_id = l.order_id " +
                        "LEFT JOIN `paimon_cat`.`default`.`ods_user_behavior_paimon` b " +
                        "    ON o.user_id = b.user_id"
        );

        // 3.2 退款宽表数据处理
        tableEnv.executeSql(
                "INSERT INTO `paimon_cat`.`default`.`dwd_refund_wide` " +
                        "SELECT " +
                        "    r.refund_id," +
                        "    r.order_id," +
                        "    r.user_id," +
                        "    r.refund_amount," +
                        "    r.refund_status," +
                        "    r.refund_apply_time," +
                        "    r.user_input_reason," +
                        "    r.refund_finish_time," +
                        "    r.data_source," +
                        "    r.sync_time," +
                        "    o.order_amount," +
                        "    o.order_status," +
                        "    o.pay_time," +
                        "    l.logistics_node," +
                        "    l.node_time," +
                        "    l.express_company " +
                        "FROM `paimon_cat`.`default`.`ods_refund_paimon` r " +
                        "LEFT JOIN `paimon_cat`.`default`.`ods_order_paimon` o " +
                        "    ON r.order_id = o.order_id " +
                        "LEFT JOIN `paimon_cat`.`default`.`ods_logistics_paimon` l " +
                        "    ON r.order_id = l.order_id"
        );

        // 3.3 用户行为宽表数据处理
        tableEnv.executeSql(
                "INSERT INTO `paimon_cat`.`default`.`dwd_user_behavior_wide` " +
                        "SELECT " +
                        "    b.behavior_id," +
                        "    b.user_id," +
                        "    b.behavior_type," +
                        "    b.search_keywords," +
                        "    b.behavior_time," +
                        "    b.page_url," +
                        "    b.data_source," +
                        "    b.sync_time," +
                        "    o.order_id," +
                        "    o.order_status," +
                        "    o.order_amount " +
                        "FROM `paimon_cat`.`default`.`ods_user_behavior_paimon` b " +
                        "LEFT JOIN `paimon_cat`.`default`.`ods_order_paimon` o " +
                        "    ON b.user_id = o.user_id"
        );
    }
}
