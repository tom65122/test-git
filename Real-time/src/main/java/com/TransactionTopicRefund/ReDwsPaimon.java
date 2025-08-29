package com.TransactionTopicRefund;

import common.utils.ConfigUtils;
import common.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * DWS层：基于DWD层数据生成聚合指标和衍生字段
 * 创建各种业务统计表
 */
public class ReDwsPaimon {

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

        // ==================== 1. 切换到Paimon Catalog ====================
        tableEnv.executeSql("USE CATALOG paimon_cat");

        // ==================== 2. 删除并重新创建DWS层聚合表 ====================
        // 2.1 删除并重新创建订单统计表（按用户）
        tableEnv.executeSql("DROP TABLE IF EXISTS `paimon_cat`.`default`.`dws_order_stats_by_user`");
        tableEnv.executeSql("CREATE TABLE `paimon_cat`.`default`.`dws_order_stats_by_user` (" +
                "    user_id STRING," +
                "    order_count BIGINT," +
                "    total_order_amount DECIMAL(18,2)," +
                "    avg_order_amount DECIMAL(18,2)," +
                "    max_order_amount DECIMAL(18,2)," +
                "    min_order_amount DECIMAL(18,2)," +
                "    first_order_time TIMESTAMP(3)," +
                "    last_order_time TIMESTAMP(3)," +
                "    has_refund_order BOOLEAN," +
                "    data_source STRING," +
                "    stats_time TIMESTAMP(3)" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // 2.2 删除并重新创建退款统计表（按用户）
        tableEnv.executeSql("DROP TABLE IF EXISTS `paimon_cat`.`default`.`dws_refund_stats_by_user`");
        tableEnv.executeSql("CREATE TABLE `paimon_cat`.`default`.`dws_refund_stats_by_user` (" +
                "    user_id STRING," +
                "    refund_count BIGINT," +
                "    total_refund_amount DECIMAL(18,2)," +
                "    avg_refund_amount DECIMAL(18,2)," +
                "    refund_rate DECIMAL(5,2)," +
                "    first_refund_time TIMESTAMP(3)," +
                "    last_refund_time TIMESTAMP(3)," +
                "    data_source STRING," +
                "    stats_time TIMESTAMP(3)" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // 2.3 删除并重新创建物流时效统计表（按快递公司）
        tableEnv.executeSql("DROP TABLE IF EXISTS `paimon_cat`.`default`.`dws_logistics_efficiency_by_company`");
        tableEnv.executeSql("CREATE TABLE `paimon_cat`.`default`.`dws_logistics_efficiency_by_company` (" +
                "    express_company STRING," +
                "    delivery_count BIGINT," +
                "    on_time_delivery_count BIGINT," +
                "    on_time_delivery_rate DECIMAL(5,2)," +
                "    data_source STRING," +
                "    stats_time TIMESTAMP(3)" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // 2.4 删除并重新创建用户行为分析表
        tableEnv.executeSql("DROP TABLE IF EXISTS `paimon_cat`.`default`.`dws_user_behavior_analysis`");
        tableEnv.executeSql("CREATE TABLE `paimon_cat`.`default`.`dws_user_behavior_analysis` (" +
                "    user_id STRING," +
                "    behavior_count BIGINT," +
                "    distinct_behavior_types BIGINT," +
                "    first_behavior_time TIMESTAMP(3)," +
                "    last_behavior_time TIMESTAMP(3)," +
                "    data_source STRING," +
                "    stats_time TIMESTAMP(3)" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // 2.5 删除并重新创建订单退款关联分析表
        tableEnv.executeSql("DROP TABLE IF EXISTS `paimon_cat`.`default`.`dws_order_refund_correlation`");
        tableEnv.executeSql("CREATE TABLE `paimon_cat`.`default`.`dws_order_refund_correlation` (" +
                "    order_status STRING," +
                "    order_count BIGINT," +
                "    refund_order_count BIGINT," +
                "    refund_rate DECIMAL(5,2)," +
                "    avg_refund_amount DECIMAL(18,2)," +
                "    data_source STRING," +
                "    stats_time TIMESTAMP(3)" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // ==================== 3. 数据聚合处理 ====================
        // 3.1 用户订单统计
        tableEnv.executeSql(
                "INSERT INTO `paimon_cat`.`default`.`dws_order_stats_by_user` " +
                        "SELECT " +
                        "    user_id," +
                        "    COUNT(*) AS order_count," +
                        "    SUM(order_amount) AS total_order_amount," +
                        "    AVG(order_amount) AS avg_order_amount," +
                        "    MAX(order_amount) AS max_order_amount," +
                        "    MIN(order_amount) AS min_order_amount," +
                        "    MIN(create_time) AS first_order_time," +
                        "    MAX(create_time) AS last_order_time," +
                        "    MAX(CASE WHEN refund_amount > 0 THEN true ELSE false END) AS has_refund_order," +
                        "    MAX(data_source) AS data_source," +
                        "    CURRENT_TIMESTAMP AS stats_time " +
                        "FROM `paimon_cat`.`default`.`dwd_order_wide` " +
                        "GROUP BY user_id"
        );

        // 3.2 用户退款统计
        tableEnv.executeSql(
                "INSERT INTO `paimon_cat`.`default`.`dws_refund_stats_by_user` " +
                        "SELECT " +
                        "    user_id," +
                        "    COUNT(*) AS refund_count," +
                        "    SUM(refund_amount) AS total_refund_amount," +
                        "    AVG(refund_amount) AS avg_refund_amount," +
                        "    CAST(COUNT(*) AS DECIMAL(5,2)) / CAST((SELECT COUNT(DISTINCT user_id) FROM `paimon_cat`.`default`.`dwd_refund_wide`) AS DECIMAL(5,2)) * 100 AS refund_rate," +
                        "    MIN(refund_apply_time) AS first_refund_time," +
                        "    MAX(refund_apply_time) AS last_refund_time," +
                        "    MAX(data_source) AS data_source," +
                        "    CURRENT_TIMESTAMP AS stats_time " +
                        "FROM `paimon_cat`.`default`.`dwd_refund_wide` " +
                        "GROUP BY user_id"
        );

        // 3.3 物流时效统计
        tableEnv.executeSql(
                "INSERT INTO `paimon_cat`.`default`.`dws_logistics_efficiency_by_company` " +
                        "SELECT " +
                        "    express_company," +
                        "    COUNT(*) AS delivery_count," +
                        "    SUM(CASE WHEN node_time <= create_time + INTERVAL '3' DAY THEN 1 ELSE 0 END) AS on_time_delivery_count," +
                        "    CAST(SUM(CASE WHEN node_time <= create_time + INTERVAL '3' DAY THEN 1 ELSE 0 END) AS DECIMAL(5,2)) / CAST(COUNT(*) AS DECIMAL(5,2)) * 100 AS on_time_delivery_rate," +
                        "    MAX(data_source) AS data_source," +
                        "    CURRENT_TIMESTAMP AS stats_time " +
                        "FROM `paimon_cat`.`default`.`dwd_order_wide` " +
                        "WHERE express_company IS NOT NULL AND node_time IS NOT NULL AND create_time IS NOT NULL " +
                        "GROUP BY express_company"
        );

        // 3.4 用户行为分析 (移除MAX_BY函数)
        tableEnv.executeSql(
                "INSERT INTO `paimon_cat`.`default`.`dws_user_behavior_analysis` " +
                        "SELECT " +
                        "    user_id," +
                        "    COUNT(*) AS behavior_count," +
                        "    COUNT(DISTINCT behavior_type) AS distinct_behavior_types," +
                        "    MIN(behavior_time) AS first_behavior_time," +
                        "    MAX(behavior_time) AS last_behavior_time," +
                        "    MAX(data_source) AS data_source," +
                        "    CURRENT_TIMESTAMP AS stats_time " +
                        "FROM `paimon_cat`.`default`.`dwd_user_behavior_wide` " +
                        "GROUP BY user_id"
        );

        // 3.5 订单退款关联分析
        tableEnv.executeSql(
                "INSERT INTO `paimon_cat`.`default`.`dws_order_refund_correlation` " +
                        "SELECT " +
                        "    order_status," +
                        "    COUNT(*) AS order_count," +
                        "    COUNT(refund_id) AS refund_order_count," +
                        "    CAST(COUNT(refund_id) AS DECIMAL(5,2)) / CAST(COUNT(*) AS DECIMAL(5,2)) * 100 AS refund_rate," +
                        "    AVG(refund_amount) AS avg_refund_amount," +
                        "    MAX(data_source) AS data_source," +
                        "    CURRENT_TIMESTAMP AS stats_time " +
                        "FROM (" +
                        "    SELECT o.order_status, o.order_id, o.data_source, " +
                        "           r.refund_id, r.refund_amount " +
                        "    FROM `paimon_cat`.`default`.`dwd_order_wide` o " +
                        "    LEFT JOIN `paimon_cat`.`default`.`dwd_refund_wide` r " +
                        "    ON o.order_id = r.order_id" +
                        ") " +
                        "GROUP BY order_status"
        );


    }
}
