package com.TransactionTopicRefund;

import common.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ADS层：基于DWS层数据生成面向应用的指标表，支持FineReport可视化
 * 该层主要为数据应用层提供直接可用的指标数据
 */
public class ReAdsPaimon {

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

        // ==================== 2. 创建ADS层指标表 ====================

        // 2.1 创建用户综合画像表（包含订单和退款信息）
        tableEnv.executeSql("DROP TABLE IF EXISTS `paimon_cat`.`default`.`ads_user_profile`");
        tableEnv.executeSql("CREATE TABLE `paimon_cat`.`default`.`ads_user_profile` (" +
                "    user_id STRING," +
                "    order_count BIGINT," +
                "    refund_count BIGINT," +
                "    total_order_amount DECIMAL(18,2)," +
                "    total_refund_amount DECIMAL(18,2)," +
                "    avg_order_amount DECIMAL(18,2)," +
                "    refund_rate DECIMAL(5,2)," +
                "    first_order_time TIMESTAMP(3)," +
                "    last_order_time TIMESTAMP(3)," +
                "    first_refund_time TIMESTAMP(3)," +
                "    last_refund_time TIMESTAMP(3)," +
                "    is_risk_user BOOLEAN," +
                "    user_value_level STRING," + // 用户价值等级: high, medium, low
                "    data_source STRING," +
                "    stats_time TIMESTAMP(3)" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // 2.2 创建快递公司综合评价表（移除OVER窗口）
        tableEnv.executeSql("DROP TABLE IF EXISTS `paimon_cat`.`default`.`ads_express_company_evaluation`");
        tableEnv.executeSql("CREATE TABLE `paimon_cat`.`default`.`ads_express_company_evaluation` (" +
                "    express_company STRING," +
                "    delivery_count BIGINT," +
                "    on_time_delivery_count BIGINT," +
                "    on_time_delivery_rate DECIMAL(5,2)," +
                "    service_level STRING," + // 服务等级: excellent, good, fair, poor
                "    data_source STRING," +
                "    stats_time TIMESTAMP(3)" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // 2.3 创建订单状态分析表
        tableEnv.executeSql("DROP TABLE IF EXISTS `paimon_cat`.`default`.`ads_order_status_analysis`");
        tableEnv.executeSql("CREATE TABLE `paimon_cat`.`default`.`ads_order_status_analysis` (" +
                "    order_status STRING," +
                "    order_count BIGINT," +
                "    refund_order_count BIGINT," +
                "    refund_rate DECIMAL(5,2)," +
                "    avg_refund_amount DECIMAL(18,2)," +
                "    status_description STRING," + // 状态描述
                "    data_source STRING," +
                "    stats_time TIMESTAMP(3)" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // 2.4 创建整体业务指标表
        tableEnv.executeSql("DROP TABLE IF EXISTS `paimon_cat`.`default`.`ads_overall_metrics`");
        tableEnv.executeSql("CREATE TABLE `paimon_cat`.`default`.`ads_overall_metrics` (" +
                "    metric_name STRING," +
                "    metric_value DECIMAL(18,2)," +
                "    metric_description STRING," +
                "    data_source STRING," +
                "    stats_time TIMESTAMP(3)" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // 2.5 创建用户行为趋势表
        tableEnv.executeSql("DROP TABLE IF EXISTS `paimon_cat`.`default`.`ads_user_behavior_trend`");
        tableEnv.executeSql("CREATE TABLE `paimon_cat`.`default`.`ads_user_behavior_trend` (" +
                "    stat_date DATE," +
                "    behavior_count BIGINT," +
                "    distinct_user_count BIGINT," +
                "    distinct_behavior_types BIGINT," +
                "    avg_behavior_per_user DECIMAL(10,2)," +
                "    data_source STRING," +
                "    stats_time TIMESTAMP(3)" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // 2.6 创建高价值用户分析表
        tableEnv.executeSql("DROP TABLE IF EXISTS `paimon_cat`.`default`.`ads_high_value_users`");
        tableEnv.executeSql("CREATE TABLE `paimon_cat`.`default`.`ads_high_value_users` (" +
                "    user_id STRING," +
                "    total_order_amount DECIMAL(18,2)," +
                "    order_count BIGINT," +
                "    avg_order_amount DECIMAL(18,2)," +
                "    last_order_time TIMESTAMP(3)," +
                "    days_since_last_order BIGINT," +
                "    data_source STRING," +
                "    stats_time TIMESTAMP(3)" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // 2.7 创建退款风险预警表
        tableEnv.executeSql("DROP TABLE IF EXISTS `paimon_cat`.`default`.`ads_refund_risk_warning`");
        tableEnv.executeSql("CREATE TABLE `paimon_cat`.`default`.`ads_refund_risk_warning` (" +
                "    user_id STRING," +
                "    refund_count BIGINT," +
                "    refund_rate DECIMAL(5,2)," +
                "    total_refund_amount DECIMAL(18,2)," +
                "    risk_level STRING," + // 风险等级: high, medium, low
                "    warning_reason STRING," +
                "    data_source STRING," +
                "    stats_time TIMESTAMP(3)" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // ==================== 3. 数据聚合处理 ====================

        // 3.1 生成用户综合画像数据
        tableEnv.executeSql(
                "INSERT INTO `paimon_cat`.`default`.`ads_user_profile` " +
                        "SELECT " +
                        "    o.user_id," +
                        "    o.order_count," +
                        "    COALESCE(r.refund_count, 0) AS refund_count," +
                        "    o.total_order_amount," +
                        "    COALESCE(r.total_refund_amount, 0) AS total_refund_amount," +
                        "    o.avg_order_amount," +
                        "    COALESCE(r.refund_rate, 0) AS refund_rate," +
                        "    o.first_order_time," +
                        "    o.last_order_time," +
                        "    r.first_refund_time," +
                        "    r.last_refund_time," +
                        "    CASE WHEN COALESCE(r.refund_rate, 0) > 50 THEN true ELSE false END AS is_risk_user," +
                        "    CASE " +
                        "        WHEN o.total_order_amount >= 10000 THEN 'high' " +
                        "        WHEN o.total_order_amount >= 5000 THEN 'medium' " +
                        "        ELSE 'low' " +
                        "    END AS user_value_level," +
                        "    o.data_source," +
                        "    CURRENT_TIMESTAMP AS stats_time " +
                        "FROM `paimon_cat`.`default`.`dws_order_stats_by_user` o " +
                        "LEFT JOIN `paimon_cat`.`default`.`dws_refund_stats_by_user` r " +
                        "ON o.user_id = r.user_id"
        );

        // 3.2 生成快递公司综合评价数据（移除OVER窗口）
        tableEnv.executeSql(
                "INSERT INTO `paimon_cat`.`default`.`ads_express_company_evaluation` " +
                        "SELECT " +
                        "    express_company," +
                        "    delivery_count," +
                        "    on_time_delivery_count," +
                        "    on_time_delivery_rate," +
                        "    CASE " +
                        "        WHEN on_time_delivery_rate >= 95 THEN 'excellent' " +
                        "        WHEN on_time_delivery_rate >= 90 THEN 'good' " +
                        "        WHEN on_time_delivery_rate >= 80 THEN 'fair' " +
                        "        ELSE 'poor' " +
                        "    END AS service_level," +
                        "    data_source," +
                        "    CURRENT_TIMESTAMP AS stats_time " +
                        "FROM `paimon_cat`.`default`.`dws_logistics_efficiency_by_company`"
        );

        // 3.3 生成订单状态分析数据
        tableEnv.executeSql(
                "INSERT INTO `paimon_cat`.`default`.`ads_order_status_analysis` " +
                        "SELECT " +
                        "    order_status," +
                        "    order_count," +
                        "    refund_order_count," +
                        "    refund_rate," +
                        "    avg_refund_amount," +
                        "    CASE order_status " +
                        "        WHEN 'completed' THEN '已完成' " +
                        "        WHEN 'cancelled' THEN '已取消' " +
                        "        WHEN 'refunded' THEN '已退款' " +
                        "        WHEN 'pending' THEN '待处理' " +
                        "        WHEN 'shipped' THEN '已发货' " +
                        "        ELSE '其他' " +
                        "    END AS status_description," +
                        "    data_source," +
                        "    CURRENT_TIMESTAMP AS stats_time " +
                        "FROM `paimon_cat`.`default`.`dws_order_refund_correlation`"
        );

        // 3.4 生成整体业务指标数据
        tableEnv.executeSql(
                "INSERT INTO `paimon_cat`.`default`.`ads_overall_metrics` " +
                        "SELECT 'total_orders', CAST(SUM(order_count) AS DECIMAL(18,2)), '总订单数', MAX(data_source), CURRENT_TIMESTAMP " +
                        "FROM `paimon_cat`.`default`.`dws_order_stats_by_user` " +
                        "UNION ALL " +
                        "SELECT 'total_refunds', CAST(SUM(refund_count) AS DECIMAL(18,2)), '总退款数', MAX(data_source), CURRENT_TIMESTAMP " +
                        "FROM `paimon_cat`.`default`.`dws_refund_stats_by_user` " +
                        "UNION ALL " +
                        "SELECT 'total_order_amount', SUM(total_order_amount), '订单总金额', MAX(data_source), CURRENT_TIMESTAMP " +
                        "FROM `paimon_cat`.`default`.`dws_order_stats_by_user` " +
                        "UNION ALL " +
                        "SELECT 'total_refund_amount', SUM(total_refund_amount), '退款总金额', MAX(data_source), CURRENT_TIMESTAMP " +
                        "FROM `paimon_cat`.`default`.`dws_refund_stats_by_user` " +
                        "UNION ALL " +
                        "SELECT 'avg_order_amount', AVG(avg_order_amount), '平均订单金额', MAX(data_source), CURRENT_TIMESTAMP " +
                        "FROM `paimon_cat`.`default`.`dws_order_stats_by_user` " +
                        "UNION ALL " +
                        "SELECT 'overall_refund_rate', " +
                        "       CAST(SUM(r.refund_count) AS DECIMAL(18,2)) / CAST(SUM(o.order_count) AS DECIMAL(18,2)) * 100, " +
                        "       '整体退款率', MAX(o.data_source), CURRENT_TIMESTAMP " +
                        "FROM `paimon_cat`.`default`.`dws_order_stats_by_user` o, `paimon_cat`.`default`.`dws_refund_stats_by_user` r " +
                        "LIMIT 1"
        );

        // 3.5 生成用户行为趋势数据（模拟按日期统计）
        tableEnv.executeSql(
                "INSERT INTO `paimon_cat`.`default`.`ads_user_behavior_trend` " +
                        "SELECT " +
                        "    CURRENT_DATE AS stat_date," +
                        "    SUM(behavior_count) AS behavior_count," +
                        "    COUNT(user_id) AS distinct_user_count," +
                        "    SUM(distinct_behavior_types) AS distinct_behavior_types," +
                        "    CAST(SUM(behavior_count) AS DECIMAL(10,2)) / COUNT(user_id) AS avg_behavior_per_user," +
                        "    MAX(data_source) AS data_source," +
                        "    CURRENT_TIMESTAMP AS stats_time " +
                        "FROM `paimon_cat`.`default`.`dws_user_behavior_analysis`"
        );

        // 3.6 生成高价值用户数据
        tableEnv.executeSql(
                "INSERT INTO `paimon_cat`.`default`.`ads_high_value_users` " +
                        "SELECT " +
                        "    user_id," +
                        "    total_order_amount," +
                        "    order_count," +
                        "    avg_order_amount," +
                        "    last_order_time," +
                        "    CAST(EXTRACT(DAY FROM (CURRENT_TIMESTAMP - last_order_time)) AS BIGINT) AS days_since_last_order," +
                        "    data_source," +
                        "    CURRENT_TIMESTAMP AS stats_time " +
                        "FROM `paimon_cat`.`default`.`dws_order_stats_by_user` " +
                        "WHERE total_order_amount >= 5000"
        );

        // 3.7 生成退款风险预警数据
        tableEnv.executeSql(
                "INSERT INTO `paimon_cat`.`default`.`ads_refund_risk_warning` " +
                        "SELECT " +
                        "    user_id," +
                        "    refund_count," +
                        "    refund_rate," +
                        "    total_refund_amount," +
                        "    CASE " +
                        "        WHEN refund_rate >= 50 OR refund_count >= 10 THEN 'high' " +
                        "        WHEN refund_rate >= 20 OR refund_count >= 5 THEN 'medium' " +
                        "        ELSE 'low' " +
                        "    END AS risk_level," +
                        "    CASE " +
                        "        WHEN refund_rate >= 50 THEN '退款率过高' " +
                        "        WHEN refund_count >= 10 THEN '退款次数过多' " +
                        "        WHEN total_refund_amount >= 5000 THEN '退款金额过大' " +
                        "        ELSE '正常' " +
                        "    END AS warning_reason," +
                        "    data_source," +
                        "    CURRENT_TIMESTAMP AS stats_time " +
                        "FROM `paimon_cat`.`default`.`dws_refund_stats_by_user` " +
                        "WHERE refund_rate >= 20 OR refund_count >= 5 OR total_refund_amount >= 5000"
        );
    }
}
