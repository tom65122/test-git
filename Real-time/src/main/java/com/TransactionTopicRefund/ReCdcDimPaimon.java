package com.TransactionTopicRefund;

import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import common.utils.ConfigUtils;
import common.utils.EnvironmentSettingUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 从 MySQL 读取五个表的数据并以 Paimon 格式存储
 */
public class ReCdcDimPaimon {

    private static final String MYSQL_HOST = ConfigUtils.getString("mysql.host");
    private static final String MYSQL_PORT = ConfigUtils.getString("mysql.port");
    private static final String MYSQL_DATABASE = ConfigUtils.getString("mysql.database");
    private static final String MYSQL_USER = ConfigUtils.getString("mysql.user");
    private static final String MYSQL_PWD = ConfigUtils.getString("mysql.pwd");
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

        // ==================== 1. 创建 MySQL CDC 表 ====================
        // 1.1 订单表 ODS
        tableEnv.executeSql("CREATE TABLE `default_catalog`.`default_database`.`ods_order_source` (" +
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
                "    db_name STRING METADATA FROM 'database_name' VIRTUAL," +
                "    table_name STRING METADATA FROM 'table_name' VIRTUAL" +
                ") WITH (" +
                "    'connector' = 'mysql-cdc'," +
                "    'hostname' = '" + MYSQL_HOST + "'," +
                "    'port' = '" + MYSQL_PORT + "'," +
                "    'username' = '" + MYSQL_USER + "'," +
                "    'password' = '" + MYSQL_PWD + "'," +
                "    'database-name' = '" + MYSQL_DATABASE + "'," +
                "    'table-name' = 'ods_order'," +
                "    'scan.startup.mode' = 'initial'," +
                "    'scan.incremental.snapshot.chunk.key-column' = 'order_id'" +
                ")");

        // 1.2 退款表 ODS
        tableEnv.executeSql("CREATE TABLE `default_catalog`.`default_database`.`ods_refund_source` (" +
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
                "    db_name STRING METADATA FROM 'database_name' VIRTUAL," +
                "    table_name STRING METADATA FROM 'table_name' VIRTUAL" +
                ") WITH (" +
                "    'connector' = 'mysql-cdc'," +
                "    'hostname' = '" + MYSQL_HOST + "'," +
                "    'port' = '" + MYSQL_PORT + "'," +
                "    'username' = '" + MYSQL_USER + "'," +
                "    'password' = '" + MYSQL_PWD + "'," +
                "    'database-name' = '" + MYSQL_DATABASE + "'," +
                "    'table-name' = 'ods_refund'," +
                "    'scan.startup.mode' = 'initial'," +
                "    'scan.incremental.snapshot.chunk.key-column' = 'refund_id'" +
                ")");

        // 1.3 物流表 ODS
        tableEnv.executeSql("CREATE TABLE `default_catalog`.`default_database`.`ods_logistics_source` (" +
                "    logistics_id STRING," +
                "    order_id STRING," +
                "    logistics_node STRING," +
                "    node_time TIMESTAMP(3)," +
                "    express_company STRING," +
                "    logistics_desc STRING," +
                "    data_source STRING," +
                "    sync_time TIMESTAMP(3)," +
                "    db_name STRING METADATA FROM 'database_name' VIRTUAL," +
                "    table_name STRING METADATA FROM 'table_name' VIRTUAL" +
                ") WITH (" +
                "    'connector' = 'mysql-cdc'," +
                "    'hostname' = '" + MYSQL_HOST + "'," +
                "    'port' = '" + MYSQL_PORT + "'," +
                "    'username' = '" + MYSQL_USER + "'," +
                "    'password' = '" + MYSQL_PWD + "'," +
                "    'database-name' = '" + MYSQL_DATABASE + "'," +
                "    'table-name' = 'ods_logistics'," +
                "    'scan.startup.mode' = 'initial'," +
                "    'scan.incremental.snapshot.chunk.key-column' = 'logistics_id'" +
                ")");

        // 1.4 用户行为表 ODS
        tableEnv.executeSql("CREATE TABLE `default_catalog`.`default_database`.`ods_user_behavior_source` (" +
                "    behavior_id STRING," +
                "    user_id STRING," +
                "    behavior_type STRING," +
                "    search_keywords ARRAY<STRING>," +
                "    behavior_time TIMESTAMP(3)," +
                "    page_url STRING," +
                "    data_source STRING," +
                "    sync_time TIMESTAMP(3)," +
                "    db_name STRING METADATA FROM 'database_name' VIRTUAL," +
                "    table_name STRING METADATA FROM 'table_name' VIRTUAL" +
                ") WITH (" +
                "    'connector' = 'mysql-cdc'," +
                "    'hostname' = '" + MYSQL_HOST + "'," +
                "    'port' = '" + MYSQL_PORT + "'," +
                "    'username' = '" + MYSQL_USER + "'," +
                "    'password' = '" + MYSQL_PWD + "'," +
                "    'database-name' = '" + MYSQL_DATABASE + "'," +
                "    'table-name' = 'ods_user_behavior'," +
                "    'scan.startup.mode' = 'initial'," +
                "    'scan.incremental.snapshot.chunk.key-column' = 'behavior_id'" +
                ")");

        // 1.5 异常数据表 ODS
        tableEnv.executeSql("CREATE TABLE `default_catalog`.`default_database`.`ods_error_data_source` (" +
                "    error_id STRING," +
                "    table_name STRING," +
                "    error_field STRING," +
                "    error_reason STRING," +
                "    raw_data STRING," +
                "    create_time TIMESTAMP(3)," +
                "    db_name STRING METADATA FROM 'database_name' VIRTUAL," +
                "    table_name_source STRING METADATA FROM 'table_name' VIRTUAL" +
                ") WITH (" +
                "    'connector' = 'mysql-cdc'," +
                "    'hostname' = '" + MYSQL_HOST + "'," +
                "    'port' = '" + MYSQL_PORT + "'," +
                "    'username' = '" + MYSQL_USER + "'," +
                "    'password' = '" + MYSQL_PWD + "'," +
                "    'database-name' = '" + MYSQL_DATABASE + "'," +
                "    'table-name' = 'ods_error_data'," +
                "    'scan.startup.mode' = 'initial'," +
                "    'scan.incremental.snapshot.chunk.key-column' = 'error_id'" +
                ")");

        // ==================== 2. 切换到 Paimon Catalog 创建目标表 ====================
        tableEnv.executeSql("USE CATALOG `paimon_cat`");

        // 2.1 创建订单表 Paimon 表
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

        // 2.2 创建退款表 Paimon 表
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

        // 2.3 创建物流表 Paimon 表
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

        // 2.4 创建用户行为表 Paimon 表
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

        // 2.5 创建异常数据表 Paimon 表
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `paimon_cat`.`default`.`ods_error_data_paimon` (" +
                "    error_id STRING," +
                "    table_name STRING," +
                "    error_field STRING," +
                "    error_reason STRING," +
                "    raw_data STRING," +
                "    create_time TIMESTAMP(3)," +
                "    db_name STRING," +
                "    table_name_source STRING" +
                ") WITH (" +
                "    'file.format' = 'parquet'," +
                "    'bucket' = '2'" +
                ")");

        // ==================== 3. 数据插入操作 ====================
        // 切换回默认 Catalog
        tableEnv.executeSql("USE CATALOG `default_catalog`");

        // 3.1 订单表数据插入
        tableEnv.executeSql("INSERT INTO `paimon_cat`.`default`.`ods_order_paimon` " +
                "SELECT * FROM `default_catalog`.`default_database`.`ods_order_source`");

        // 3.2 退款表数据插入
        tableEnv.executeSql("INSERT INTO `paimon_cat`.`default`.`ods_refund_paimon` " +
                "SELECT * FROM `default_catalog`.`default_database`.`ods_refund_source`");

        // 3.3 物流表数据插入
        tableEnv.executeSql("INSERT INTO `paimon_cat`.`default`.`ods_logistics_paimon` " +
                "SELECT * FROM `default_catalog`.`default_database`.`ods_logistics_source`");

        // 3.4 用户行为表数据插入
        tableEnv.executeSql("INSERT INTO `paimon_cat`.`default`.`ods_user_behavior_paimon` " +
                "SELECT * FROM `default_catalog`.`default_database`.`ods_user_behavior_source`");

        // 3.5 异常数据表数据插入
        tableEnv.executeSql("INSERT INTO `paimon_cat`.`default`.`ods_error_data_paimon` " +
                "SELECT * FROM `default_catalog`.`default_database`.`ods_error_data_source`");

        // 移除原来的 env.execute() 调用，Table API/SQL 操作会自动执行
    }
}
