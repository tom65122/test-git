package com.retailersv1.dws;

import common.utils.ConfigUtils;
import common.utils.EnvironmentSettingUtils;
import common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
/**
 * @Description: DWS层数据同步到MySQL
 * @Author: zhongying.chen
 * @Date: 2022/9/12
 **/
public class DwsToMysqlSync {
    // DWS层Kafka主题
    private static final String DWS_USER_GOODS_STATS = ConfigUtils.getString("kafka.dws.user.goods.stats");
    private static final String DWS_INTERACTION_USER_STATS = ConfigUtils.getString("kafka.dws.interaction.user.stats");
    private static final String DWS_TRADE_PROVINCE_STATS = ConfigUtils.getString("kafka.dws.trade.province.stats");

    // MySQL配置
    private static final String MYSQL_URL = ConfigUtils.getString("mysql.url");
    private static final String MYSQL_USERNAME = ConfigUtils.getString("mysql.user");
    private static final String MYSQL_PASSWORD = ConfigUtils.getString("mysql.pwd");
    private static final String MYSQL_DRIVER = ConfigUtils.getString("mysql.driver");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setStateBackend(new MemoryStateBackend());
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 同步用户商品统计数据到MySQL
        syncUserGoodsStatsToMysql(tEnv);

        // 同步用户交互统计数据到MySQL
        syncUserInteractionStatsToMysql(tEnv);

        // 同步省份交易统计数据到MySQL
        syncProvinceTradeStatsToMysql(tEnv);

    }

    private static void syncUserGoodsStatsToMysql(StreamTableEnvironment tEnv) {
        // 从Kafka读取用户商品统计数据
        tEnv.executeSql("CREATE TABLE dws_user_goods_stats_kafka (\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  date_id STRING,\n" +
                "  order_count BIGINT,\n" +
                "  order_num BIGINT,\n" +
                "  order_original_amount DECIMAL(16,2),\n" +
                "  order_total_amount DECIMAL(16,2),\n" +
                "  order_activity_amount DECIMAL(16,2),\n" +
                "  order_coupon_amount DECIMAL(16,2),\n" +
                "  cart_add_count BIGINT,\n" +
                "  cart_add_num BIGINT,\n" +
                "  comment_count BIGINT,\n" +
                "  good_comment_count BIGINT\n" +
                ")" + SqlUtil.getKafka(DWS_USER_GOODS_STATS, "mysql_sync_group_1"));

        // 写入MySQL
        tEnv.executeSql("CREATE TABLE dws_user_goods_stats_mysql (\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  date_id STRING,\n" +
                "  order_count BIGINT,\n" +
                "  order_num BIGINT,\n" +
                "  order_original_amount DECIMAL(16,2),\n" +
                "  order_total_amount DECIMAL(16,2),\n" +
                "  order_activity_amount DECIMAL(16,2),\n" +
                "  order_coupon_amount DECIMAL(16,2),\n" +
                "  cart_add_count BIGINT,\n" +
                "  cart_add_num BIGINT,\n" +
                "  comment_count BIGINT,\n" +
                "  good_comment_count BIGINT,\n" +
                "  PRIMARY KEY (user_id, sku_id, date_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = '" + MYSQL_URL + "',\n" +
                "  'table-name' = 'dws_user_goods_stats',\n" +
                "  'username' = '" + MYSQL_USERNAME + "',\n" +
                "  'password' = '" + MYSQL_PASSWORD + "',\n" +
                "  'driver' = '" + MYSQL_DRIVER + "'\n" +
                ")");

        // 执行数据同步
        tEnv.executeSql("INSERT INTO dws_user_goods_stats_mysql SELECT * FROM dws_user_goods_stats_kafka");
    }

    private static void syncUserInteractionStatsToMysql(StreamTableEnvironment tEnv) {
        // 从Kafka读取用户交互统计数据
        tEnv.executeSql("CREATE TABLE dws_interaction_user_stats_kafka (\n" +
                "  user_id STRING,\n" +
                "  date_id STRING,\n" +
                "  cart_add_count BIGINT,\n" +
                "  cart_add_num BIGINT,\n" +
                "  comment_count BIGINT,\n" +
                "  good_comment_count BIGINT\n" +
                ")" + SqlUtil.getKafka(DWS_INTERACTION_USER_STATS, "mysql_sync_group_2"));

        // 写入MySQL
        tEnv.executeSql("CREATE TABLE dws_interaction_user_stats_mysql (\n" +
                "  user_id STRING,\n" +
                "  date_id STRING,\n" +
                "  cart_add_count BIGINT,\n" +
                "  cart_add_num BIGINT,\n" +
                "  comment_count BIGINT,\n" +
                "  good_comment_count BIGINT,\n" +
                "  PRIMARY KEY (user_id, date_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = '" + MYSQL_URL + "',\n" +
                "  'table-name' = 'dws_interaction_user_stats',\n" +
                "  'username' = '" + MYSQL_USERNAME + "',\n" +
                "  'password' = '" + MYSQL_PASSWORD + "',\n" +
                "  'driver' = '" + MYSQL_DRIVER + "'\n" +
                ")");

        // 执行数据同步
        tEnv.executeSql("INSERT INTO dws_interaction_user_stats_mysql SELECT * FROM dws_interaction_user_stats_kafka");
    }

    private static void syncProvinceTradeStatsToMysql(StreamTableEnvironment tEnv) {
        // 从Kafka读取省份交易统计数据
        tEnv.executeSql("CREATE TABLE dws_trade_province_stats_kafka (\n" +
                "  province_id STRING,\n" +
                "  date_id STRING,\n" +
                "  order_count BIGINT,\n" +
                "  order_num BIGINT,\n" +
                "  order_original_amount DECIMAL(16,2),\n" +
                "  order_total_amount DECIMAL(16,2),\n" +
                "  order_activity_amount DECIMAL(16,2),\n" +
                "  order_coupon_amount DECIMAL(16,2)\n" +
                ")" + SqlUtil.getKafka(DWS_TRADE_PROVINCE_STATS, "mysql_sync_group_3"));

        // 写入MySQL
        tEnv.executeSql("CREATE TABLE dws_trade_province_stats_mysql (\n" +
                "  province_id STRING,\n" +
                "  date_id STRING,\n" +
                "  order_count BIGINT,\n" +
                "  order_num BIGINT,\n" +
                "  order_original_amount DECIMAL(16,2),\n" +
                "  order_total_amount DECIMAL(16,2),\n" +
                "  order_activity_amount DECIMAL(16,2),\n" +
                "  order_coupon_amount DECIMAL(16,2),\n" +
                "  PRIMARY KEY (province_id, date_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = '" + MYSQL_URL + "',\n" +
                "  'table-name' = 'dws_trade_province_stats',\n" +
                "  'username' = '" + MYSQL_USERNAME + "',\n" +
                "  'password' = '" + MYSQL_PASSWORD + "',\n" +
                "  'driver' = '" + MYSQL_DRIVER + "'\n" +
                ")");

        // 执行数据同步
        tEnv.executeSql("INSERT INTO dws_trade_province_stats_mysql SELECT * FROM dws_trade_province_stats_kafka");
    }
}
