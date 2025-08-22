package com.retailersv1.dws;

import common.utils.ConfigUtils;
import common.utils.EnvironmentSettingUtils;
import common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 省份级别交易统计DWS层
 */
public class DwsTradeProvinceStats {
    private static final String DWD_TRADE_ORDER_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.detail");
    private static final String DWS_TRADE_PROVINCE_STATS = ConfigUtils.getString("kafka.dws.trade.province.stats");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setStateBackend(new MemoryStateBackend());
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 创建订单明细源表
        tEnv.executeSql("CREATE TABLE dwd_trade_order_detail (\n" +
                "  id STRING,\n" +
                "  order_id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  sku_name STRING,\n" +
                "  province_id STRING,\n" +
                "  activity_id STRING,\n" +
                "  activity_rule_id STRING,\n" +
                "  coupon_id STRING,\n" +
                "  date_id STRING,\n" +
                "  create_time STRING,\n" +
                "  sku_num STRING,\n" +
                "  split_original_amount STRING,\n" +
                "  split_activity_amount STRING,\n" +
                "  split_coupon_amount STRING,\n" +
                "  split_total_amount STRING,\n" +
                "  ts_ms BIGINT\n" +
                ")" + SqlUtil.getKafka(DWD_TRADE_ORDER_DETAIL, "dws_province_group"));

        // 省份维度统计
        Table provinceStats = tEnv.sqlQuery(
                "SELECT " +
                        "province_id, " +
                        "date_id, " +
                        "COUNT(DISTINCT order_id) AS order_count, " +
                        "SUM(CAST(sku_num AS BIGINT)) AS order_num, " +
                        "SUM(CAST(split_original_amount AS DECIMAL(16,2))) AS order_original_amount, " +
                        "SUM(CAST(split_total_amount AS DECIMAL(16,2))) AS order_total_amount, " +
                        "SUM(CAST(split_activity_amount AS DECIMAL(16,2))) AS order_activity_amount, " +
                        "SUM(CAST(split_coupon_amount AS DECIMAL(16,2))) AS order_coupon_amount " +
                        "FROM dwd_trade_order_detail " +
                        "GROUP BY province_id, date_id"
        );

//        provinceStats.execute().print();

        // 创建目标表并写入数据
        tEnv.executeSql("CREATE TABLE " + DWS_TRADE_PROVINCE_STATS + " (\n" +
                "province_id STRING,\n" +
                "date_id STRING,\n" +
                "order_count BIGINT,\n" +
                "order_num BIGINT,\n" +
                "order_original_amount DECIMAL(16,2),\n" +
                "order_total_amount DECIMAL(16,2),\n" +
                "order_activity_amount DECIMAL(16,2),\n" +
                "order_coupon_amount DECIMAL(16,2),\n" +
                "PRIMARY KEY (province_id, date_id) NOT ENFORCED\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(DWS_TRADE_PROVINCE_STATS));

        // 执行插入操作并等待完成
        TableResult tableResult = provinceStats.executeInsert(DWS_TRADE_PROVINCE_STATS);
        tableResult.await();
    }
}
