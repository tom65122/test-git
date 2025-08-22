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
 * 用户级别互动统计DWS层
 */
public class DwsInteractionUserStats {
    private static final String DWD_CART_INFO = ConfigUtils.getString("kafka.dwd.cart.info");
    private static final String DWD_COMMENT_INFO = ConfigUtils.getString("kafka.dwd.comment.info");
    private static final String DWS_INTERACTION_USER_STATS = ConfigUtils.getString("kafka.dws.interaction.user.stats");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setStateBackend(new MemoryStateBackend());
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 创建购物车信息源表
        tEnv.executeSql("CREATE TABLE dwd_trade_cart_info (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  sku_num INT,\n" +
                "  ts_ms BIGINT\n" +
                ")" + SqlUtil.getKafka(DWD_CART_INFO, "dws_interaction_group"));

        // 创建评论信息源表
        tEnv.executeSql("CREATE TABLE dwd_interaction_comment_info (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  appraise STRING,\n" +
                "  appraise_name STRING,\n" +
                "  comment_txt STRING,\n" +
                "  ts_ms BIGINT\n" +
                ")" + SqlUtil.getKafka(DWD_COMMENT_INFO, "dws_interaction_group"));

        // 购物车统计（按用户）
        Table cartStats = tEnv.sqlQuery(
                "SELECT " +
                        "user_id, " +
                        "DATE_FORMAT(FROM_UNIXTIME(CAST(ts_ms AS BIGINT)/1000), 'yyyy-MM-dd') AS date_id, " +
                        "COUNT(*) AS cart_add_count, " +
                        "SUM(sku_num) AS cart_add_num " +
                        "FROM dwd_trade_cart_info " +
                        "GROUP BY user_id, DATE_FORMAT(FROM_UNIXTIME(CAST(ts_ms AS BIGINT)/1000), 'yyyy-MM-dd')"
        );

        // 评论统计（按用户）
        Table commentStats = tEnv.sqlQuery(
                "SELECT " +
                        "user_id, " +
                        "DATE_FORMAT(FROM_UNIXTIME(CAST(ts_ms AS BIGINT)/1000), 'yyyy-MM-dd') AS date_id, " +
                        "COUNT(*) AS comment_count, " +
                        "COUNT(CASE WHEN appraise = '1201' THEN 1 END) AS good_comment_count " +
                        "FROM dwd_interaction_comment_info " +
                        "GROUP BY user_id, DATE_FORMAT(FROM_UNIXTIME(CAST(ts_ms AS BIGINT)/1000), 'yyyy-MM-dd')"
        );

        // 注册临时视图
        tEnv.createTemporaryView("cart_stats", cartStats);
        tEnv.createTemporaryView("comment_stats", commentStats);

        // 关联所有统计信息
        Table result = tEnv.sqlQuery(
                "SELECT " +
                        "COALESCE(cs.user_id, cts.user_id) AS user_id, " +
                        "COALESCE(cs.date_id, cts.date_id) AS date_id, " +
                        // 购物车相关指标
                        "COALESCE(cs.cart_add_count, 0) AS cart_add_count, " +
                        "COALESCE(cs.cart_add_num, 0) AS cart_add_num, " +
                        // 评论相关指标
                        "COALESCE(cts.comment_count, 0) AS comment_count, " +
                        "COALESCE(cts.good_comment_count, 0) AS good_comment_count " +
                        "FROM cart_stats cs " +
                        "FULL OUTER JOIN comment_stats cts ON cs.user_id = cts.user_id AND cs.date_id = cts.date_id"
        );
//        result.execute().print();

        // 创建目标表并写入数据
        tEnv.executeSql("CREATE TABLE " + DWS_INTERACTION_USER_STATS + " (\n" +
                "user_id STRING,\n" +
                "date_id STRING,\n" +
                "cart_add_count BIGINT,\n" +
                "cart_add_num BIGINT,\n" +
                "comment_count BIGINT,\n" +
                "good_comment_count BIGINT,\n" +
                "PRIMARY KEY (user_id, date_id) NOT ENFORCED\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(DWS_INTERACTION_USER_STATS));

        // 执行插入操作并等待完成
        TableResult tableResult = result.executeInsert(DWS_INTERACTION_USER_STATS);
        tableResult.await();
    }
}
