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
 * @BelongsProject: test-git
 * @BelongsPackage: com.retailersv1.dws
 * @Author: liwenjie
 * @CreateTime: 2025-08-22  15:40
 * @Description: TODO
 * @Version: 1.0
 */
public class DwsTradeUserGoodsStats {
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String DWD_TRADE_ORDER_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.detail");
    private static final String DWD_CART_INFO = ConfigUtils.getString("kafka.dwd.cart.info");
    private static final String DWD_COMMENT_INFO = ConfigUtils.getString("kafka.dwd.comment.info");
    private static final String DWS_USER_GOODS_STATS = ConfigUtils.getString("kafka.dws.user.goods.stats");

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
                ")" + SqlUtil.getKafka(DWD_TRADE_ORDER_DETAIL, "dws_group"));

        // 创建购物车信息源表
        tEnv.executeSql("CREATE TABLE dwd_trade_cart_info (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  sku_num INT,\n" +
                "  ts_ms BIGINT\n" +
                ")" + SqlUtil.getKafka(DWD_CART_INFO, "dws_group"));

        // 创建评论信息源表
        tEnv.executeSql("CREATE TABLE dwd_interaction_comment_info (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  appraise STRING,\n" +
                "  appraise_name STRING,\n" +
                "  comment_txt STRING,\n" +
                "  ts_ms BIGINT\n" +
                ")" + SqlUtil.getKafka(DWD_COMMENT_INFO, "dws_group"));

        // 创建用户维度表
        tEnv.executeSql("CREATE TABLE dim_user_info (\n" +
                "  id STRING,\n" +
                "  login_name STRING,\n" +
                "  nick_name STRING,\n" +
                "  passwd STRING,\n" +
                "  name STRING,\n" +
                "  phone_num STRING,\n" +
                "  email STRING,\n" +
                "  head_img STRING,\n" +
                "  user_level STRING,\n" +
                "  birthday STRING,\n" +
                "  gender STRING,\n" +
                "  create_time STRING,\n" +
                "  operate_time STRING,\n" +
                "  status STRING,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SqlUtil.getHbaseDDL("dim_user_info"));

        // 创建商品维度表
        tEnv.executeSql("CREATE TABLE dim_sku_info (\n" +
                "  id STRING,\n" +
                "  spu_id STRING,\n" +
                "  price DECIMAL(16, 2),\n" +
                "  sku_name STRING,\n" +
                "  sku_desc STRING,\n" +
                "  weight DECIMAL(16, 2),\n" +
                "  tm_id STRING,\n" +
                "  category3_id STRING,\n" +
                "  category2_id STRING,\n" +
                "  category1_id STRING,\n" +
                "  sku_default_img STRING,\n" +
                "  is_sale STRING,\n" +
                "  create_time STRING,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SqlUtil.getHbaseDDL("dim_sku_info"));

        // 订单统计（按用户和商品）
        Table orderStats = tEnv.sqlQuery(
                "SELECT " +
                        "user_id, " +
                        "sku_id, " +
                        "DATE_FORMAT(FROM_UNIXTIME(CAST(create_time AS BIGINT)/1000), 'yyyy-MM-dd') AS date_id, " +
                        "COUNT(DISTINCT order_id) AS order_count, " +
                        "SUM(CAST(sku_num AS BIGINT)) AS order_num, " +
                        "SUM(CAST(split_original_amount AS DECIMAL(16,2))) AS order_original_amount, " +
                        "SUM(CAST(split_total_amount AS DECIMAL(16,2))) AS order_total_amount, " +
                        "SUM(CAST(split_activity_amount AS DECIMAL(16,2))) AS order_activity_amount, " +
                        "SUM(CAST(split_coupon_amount AS DECIMAL(16,2))) AS order_coupon_amount " +
                        "FROM dwd_trade_order_detail " +
                        "GROUP BY user_id, sku_id, DATE_FORMAT(FROM_UNIXTIME(CAST(create_time AS BIGINT)/1000), 'yyyy-MM-dd')"
        );

        // 购物车统计（按用户和商品）
        Table cartStats = tEnv.sqlQuery(
                "SELECT " +
                        "user_id, " +
                        "sku_id, " +
                        "COUNT(*) AS cart_add_count, " +
                        "SUM(sku_num) AS cart_add_num " +
                        "FROM dwd_trade_cart_info " +
                        "GROUP BY user_id, sku_id"
        );

        // 评论统计（按用户和商品）
        Table commentStats = tEnv.sqlQuery(
                "SELECT " +
                        "user_id, " +
                        "sku_id, " +
                        "COUNT(*) AS comment_count, " +
                        "COUNT(CASE WHEN appraise = '1201' THEN 1 END) AS good_comment_count " +
                        "FROM dwd_interaction_comment_info " +
                        "GROUP BY user_id, sku_id"
        );

        // 注册临时视图
        tEnv.createTemporaryView("order_stats", orderStats);
        tEnv.createTemporaryView("cart_stats", cartStats);
        tEnv.createTemporaryView("comment_stats", commentStats);

        // 关联所有统计信息
        Table result = tEnv.sqlQuery(
                "SELECT " +
                        "os.user_id, " +
                        "os.sku_id, " +
                        "os.date_id, " +
                        // 订单相关指标
                        "os.order_count, " +
                        "os.order_num, " +
                        "os.order_original_amount, " +
                        "os.order_total_amount, " +
                        "os.order_activity_amount, " +
                        "os.order_coupon_amount, " +
                        // 购物车相关指标
                        "cs.cart_add_count, " +
                        "cs.cart_add_num, " +
                        // 评论相关指标
                        "cts.comment_count, " +
                        "cts.good_comment_count " +
                        "FROM order_stats os " +
                        "LEFT JOIN cart_stats cs ON os.user_id = cs.user_id AND os.sku_id = cs.sku_id " +
                        "LEFT JOIN comment_stats cts ON os.user_id = cts.user_id AND os.sku_id = cts.sku_id"
        );

//        result.execute().print();
        // 创建目标表并写入数据
        tEnv.executeSql("CREATE TABLE " + DWS_USER_GOODS_STATS + " (\n" +
                "user_id STRING,\n" +
                "sku_id STRING,\n" +
                "date_id STRING,\n" +
                "order_count BIGINT,\n" +
                "order_num BIGINT,\n" +
                "order_original_amount DECIMAL(16,2),\n" +
                "order_total_amount DECIMAL(16,2),\n" +
                "order_activity_amount DECIMAL(16,2),\n" +
                "order_coupon_amount DECIMAL(16,2),\n" +
                "cart_add_count BIGINT,\n" +
                "cart_add_num BIGINT,\n" +
                "comment_count BIGINT,\n" +
                "good_comment_count BIGINT,\n" +
                "PRIMARY KEY (user_id, sku_id, date_id) NOT ENFORCED\n" +
                ")" + SqlUtil.getUpsertKafkaDDL(DWS_USER_GOODS_STATS));

        // 关键修复：添加执行触发并等待结果
        TableResult tableResult = result.executeInsert(DWS_USER_GOODS_STATS);
        tableResult.await();
    }
}
