package com.TransactionTopicRefund;

import common.utils.EnvironmentSettingUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @BelongsProject: test-git
 * @BelongsPackage: com.TransactionTopicRefund
 * @Author: liwenjie
 * @CreateTime: 2025-08-26  18:54
 * @Description: TODO
 * @Version: 1.0
 */
public class ODS {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //1.1 订单原始表(ods_order)- 对应文档数据源“订单数据”

        tEnv.executeSql("CREATE TABLE ods_order (\n" +
                "    order_id STRING COMMENT '订单唯一标识',\n" +
                "    user_id STRING COMMENT '用户ID',\n" +
                "    sku_id STRING COMMENT '商品ID',\n" +
                "    order_amount DECIMAL(18,2) COMMENT '订单金额',\n" +
                "    pay_time TIMESTAMP COMMENT '支付时间(文档3.1核心时间口径)',\n" +
                "    commit_ship_time TIMESTAMP COMMENT '承诺发货时间(文档3.3退前预警字段)',\n" +
                "    order_status STRING COMMENT '订单状态(待支付/已支付/已发货)',\n" +
                "    create_time TIMESTAMP COMMENT '订单创建时间',\n" +
                "    data_source STRING COMMENT '数据来源(固定：order_system)',\n" +
                "    sync_time TIMESTAMP COMMENT '同步至ODS层时间',\n" +
                "    `dt` STRING COMMENT '分区字段(按同步时间天分桶)',\n" +  // 1. 先声明字段和注释
                "    PRIMARY KEY (order_id) NOT ENFORCED\n" +
                ") PARTITIONED BY (`dt`)\n" +
                "WITH (\n" +
                "    'connector' = 'paimon',\n" +
                "    'path' = 'hdfs:///refund_data/ods/ods_order', \n" +
                "    'file.format' = 'parquet',\n" +
                "    'sink.parallelism' = '1',\n" +
                "    'computed-column.expression.dt' = 'DATE_FORMAT(sync_time, ''yyyy-MM-dd'')'  // 2. 单独配置计算列表达式\n" +
                ");");

        // 1.2 退款原始表(ods_refund) - 修复计算列语法
        tEnv.executeSql("CREATE TABLE ods_refund (\n" +
                "    refund_id STRING COMMENT '退款唯一标识(主键)',\n" +
                "    order_id STRING COMMENT '关联订单ID(外键)',\n" +
                "    user_id STRING COMMENT '用户ID',\n" +
                "    refund_amount DECIMAL(18,2) COMMENT '退款金额(文档4.核心指标)',\n" +
                "    refund_status STRING COMMENT '退款状态(待处理/待寄件/已完成)',\n" +
                "    refund_apply_time TIMESTAMP COMMENT '退款申请时间',\n" +
                "    user_input_reason STRING COMMENT '用户填写退款原因(文档4.原因识别)',\n" +
                "    refund_finish_time TIMESTAMP COMMENT '退款完结时间(文档3.1可扩展口径)',\n" +
                "    data_source STRING COMMENT '固定：refund_system',\n" +
                "    sync_time TIMESTAMP COMMENT '同步时间',\n" +
                "    `dt` STRING COMMENT '分区字段',\n" +  // 先声明字段
                "    PRIMARY KEY (refund_id) NOT ENFORCED\n" +
                ") PARTITIONED BY (`dt`)\n" +
                "WITH (\n" +
                "    'connector' = 'paimon',\n" +
                "    'path' = 'hdfs:///refund_data/ods/ods_refund',\n" +
                "    'file.format' = 'parquet',\n" +
                "    'computed-column.expression.dt' = 'DATE_FORMAT(sync_time, ''yyyy-MM-dd'')'  // 计算列配置\n" +
                ");");

        // 1.3 物流原始表(ods_logistics) - 修复计算列语法
        tEnv.executeSql("CREATE TABLE ods_logistics (\n" +
                "    logistics_id STRING COMMENT '物流单唯一标识(主键)',\n" +
                "    order_id STRING COMMENT '关联订单ID',\n" +
                "    logistics_node STRING COMMENT '物流节点',\n" +
                "    node_time TIMESTAMP COMMENT '节点时间',\n" +
                "    express_company STRING COMMENT '快递公司',\n" +
                "    logistics_desc STRING COMMENT '物流节点描述',\n" +
                "    data_source STRING COMMENT '固定logistics_system',\n" +
                "    sync_time TIMESTAMP COMMENT '同步时间',\n" +
                "    `dt` STRING COMMENT '分区字段',\n" +  // 先声明字段
                "    PRIMARY KEY (logistics_id) NOT ENFORCED\n" +
                ") PARTITIONED BY (`dt`)\n" +
                "WITH (\n" +
                "    'connector' = 'paimon',\n" +
                "    'path' = 'hdfs:///refund_data/ods/ods_logistics',\n" +
                "    'file.format' = 'parquet',\n" +
                "    'computed-column.expression.dt' = 'DATE_FORMAT(sync_time, ''yyyy-MM-dd'')'  // 计算列配置\n" +
                ");");

        // 1.4 用户行为原始表(ods_user_behavior) - 修复计算列语法
        tEnv.executeSql("CREATE TABLE ods_user_behavior (\n" +
                "    behavior_id STRING COMMENT '行为唯一标识(主键)',\n" +
                "    user_id STRING COMMENT '用户ID',\n" +
                "    behavior_type STRING COMMENT '行为类型(搜索/浏览/加购，文档5.退后追踪字段)',\n" +
                "    search_keywords ARRAY<STRING> COMMENT '搜索关键词(文档5.流失去向分析)',\n" +
                "    behavior_time TIMESTAMP COMMENT '行为发生时间',\n" +
                "    page_url STRING COMMENT '访问页面URL',\n" +
                "    data_source STRING COMMENT '固定：user_behavior_platform',\n" +
                "    sync_time TIMESTAMP COMMENT '同步时间',\n" +
                "    `dt` STRING COMMENT '分区字段',\n" +  // 先声明字段
                "    PRIMARY KEY (behavior_id) NOT ENFORCED\n" +
                ") PARTITIONED BY (`dt`)\n" +
                "WITH (\n" +
                "    'connector' = 'paimon',\n" +
                "    'path' = 'hdfs:///refund_data/ods/ods_user_behavior',\n" +
                "    'file.format' = 'parquet',\n" +
                "    'computed-column.expression.dt' = 'DATE_FORMAT(sync_time, ''yyyy-MM-dd'')'  // 计算列配置\n" +
                ");");

        // 1.5 异常数据日志表(ods_error_data) - 修复计算列语法
        tEnv.executeSql("CREATE TABLE ods_error_data (\n" +
                "    error_id STRING COMMENT '异常ID(UUID)',\n" +
                "    table_name STRING COMMENT '异常所属表(ods_order/ods_refund等)',\n" +
                "    error_field STRING COMMENT '异常字段',\n" +
                "    error_reason STRING COMMENT '异常原因(非空/格式错误)',\n" +
                "    raw_data STRING COMMENT '原始异常数据',\n" +
                "    create_time TIMESTAMP COMMENT '异常时间',\n" +
                "    `dt` STRING COMMENT '分区字段',\n" +  // 先声明字段
                "    PRIMARY KEY (error_id) NOT ENFORCED\n" +
                ") PARTITIONED BY (`dt`)\n" +
                "WITH (\n" +
                "    'connector' = 'paimon',\n" +
                "    'path' = 'hdfs:///refund_data/ods/ods_error_data',\n" +
                "    'file.format' = 'parquet',\n" +
                "    'computed-column.expression.dt' = 'DATE_FORMAT(create_time, ''yyyy-MM-dd'')'  // 计算列配置\n" +
                ");");
    }
}
