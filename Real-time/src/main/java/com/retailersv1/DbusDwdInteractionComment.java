package com.retailersv1;

import com.alibaba.fastjson.JSON;
import com.retailersv1.func.ProcessSplitStreamToKafka;
import common.utils.ConfigUtils;
import common.utils.EnvironmentSettingUtils;
import common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
/**
 * @BelongsProject: test-git
 * @BelongsPackage: com.retailersv1
 * @Author: liwenjie
 * @CreateTime: 2025-08-19  20:24
 * @Description: TODO
 * @Version: 1.0
 */
public class DbusDwdInteractionComment {
    private static final String CDH_KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String DWD_COMMENT_TOPIC = "dwd_interaction_comment";
    private static final String SOURCE_TOPIC_DB = "topic_db";
    private static final String DICT_TOPIC = "dim_dict"; // 假设字典表变更也通过Kafka传递

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // 构建Kafka Source读取topic_db数据
        DataStreamSource<String> topicDbStream = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        CDH_KAFKA_SERVER,
                        SOURCE_TOPIC_DB,
                        "dwd_interaction_comment_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer
                                .OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "kafka_topic_db_source"
        );

        // 解析JSON数据
        SingleOutputStreamOperator<JSONObject> jsonParseStream = topicDbStream
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            out.collect(jsonObject);
                        } catch (Exception e) {
                            // 错误数据输出到侧输出流
                            ctx.output(new OutputTag<String>("dirty-data") {}, value);
                        }
                    }
                })
                .uid("parse_json_data")
                .name("parse_json_data");

        // 构建字典表变更数据源（如果字典表变更也通过Kafka传递）
        DataStreamSource<String> dictStream = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        CDH_KAFKA_SERVER,
                        DICT_TOPIC,
                        "dwd_dict_comment_group",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "kafka_dict_source"
        );

        // 解析字典表变更数据
        SingleOutputStreamOperator<JSONObject> dictJsonStream = dictStream
                .map(JSON::parseObject)
                .uid("parse_dict_json")
                .name("parse_dict_json");

        // 创建广播状态描述符
        MapStateDescriptor<String, JSONObject> mapStateDesc =
                new MapStateDescriptor<>("dict-map", String.class, JSONObject.class);

        // 将字典流广播
        BroadcastStream<JSONObject> broadcastDictStream = dictJsonStream.broadcast(mapStateDesc);

        // 连接主数据流和广播字典流
        BroadcastConnectedStream<JSONObject, JSONObject> connectedStream =
                jsonParseStream.connect(broadcastDictStream);

        // 处理连接后的流
        SingleOutputStreamOperator<String> resultStream = connectedStream
                .process(new ProcessSplitStreamToKafka(mapStateDesc, CDH_ZOOKEEPER_SERVER))
                .uid("process_comment_with_dict")
                .name("process_comment_with_dict");




        // 将结果写入Kafka
        resultStream
                .sinkTo(KafkaUtils.buildKafkaSink(CDH_KAFKA_SERVER, DWD_COMMENT_TOPIC))
                .uid("dwd_comment_to_kafka")
                .name("dwd_comment_to_kafka");

        // 打印中间结果用于调试
        jsonParseStream.print("json_parse -> ");
        resultStream.print("result_data -> ");

        env.disableOperatorChaining();
        env.execute("DbusDwdInteractionComment");
    }
}
