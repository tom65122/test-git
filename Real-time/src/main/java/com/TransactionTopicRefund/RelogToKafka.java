package com.TransactionTopicRefund;
import com.alibaba.fastjson.JSONObject;
import common.utils.*;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Date;
import java.util.HashMap;
/**
 * @BelongsProject: test-git
 * @BelongsPackage: com.TransactionTopicRefund
 * @Author: liwenjie
 * @CreateTime: 2025-08-28  17:28
 * @Description: TODO
 * @Version: 1.0
 */
public class RelogToKafka {
    // Kafka 配置参数
    private static final String kafka_input_topic = ConfigUtils.getString("CUSTOM.KAFKA.INPUT.TOPIC");
    private static final String kafka_bootstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_user_topic = ConfigUtils.getString("kafka.user.topic");
    private static final String kafka_order_topic = ConfigUtils.getString("kafka.order.topic");
    private static final String kafka_product_topic = ConfigUtils.getString("kafka.product.topic");
    private static final String kafka_dirty_topic = ConfigUtils.getString("kafka.dirty.topic");

    // 定义侧输出标签
    private static final OutputTag<String> userTag = new OutputTag<String>("userTag") {};
    private static final OutputTag<String> orderTag = new OutputTag<String>("orderTag") {};
    private static final OutputTag<String> productTag = new OutputTag<String>("productTag") {};
    private static final OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {};

    // 存储分流后的数据流
    private static final HashMap<String, DataStream<String>> streamMap = new HashMap<>();

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        // 打印检查配置信息
        CommonUtils.printCheckPropEnv(
                false,
                kafka_input_topic,
                kafka_bootstrap_servers,
                kafka_user_topic,
                kafka_order_topic,
                kafka_product_topic,
                kafka_dirty_topic
        );

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // 从 Kafka 读取数据
        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_bootstrap_servers,
                        kafka_input_topic,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "read_kafka_custom_log"
        );

        // 解析 JSON 数据并处理脏数据
        SingleOutputStreamOperator<JSONObject> processDS = kafkaSourceDs.process(
                        new ProcessFunction<String, JSONObject>() {
                            @Override
                            public void processElement(String value, Context ctx, Collector<JSONObject> out) {
                                try {
                                    JSONObject jsonObject = JSONObject.parseObject(value);
                                    out.collect(jsonObject);
                                } catch (Exception e) {
                                    // 将无法解析的数据发送到脏数据主题
                                    ctx.output(dirtyTag, value);
                                    System.err.println("Convert JsonData Error: " + e.getMessage());
                                }
                            }
                        })
                .uid("convert_json_process")
                .name("convert_json_process");

        // 获取脏数据流并发送到 Kafka
        SideOutputDataStream<String> dirtyDS = processDS.getSideOutput(dirtyTag);
        dirtyDS.sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_servers, kafka_dirty_topic))
                .uid("sink_dirty_data_to_kafka")
                .name("sink_dirty_data_to_kafka");

        // 根据日志类型进行分流
        SingleOutputStreamOperator<String> splitStream = processDS.process(
                        new ProcessFunction<JSONObject, String>() {
                            @Override
                            public void processElement(JSONObject jsonObject, Context ctx, Collector<String> out) {
                                try {
                                    String type = jsonObject.getString("type");
                                    String jsonData = jsonObject.toJSONString();

                                    if ("user".equals(type)) {
                                        ctx.output(userTag, jsonData);
                                    } else if ("order".equals(type)) {
                                        ctx.output(orderTag, jsonData);
                                    } else if ("product".equals(type)) {
                                        ctx.output(productTag, jsonData);
                                    } else {
                                        // 未知类型数据也发送到脏数据主题
                                        ctx.output(dirtyTag, jsonData);
                                    }

                                    // 主流也输出一份完整数据
                                    out.collect(jsonData);
                                } catch (Exception e) {
                                    // 处理异常，将原始数据发送到脏数据主题
                                    ctx.output(dirtyTag, jsonObject.toJSONString());
                                    System.err.println("Process data error: " + e.getMessage());
                                }
                            }
                        })
                .uid("split_stream_process")
                .name("split_stream_process");

        // 获取各个分流后的数据流
        SideOutputDataStream<String> userStream = splitStream.getSideOutput(userTag);
        SideOutputDataStream<String> orderStream = splitStream.getSideOutput(orderTag);
        SideOutputDataStream<String> productStream = splitStream.getSideOutput(productTag);
        SideOutputDataStream<String> dirtyStream = splitStream.getSideOutput(dirtyTag);

        // 将分流后的数据流存储到 Map 中
        streamMap.put("user", userStream);
        streamMap.put("order", orderStream);
        streamMap.put("product", productStream);
        streamMap.put("dirty", dirtyStream);
        streamMap.put("all", splitStream); // 主流包含所有数据

        // 将分流后的数据发送到对应的 Kafka 主题
        sendDataToKafka(streamMap);

        // 执行任务
        env.execute("Job-CustomLogDataProcess2Kafka");
    }

    /**
     * 将分流后的数据发送到对应的 Kafka 主题
     * @param dataStreamMap 包含各数据流的 Map
     */
    public static void sendDataToKafka(HashMap<String, DataStream<String>> dataStreamMap) {
        // 用户数据流发送到用户主题
        dataStreamMap.get("user")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_servers, kafka_user_topic))
                .uid("sink_user_data_to_kafka")
                .name("sink_user_data_to_kafka");

        // 订单数据流发送到订单主题
        dataStreamMap.get("order")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_servers, kafka_order_topic))
                .uid("sink_order_data_to_kafka")
                .name("sink_order_data_to_kafka");

        // 产品数据流发送到产品主题
        dataStreamMap.get("product")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_servers, kafka_product_topic))
                .uid("sink_product_data_to_kafka")
                .name("sink_product_data_to_kafka");

        // 脏数据流发送到脏数据主题
        dataStreamMap.get("dirty")
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_servers, kafka_dirty_topic))
                .uid("sink_dirty_data_to_kafka_v2")
                .name("sink_dirty_data_to_kafka_v2");

        // 打印各数据流信息（用于调试）
        dataStreamMap.get("user").print("user ->");
        dataStreamMap.get("order").print("order ->");
        dataStreamMap.get("product").print("product ->");
        dataStreamMap.get("dirty").print("dirty ->");
        dataStreamMap.get("all").print("all ->");
    }
}
