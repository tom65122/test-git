//package common.utils;
//
//import cn.hutool.json.JSONObject;
//
///**
// * @BelongsProject: test-git
// * @BelongsPackage: common.utils
// * @Author: liwenjie
// * @CreateTime: 2025-08-22  10:08
// * @Description: TODO
// * @Version: 1.0
// */
//public class KafkaSink {
//    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>>  getKafkaSinkDwd(){
//        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> kafkaSink = KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
//                .setBootstrapServers(ConfigUtils.getString("kafka.bootstrap.servers"))
//                .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
//                    @Nullable
//                    @Override
//                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> tup2, KafkaSinkContext context, Long timestamp) {
//                        JSONObject jsonObj = tup2.f0;
//                        TableProcessDwd tp = tup2.f1;
//                        String topic = tp.getSinkTable();
//                        return new ProducerRecord<>(topic, jsonObj.toJSONString().getBytes());
//                    }
//
//                })
//                .build();
//        return kafkaSink;
//    }
//}
