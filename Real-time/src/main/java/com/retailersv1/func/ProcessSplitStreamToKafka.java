package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import common.utils.HbaseUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * @BelongsProject: test-git
 * @BelongsPackage: com.retailersv1.func
 * @Author: liwenjie
 * @CreateTime: 2025-08-19  20:21
 * @Description: TODO
 * @Version: 1.0
 */
public class ProcessSplitStreamToKafka extends BroadcastProcessFunction<JSONObject, JSONObject, String> {
    private MapStateDescriptor<String, JSONObject> mapStateDescriptor;
    private org.apache.hadoop.hbase.client.Connection hbaseConnection;
    private HbaseUtils hbaseUtils;
    private final String zookeeperServer;

    // 缓存字典数据
    private Map<String, String> dictCache = new HashMap<>();

    /**
     * 构造函数 - 接收广播状态描述符和ZooKeeper服务器地址
     */
    public ProcessSplitStreamToKafka(MapStateDescriptor<String, JSONObject> mapStateDescriptor, String zookeeperServer) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.zookeeperServer = zookeeperServer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化HBase连接
        hbaseUtils = new HbaseUtils(zookeeperServer);
        hbaseConnection = hbaseUtils.getConnection();

        // 初始化时加载字典数据
        loadDictData();
    }

    /**
     * 处理主数据流（评论数据）
     */
    @Override
    public void processElement(JSONObject commentData,
                               ReadOnlyContext context,
                               Collector<String> out) throws Exception {

        // 处理评论数据
        String table = commentData.getString("table");
        String type = commentData.getString("type");

        // 只处理评论表的insert和update操作
        if ("comment_info".equals(table) &&
                ("insert".equals(type) || "bootstrap-insert".equals(type))) {

            JSONObject after = commentData.getJSONObject("after");

            // 关联字典数据
            String appraise = after.getString("appraise");
            String appraiseName = dictCache.get("appraise_" + appraise);

            if (appraiseName != null) {
                after.put("appraise_name", appraiseName);
            }

            // 添加时间戳
            after.put("ts", commentData.getString("ts"));

            // 输出处理后的数据
            out.collect(after.toJSONString());
        }
    }

    /**
     * 处理广播流（字典表更新数据）
     */
    @Override
    public void processBroadcastElement(JSONObject dictData,
                                        Context context,
                                        Collector<String> out) throws Exception {

        BroadcastState<String, JSONObject> broadcastState =
                context.getBroadcastState(mapStateDescriptor);

        String op = dictData.getString("op");

        if (dictData.containsKey("after")) {
            JSONObject after = dictData.getJSONObject("after");
            String dictCode = after.getString("dict_code"); // 假设字典编码字段
            String dictName = after.getString("dict_name"); // 假设字典名称字段

            if ("d".equals(op)) {
                // 删除字典项
                dictCache.remove(dictCode);
                broadcastState.remove(dictCode);
            } else {
                // 新增或更新字典项
                dictCache.put(dictCode, dictName);
                broadcastState.put(dictCode, dictData);
            }
        }
    }

    /**
     * 从HBase加载字典数据到缓存
     */
    private void loadDictData() throws Exception {
        Table table = hbaseConnection.getTable(TableName.valueOf("dim_dict"));

        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setCacheBlocks(false);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            String rowKey = Bytes.toString(result.getRow());
            String dictName = Bytes.toString(
                    result.getValue(Bytes.toBytes("info"), Bytes.toBytes("dict_name"))
            );
            dictCache.put(rowKey, dictName);
        }

        scanner.close();
        table.close();
    }

    @Override
    public void close() throws Exception {
        if (hbaseConnection != null) {
            hbaseConnection.close();
        }
    }
}
