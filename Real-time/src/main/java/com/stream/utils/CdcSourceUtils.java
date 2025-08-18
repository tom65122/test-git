package com.stream.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import common.utils.ConfigUtils;

import java.util.Properties;

/**
 * @BelongsProject: test-git
 * @BelongsPackage: com.stream.utils
 * @Author: liwenjie
 * @CreateTime: 2025-08-18  14:40
 * @Description: TODO
 * @Version: 1.0
 */
public class CdcSourceUtils {

    public static MySqlSource<String> getMySQLCdcSource(String database,String table,String username,String pwd,String serverId,StartupOptions model){
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("database.connectionCharset", "UTF-8");
        debeziumProperties.setProperty("decimal.handling.mode","string");
        debeziumProperties.setProperty("time.precision.mode","connect");
        debeziumProperties.setProperty("snapshot.mode", "schema_only");
        debeziumProperties.setProperty("include.schema.changes", "false");
        debeziumProperties.setProperty("database.connectionTimeZone", "Asia/Shanghai");
        return  MySqlSource.<String>builder()
                .hostname(ConfigUtils.getString("mysql.host"))
                .port(ConfigUtils.getInt("mysql.port"))
                .databaseList(database)
                .tableList(table)
                .username(username)
                .password(pwd)
                .serverId(serverId)
//                .connectionTimeZone(ConfigUtils.getString("mysql.timezone"))、
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(model)
                .includeSchemaChanges(true)
                .debeziumProperties(debeziumProperties)
                .build();
    }
}
