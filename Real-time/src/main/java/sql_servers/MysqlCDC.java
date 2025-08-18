package sql_servers;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * @BelongsProject: test-git
 * @BelongsPackage: sql_servers
 * @Author: liwenjie
 * @CreateTime: 2025-08-12  09:01
 * @Description: TODO
 * @Version: 1.0
 */
public class MysqlCDC {

        public static void main(String[] args) throws Exception {

            // 1. 定义 MySQL CDC Source
            MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                    .hostname("cdh03")
                    .port(3306)
                    .databaseList("test_db") // 要同步的库
                    .tableList("test_db.user_info") // 要同步的表
                    .username("root")
                    .password("root")
                    .startupOptions(StartupOptions.initial()) // initial=全量+增量，latest=从最新binlog开始
                    .deserializer(new JsonDebeziumDeserializationSchema()) // 输出 4JSON 格式
                    .build();

            // 2. 创建 Flink 执行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);


            // 3. 添加 MySQL CDC Source
            DataStreamSource<String> streamSource = env.fromSource(
                    mySqlSource,
                    WatermarkStrategy.noWatermarks(),
                    "MySQL-CDC-Source"
            );
            // 4. 输出数据到控制台
            streamSource.print();


            // 5. 执行任务
            env.execute("MySQL CDC Flink Job");
        }
    }

