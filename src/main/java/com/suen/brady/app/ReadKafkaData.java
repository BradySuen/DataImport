package com.suen.brady.app;

import com.alibaba.fastjson.JSONObject;
import com.suen.brady.sink.DorisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @author BradySuen
 * @create_time 2021/9/14
 * @description
 *
 * 该Demo 应用于读取KAFKA数据并写入到Doris中
 *
 *
 *
 **/
public class ReadKafkaData {
    public static void main(String[] args) throws IOException {
        //创建流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(8);
        env.enableCheckpointing(5000);
        env.setStateBackend(new MemoryStateBackend());

        //从 kafka 中读取数据
        Properties kafkaConsumerProp = new Properties();
        kafkaConsumerProp.setProperty("bootstrap.servers","doris.nana666.com:9092");
        kafkaConsumerProp.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProp.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProp.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource =
                env.addSource(new FlinkKafkaConsumer<String>("installs_primary_ios", new SimpleStringSchema(), kafkaConsumerProp));

        //dataStreamSource.print();


        SingleOutputStreamOperator<JSONObject> data = dataStreamSource.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                String partitionDate = sdf.format(
                        new Date(Long.parseLong(jsonObject.get("date_received").toString().substring(0, 10)) * 1000L));
                jsonObject.put("partition_date", partitionDate);
                return jsonObject;
            }
        });


        KeyedStream<JSONObject, String> result = data.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.get("partition_date").toString();
            }
        });

        //配置Doris属性
        Properties dorisProp = new Properties();
        dorisProp.load(new FileInputStream("/Volumes/Brady/code/DataCollect/src/main/resources/text.properties"));
        String databaseName = dorisProp.getProperty("database-name");
        String tableName = dorisProp.getProperty("table-name");
        JSONObject keyCols = JSONObject.parseObject(dorisProp.getProperty("keyCols"));
        String partitionKey = dorisProp.getProperty("partitionKey");
        String distributeKey = dorisProp.getProperty("distributeKey");
        JSONObject valueCols = JSONObject.parseObject(dorisProp.getProperty("valueCols"));

        result.addSink(new DorisSink(databaseName,tableName,keyCols,partitionKey,distributeKey,valueCols)).setParallelism(8);
        //启动任务
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}




//        try{
//                SingleOutputStreamOperator<JSONObject> result = dataStreamSource.map(JSONObject::parseObject);
//
//        result.addSink(JdbcSink.sink(
//        s,
//        (preparedStatement, s) -> {
//        int i =0;
//        for (String key : s.keySet()) {
//        i++;
//        preparedStatement.setString(i,s.getString(key));
//        }
//        },
//        JdbcExecutionOptions.builder()
//        .withBatchSize(1000)
//        .withBatchIntervalMs(200)
//        .withMaxRetries(5)
//        .build(),
//        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//        .withUrl("jdbc:mysql://doris.nana666.com:9030/test?useUnicode=true&amp&characterEncoding=utf-8")
//        .withDriverName("com.mysql.jdbc.Driver")
//        .withUsername("root")
//        .withPassword("upup666")
//        .build()
//
//        ));
//        }catch(Exception e){
//        //创建数据表
//        //dataStreamSource
//        result -> {
//        result.
//        // DorisClient.createTable(kafkaTopic,);
//        }
//        }

