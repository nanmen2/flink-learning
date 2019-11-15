package com.akun.data.sources.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 利用 flink kafka 自带的 source 读取 kafka 里面的数据
 *
 * @author akun
 */
public class KafkaReadMain {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "47.98.182.82:9092");
        props.put("zookeeper.connect", "47.98.182.82:2181");
        props.put("group.id", "metric-group");
        //key 反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //value 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                //kafka topic
                "metric",
                // String 序列化
                new SimpleStringSchema(),
                props);
        DataStreamSource<String> dataStreamSource;
        dataStreamSource = env.addSource(consumer);

        //把从 kafka 读取到的数据打印在控制台
        dataStreamSource.print();

        env.execute("Flink add data source");
    }

}
