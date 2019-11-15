package com.akun.data.sources.kafka2mysql;

import com.akun.data.sources.mysql.Student;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author akun
 * @date 2019/11/15
 */
public class KafkaToMySQLMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "47.98.182.82:9092");
        props.put("zookeeper.connect", "47.98.182.82:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer<>(
                //这个 kafka topic 需要和上面的工具类的 topic 一致
                "student",
                new SimpleStringSchema(),
                props)).setParallelism(1)
                //Fastjson 解析字符串成 student 对象
                .map(string -> JSON.parseObject(string, Student.class));

        //数据 sink 到 mysql
        student.addSink(new SinkToMySQL());

        env.execute("Flink add sink");
    }
}
