package com.akun.data.sources.kafka2mysql;

import com.akun.data.sources.mysql.Student;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.stream.Stream;

/**
 * 往kafka中写数据
 *
 * @author akun
 * @date 2019/11/14
 */
public class ToKafka {

    private static final String BROKER_LIST = "47.98.182.82:9092";
    /**
     * Kafka topic，Flink 统一
     */
    private static final String TOPIC = "student";


    private static void writeToKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        //key 序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //value 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        Stream.iterate(1, n -> n += 1).limit(100).forEach(
                num -> {
                    Student student = Student.builder().id(num).name("akun" + num).age(num).password("password" + num).build();
                    ProducerRecord record = new ProducerRecord<String, String>(TOPIC, null, null, JSON.toJSONString(student));
                    producer.send(record);
                    System.out.println("发送数据: " + JSON.toJSONString(student));
                }
        );
        producer.flush();
    }

    public static void main(String[] args) {
        writeToKafka();
    }
}
