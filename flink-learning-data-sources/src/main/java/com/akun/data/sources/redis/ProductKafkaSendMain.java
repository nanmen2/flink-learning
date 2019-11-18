package com.akun.data.sources.redis;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * 不断的模拟商品数据发往 Kafka
 *
 * @author akun
 * @date 2019/11/18
 */
public class ProductKafkaSendMain {

    private static final String BROKER_LIST = "47.98.182.82:9092";
    /**
     * kafka topic 需要和 flink 程序用同一个 topic
     */
    private static final String TOPIC = "akun";

    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 1; i <= 10000; i++) {
            //商品的 id
            ProductEvent product = ProductEvent.builder().id((long) i)
                    //商品 name
                    .name("product" + i)
                    //商品价格（以分为单位）
                    .price(RANDOM.nextLong() / 10000000000000L)
                    //商品编码
                    .code("code" + i).build();

            ProducerRecord record = new ProducerRecord<String, String>(TOPIC, null, null, JSON.toJSONString(product));
            producer.send(record);
            System.out.println("发送数据-kafka: " + JSON.toJSONString(product));
        }
        producer.flush();
    }
}
