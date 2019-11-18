package com.akun.data.sources.redis;

import com.akun.data.sources.elasticsearch.KafkaConfigUtil;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author akun
 * @date 2019/11/18
 */
public class SinkRedisMain {

    /**
     * kafka topic 需要和 flink 程序用同一个 topic
     */
    private static final String TOPIC = "akun";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = KafkaConfigUtil.buildKafkaProps();

        SingleOutputStreamOperator<Tuple2<String, String>> product = env.addSource(new FlinkKafkaConsumer<>(
                TOPIC,
                new SimpleStringSchema(),
                props))
                //反序列化 JSON
                .map(string -> JSON.parseObject(string, ProductEvent.class))
                .flatMap(new FlatMapFunction<ProductEvent, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(ProductEvent value, Collector<Tuple2<String, String>> out) throws Exception {
                        //收集商品 id 和 price 两个属性
                        out.collect(new Tuple2<>(value.getId().toString(), value.getPrice().toString()));
                    }
                });
//        product.print();

        //单个 Redis
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("47.98.182.82").build();
        product.addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisSinkMapper()));

        //Redis 的 ip 信息一般都从配置文件取出来
        //Redis 集群
/*        FlinkJedisClusterConfig clusterConfig = new FlinkJedisClusterConfig.Builder()
                .setNodes(new HashSet<InetSocketAddress>(
                        Arrays.asList(new InetSocketAddress("redis1", 6379)))).build();*/

        //Redis Sentinels
/*        FlinkJedisSentinelConfig sentinelConfig = new FlinkJedisSentinelConfig.Builder()
                .setMasterName("master")
                .setSentinels(new HashSet<>(Arrays.asList("sentinel1", "sentinel2")))
                .setPassword("")
                .setDatabase(1).build();*/

        env.execute("flink redis connector");
    }

    public static class RedisSinkMapper implements RedisMapper<Tuple2<String, String>> {
        /**
         * 设置使用 Redis 的数据结构类型，和 key 的名词，RedisCommandDescription 中有两个属性 RedisCommand、key
         *
         * @return
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "akun");
        }

        //获取 key 值
        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        //获取 value 值
        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }

}
