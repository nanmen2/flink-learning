package com.akun.streaming.examples.socket;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Main {
    public static void main(String[] args) throws Exception {
        //1、参数检查，需要传入两个参数（hostname 和 port），符合条件就赋值给 hostname 和 port
        if (args.length != 2) {
            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }
        String hostname = args[0];
        int port = Integer.parseInt(args[1]);
        //2、创建好 StreamExecutionEnvironment（流程序的运行环境）
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //3、构建数据源，获取 Socket 数据
        DataStreamSource<String> stream = env.socketTextStream(hostname, port);
        //4、对 Socket 数据字符串分隔后收集在根据 word 分组后计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new LineSplitter())
                .keyBy(0)
                .sum(1);
        //5、打印所有的数据流，格式是 (word，count)，count 代表 word 出现的次数
        sum.print();
        //6、开始执行 Job
        env.execute("Java WordCount from SocketText");
    }

    //将字符串进行分隔然后收集，组装后的数据格式是 (word、1)，1 代表 word 出现的次数为 1
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            String[] tokens = s.toLowerCase().split("\\W+");

            for (String token : tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
