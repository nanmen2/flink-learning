package com.akun.data.sources.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author akun
 * @date 2019/11/14
 */
public class SourceFromMySQLMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new SourceFromMySQL()).print();

        env.execute("Flink add data sourc");
    }
}
