package com.akun.libraries.cep;

import com.google.common.base.Strings;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author akun
 * @date 2019/11/20
 */
public class CepMain {

    private static final Logger LOG = LoggerFactory.getLogger(CepMain.class);

    public static void main(String[] args) {
        //1、创建流程序的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> eventDataStream = env.socketTextStream("47.98.182.82", 9200)
                .flatMap(new FlatMapFunction<String, Event>() {
                    @Override
                    public void flatMap(String s, Collector<Event> collector) throws Exception {
                        if (!Strings.isNullOrEmpty(s)) {
                            String[] split = s.split(",");
                            if (split.length == 2) {
                                collector.collect(new Event(Integer.valueOf(split[0]), split[1]));
                            }
                        }
                    }
                });

        Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        LOG.info("start {}", event.getId());
                        return event.getId() == 42;
                    }
                }
        ).next("middle").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        LOG.info("middle {}", event.getId());
                        return event.getId() >= 10;
                    }
                }
        );

        CEP.pattern(eventDataStream, pattern).select(new PatternSelectFunction<Event, String>() {
            @Override
            public String select(Map<String, List<Event>> p) throws Exception {
                StringBuilder builder = new StringBuilder();
                LOG.info("p = {}", p);
                builder.append(p.get("start").get(0).getId()).append(",").append(p.get("start").get(0).getName()).append("\n")
                        .append(p.get("middle").get(0).getId()).append(",").append(p.get("middle").get(0).getName());
                return builder.toString();
            }
        }).print();//打印结果
    }
}
