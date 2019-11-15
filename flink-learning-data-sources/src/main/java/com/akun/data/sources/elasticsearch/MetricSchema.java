package com.akun.data.sources.elasticsearch;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Metric Schema ，支持序列化和反序列化
 *
 * @author akun
 */
public class MetricSchema implements DeserializationSchema<MetricEvent>, SerializationSchema<MetricEvent> {


    @Override
    public MetricEvent deserialize(byte[] bytes) throws IOException {
        return JSON.parseObject(new String(bytes), MetricEvent.class);
    }

    @Override
    public boolean isEndOfStream(MetricEvent metricEvent) {
        return false;
    }

    @Override
    public byte[] serialize(MetricEvent metricEvent) {
        return JSON.toJSONBytes(metricEvent);
    }

    @Override
    public TypeInformation<MetricEvent> getProducedType() {
        return TypeInformation.of(MetricEvent.class);
    }
}
