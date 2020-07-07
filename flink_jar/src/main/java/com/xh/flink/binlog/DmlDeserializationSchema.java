package com.xh.flink.binlog;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.xh.flink.binlog.Dml;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class DmlDeserializationSchema implements DeserializationSchema<Dml> {

    @Override
    public Dml deserialize(byte[] message) throws IOException {
        return JSON.parseObject(new String(message),new TypeReference<Dml>(){});
    }

    @Override
    public boolean isEndOfStream(Dml nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Dml> getProducedType() {
        return TypeInformation.of(new TypeHint<Dml>(){});
    }
}
