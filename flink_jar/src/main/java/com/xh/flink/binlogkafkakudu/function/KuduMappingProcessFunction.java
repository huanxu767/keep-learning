package com.xh.flink.binlogkafkakudu.function;

import com.xh.flink.binlog.Dml;
import com.xh.flink.binlogkafkakudu.BinlogToKudu;
import com.xh.flink.binlogkafkakudu.config.KuduMapping;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduMappingProcessFunction extends KeyedBroadcastProcessFunction<String, Dml, KuduMapping, Tuple2<Dml, KuduMapping>> {

    private static final Logger log = LoggerFactory.getLogger(KuduMappingProcessFunction.class);


    @Override
    public void processElement(Dml dml, ReadOnlyContext ctx, Collector<Tuple2<Dml, KuduMapping>> out) throws Exception {
        // 获取配置流
        KuduMapping kuduMapping = ctx.getBroadcastState(BinlogToKudu.flowStateDescriptor).get(dml.getDatabase() + dml.getTable());

        if (null != kuduMapping ) {
            log.info(dml.toString());
            out.collect(Tuple2.of(dml, kuduMapping));
        }
    }

    @Override
    public void processBroadcastElement(KuduMapping mapping, Context ctx, Collector<Tuple2<Dml, KuduMapping>> out) throws Exception {
        // 获取state状态
        BroadcastState<String, KuduMapping> broadcastState = ctx.getBroadcastState(BinlogToKudu.flowStateDescriptor);

        // 更新state
        broadcastState.put(mapping.getDatabase() + mapping.getTable(), mapping);
    }
}
