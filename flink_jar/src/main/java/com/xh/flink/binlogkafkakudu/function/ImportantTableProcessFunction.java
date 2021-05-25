package com.xh.flink.binlogkafkakudu.function;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.xh.flink.binlog.Dml;
import com.xh.flink.binlogkafkakudu.BinlogToKudu;
import com.xh.flink.binlogkafkakudu.config.ImportantTableDO;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImportantTableProcessFunction extends KeyedBroadcastProcessFunction<String, Dml, ImportantTableDO, String> {

    private static final Logger log = LoggerFactory.getLogger(ImportantTableProcessFunction.class);


    @Override
    public void processElement(Dml dml, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        // 获取配置流
        ImportantTableDO importantTableDO = ctx.getBroadcastState(BinlogToKudu.importantTableFlowStateDescriptor).get(dml.getDatabase() + dml.getTable());

        Gson gson = new GsonBuilder().serializeNulls().create();

        if (null != importantTableDO ) {
            log.info(importantTableDO.toString());
            out.collect(gson.toJson(dml));
        }
    }

    @Override
    public void processBroadcastElement(ImportantTableDO importantTableDO, Context ctx, Collector<String> out) throws Exception {
        // 获取state状态
        BroadcastState<String, ImportantTableDO> broadcastState = ctx.getBroadcastState(BinlogToKudu.importantTableFlowStateDescriptor);

        // 更新state
        broadcastState.put(importantTableDO.getDbName() + importantTableDO.getTableName(), importantTableDO);
    }
}
