package com.xh.flink.binlogkafkaflinkhbase.support;

import com.xh.flink.binlog.Dml;
import com.xh.flink.binlogkafkaflinkhbase.BinlogToHBase;
import com.xh.flink.binlogkafkaflinkhbase.FlowStatusEnum;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class DbusProcessFunction extends KeyedBroadcastProcessFunction<String, Dml, Flow, Tuple2<Dml, Flow>> {

    @Override
    public void processElement(Dml value, ReadOnlyContext ctx, Collector<Tuple2<Dml, Flow>> out) throws Exception {
        // 获取配置流
        Flow flow = ctx.getBroadcastState(BinlogToHBase.flowStateDescriptor).get(value.getDatabase() + value.getTable());

        if (null != flow && flow.getStatus() == FlowStatusEnum.FLOW_STATUS_RUNNING.getCode()) {
            out.collect(Tuple2.of(value, flow));
        }
    }

    @Override
    public void processBroadcastElement(Flow flow, Context ctx, Collector<Tuple2<Dml, Flow>> out) throws Exception {
        // 获取state状态
        BroadcastState<String, Flow> broadcastState = ctx.getBroadcastState(BinlogToHBase.flowStateDescriptor);

        // 更新state
        broadcastState.put(flow.getDatabaseName() + flow.getTableName(), flow);
    }
}
