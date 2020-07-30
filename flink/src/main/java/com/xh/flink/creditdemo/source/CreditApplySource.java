package com.xh.flink.creditdemo.source;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.xh.flink.creditdemo.model.CreditApply;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

public class CreditApplySource implements SourceFunction<String>, CheckpointedFunction{

    private volatile boolean isRunning = true;
    private long count = 0L;
    private transient ListState<Long> checkpointedCount;

    private int i = 1;
    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        String fo = "yyyy-MM-dd'T'HH:mm:ss";
//        String fo = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

//        String fo = "yyyy-MM-dd HH:mm:ss.SSS";
//        String fo = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        SimpleDateFormat sf = new SimpleDateFormat(fo);

        while (isRunning && count < 1000) {
            CreditApply creditApply = CreditApply.builder()
                    .qryCreditId(i++ +"")
                    .name("名"+ i % 5)
                    .idCard("身份证" + i % 5)
                    .province("江苏")
                    .ts(sf.format(new Date()))
//                    .ts(System.currentTimeMillis())
//                    .rowtime(new Date())
//                    .timestamp(SqlTimestamp.fromEpochMillis(System.currentTimeMillis()).toString())
                    .build();
            Thread.sleep(1000);
            synchronized (ctx.getCheckpointLock()) {
                Gson gson = new GsonBuilder().serializeNulls().create();
                ctx.collect(gson.toJson(creditApply));
                count++;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.checkpointedCount = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("count", Long.class));

        if (context.isRestored()) {
            for (Long count : this.checkpointedCount.get()) {
                this.count = count;
            }
        }
    }
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.checkpointedCount.clear();
        this.checkpointedCount.add(count);
    }
}
