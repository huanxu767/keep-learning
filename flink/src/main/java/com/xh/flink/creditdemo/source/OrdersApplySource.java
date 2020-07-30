package com.xh.flink.creditdemo.source;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.xh.flink.creditdemo.model.CreditApply;
import com.xh.flink.creditdemo.model.OrdersApply;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

public class OrdersApplySource implements SourceFunction<String>{

    private volatile boolean isRunning = true;
    private int i = 1;
    @Override
    public void run(SourceContext<String> ctx) throws Exception {

//        String fo = "yyyy-MM-dd'T'HH:mm:ss";
        String fo = "yyyy-MM-dd HH:mm:ss";
//        String fo = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

//        String fo = "yyyy-MM-dd HH:mm:ss.SSS";
//        String fo = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        SimpleDateFormat sf = new SimpleDateFormat(fo);
        while (isRunning) {
            OrdersApply creditApply = OrdersApply.builder()
                    .user_id(i++ +"")
                    .product("名"+ i % 5)
                    .amount( new Double(i))
//                    .ts(System.currentTimeMillis() + "")
                    .ts(sf.format(new Date()))
                    .build();
            Thread.sleep(1000);
            Gson gson = new GsonBuilder().serializeNulls().create();
            ctx.collect(gson.toJson(creditApply));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

}
