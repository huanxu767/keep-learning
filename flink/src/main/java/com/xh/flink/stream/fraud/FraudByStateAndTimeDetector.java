package com.xh.flink.stream.fraud;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 欺诈检测器
 * 欺诈检测器实现为KeyedProcessFunction。KeyedProcessFunction#processElement每个交易事件都会调用其方法。第一个版本会在每笔交易时发出警报，有人会说这过于保守。
 * 本教程的后续步骤将指导您使用更有意义的业务逻辑扩展欺诈检测器。
 * 骗子们不会等很久就进行大量购买，以减少注意到他们的测试交易的机会。
 * 例如，假设您要为欺诈检测器设置1分钟超时；也就是说，在前面的示例中，如果交易3和4发生在彼此之间1分钟之内，则交易3和4仅被视为欺诈。
 * Flink KeyedProcessFunction允许您设置计时器，该计时器在将来的某个时间点调用回调方法。
 */
public class FraudByStateAndTimeDetector extends KeyedProcessFunction<Long, Transaction, Alert> {
    private static final Logger LOG = LoggerFactory.getLogger(FraudByStateAndTimeDetector.class);

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    //让我们看看如何修改工作以符合我们的新要求：
    //每当标志设置为时true，还要在将来设置1分钟的计时器。
    //当计时器触发时，通过清除其状态来重置标志。
    //如果清除了该标志，则应取消计时器。
    //要取消计时器，您必须记住设置的时间，并记住隐含的状态，因此首先要创建一个计时器状态以及您的标志状态。
    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;


    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info(Thread.currentThread().getId() + ":open");
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<Boolean>("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<Long>("timer-state",Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
        LOG.info("processElement");
        LOG.info(transaction.toString());

        // Get the current state for the current key
        Boolean lastTransactionWasSmall = flagState.value();

        // Check if the flag is set
        if(lastTransactionWasSmall != null){
            if(transaction.getAmount() > LARGE_AMOUNT){
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);
            }
            // Clean up our state
            flagState.clear();
        }
        LOG.info(transaction.getAmount() + "");
        if (transaction.getAmount() < SMALL_AMOUNT) {
            // Set the flag to true
            flagState.update(true);
            // KeyedProcessFunction#processElement用Context包含计时器服务的调用。
            // 计时器服务可用于查询当前时间，注册计时器和删除计时器。这样一来，您可以在每次设置标记后将计时器设置为将来1分钟，并将时间戳记存储在中timerState。
            // set the timer and timer state
            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);
            timerState.update(timer);
        }
    }
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
        // remove flag after 1 minute
        LOG.info("onTimer");
        timerState.clear();
        flagState.clear();
    }

    private void cleanUp(Context ctx) throws Exception {
        LOG.info("cleanUp");
        // delete timer
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        // clean up all state
        timerState.clear();
        flagState.clear();
    }

}
