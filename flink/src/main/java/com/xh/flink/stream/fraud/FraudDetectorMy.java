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
 */
public class FraudDetectorMy extends KeyedProcessFunction<Long, Transaction, Alert> {
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectorMy.class);

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;
    //将不需要序列化的属性前添加关键字transient，序列化对象的时候，这个属性就不会被序列化。
    //ValueState是包装类，类似于Java标准库AtomicReference或AtomicLong在Java标准库中。
    // 它提供了三种与其内容进行交互的方法；update设置状态，value获取当前值并clear删除其内容。
    // 如果特定键的状态为空，例如在应用程序开始时或在调用之后ValueState#clear，ValueState#value则将返回null。ValueState#value系统不保证对由返回的对象进行的修改不会被系统识别，
    // 因此必须使用进行所有更改ValueState#update。否则，容错功能由Flink在后台自动管理，因此您可以像使用任何标准变量一样与之交互。
    private transient ValueState<Boolean> flagState;

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("open");
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<Boolean>("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);
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
        }

    }
}
