package com.xh.flink.stream.spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 欺诈检测器
 * 欺诈检测器实现为KeyedProcessFunction。KeyedProcessFunction#processElement每个交易事件都会调用其方法。第一个版本会在每笔交易时发出警报，有人会说这过于保守。
 * 本教程的后续步骤将指导您使用更有意义的业务逻辑扩展欺诈检测器。
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetector.class);

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
        LOG.info(transaction.toString());
        Alert alert = new Alert();
        alert.setId(transaction.getAccountId());
        collector.collect(alert);
    }
}
