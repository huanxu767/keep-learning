package com.xh.flink.table.spend;

import org.apache.flink.walkthrough.common.table.SpendReportTableSink;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.walkthrough.common.table.UnboundedTransactionTableSource;

/**
 * 您将学习如何建立一个连续的ETL管道，以随时间推移按帐户跟踪财务交易。您将首先将报告构建为夜间批处理作业，然后迁移到流式处理管道。
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/getting-started/walkthroughs/table_api.html
 */
public class SpendReport {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //接下来，在执行环境中注册表，您可以使用这些表连接到外部系统以读取和写入批处理和流数据。
        // 表源提供对存储在外部系统中的数据的访问；例如数据库，键值存储，消息队列或文件系统。
        // 表接收器将表发送到外部存储系统。根据源和接收器的类型，它们支持不同的格式，例如CSV，JSON，Avro或Parquet。
        tEnv.registerTableSource("transactions", new UnboundedTransactionTableSource());
        tEnv.registerTableSink("spend_report", new SpendReportTableSink());

        tEnv
                .scan("transactions")
                .window(Tumble.over("1.hour").on("timestamp").as("w"))
                .groupBy("accountId, w")
                .select("accountId, w.start as timestamp, amount.sum")
                .insertInto("spend_report");

        env.execute("Spend Report");
    }
}
