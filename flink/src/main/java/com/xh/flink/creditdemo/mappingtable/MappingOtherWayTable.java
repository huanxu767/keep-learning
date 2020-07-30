package com.xh.flink.creditdemo.mappingtable;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.xh.flink.creditdemo.config.CreditConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.slf4j.LoggerFactory;

/**
 * 将kafka数据映射table
 * https://blog.csdn.net/baichoufei90/article/details/102747748
 */
public class MappingOtherWayTable {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env;
        StreamTableEnvironment tEnv;

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance()
                        // Watermark is only supported in blink planner
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build()
        );
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);


        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // default

        final String createTable = String.format(
                    "create table credit_apply (\n" +
                            "  qryCreditId STRING,\n" +
                            "  name STRING,\n" +
                            "  idCard STRING,\n" +
                            "  province STRING,\n" +
                            "  ts TIMESTAMP(3),\n" +
                            "  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND\n" +
                            ") with (\n" +
                            " 'connector' = 'kafka',\n" +
                            " 'topic' = '%s',\n" +
                            " 'properties.bootstrap.servers' = '%s',\n" +
                            " 'properties.group.id' = '%s',\n" +
                            " 'format' = '%s',\n" +
                            " 'scan.startup.mode' = 'earliest-offset'" +
                            ")",
                    "flink_credit_apply_topic",
                    CreditConfig.BOOTSTRAP_SERVERS,
                    CreditConfig.GROUP_ID,
                    "csv");
        tEnv.executeSql(createTable);


        Table result = tEnv.sqlQuery("" +
                "SELECT * " +
                "FROM credit_apply");

//
//        Table result = tEnv.sqlQuery("" +
//                "SELECT CAST(TUMBLE_START(ts, INTERVAL '5' SECOND) AS STRING) window_start," +
//                "COUNT(*) order_num\n" +
//                "FROM credit_apply\n" +
//                "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)");
        tEnv.toAppendStream(result, Row.class).print();

        env.execute("Streaming Window SQL Job");

    }
}
