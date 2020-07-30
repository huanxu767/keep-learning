package com.xh.flink.creditdemo.mappingtable;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.xh.flink.creditdemo.config.CreditConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.slf4j.LoggerFactory;

/**
 * 将kafka数据映射table
 *  https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/connect.html
 */
public class MappingTable {


    public static void main(String[] args) throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.ERROR);
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(bsEnv, bsSettings);

        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        bsTableEnv.useCatalog("credit_catalog");
//        bsTableEnv.useDatabase("credit_database");
        tableEnvironment
                .connect(
                        new Kafka()
                                .version("universal")// required: valid connector versions are "0.8", "0.9", "0.10", "0.11", and "universal"
                                .topic(CreditConfig.CREDIT_APPLY_TOPIC)
//                                .startFromLatest()
                                .startFromEarliest()
                                .property("group.id",CreditConfig.GROUP_ID)
//                                .property("zookeeper.connect", CreditConfig.ZOOKEEPER_CONNECT)
                                .property("bootstrap.servers", CreditConfig.BOOTSTRAP_SERVERS)
                )
                .withFormat(
                        new Json()
                ).withSchema(
                        new Schema()
//                                .field("rowtime", DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class))
                                // Converts an existing LONG or SQL_TIMESTAMP field in the input into the rowtime attribute.
//                                .field("rowtime", DataTypes.TIMESTAMP(3))
//                                    .rowtime(new Rowtime()
//                                            .timestampsFromField("timestamp.millisecond")
////                                             .timestampsFromField("timestamp")
//                                            .watermarksPeriodicBounded(60000)
//                                    )
                                .field("timestamp", DataTypes.STRING())
                                .field("qryCreditId", DataTypes.STRING())
                                .field("name", DataTypes.STRING())
                                .field("idCard", DataTypes.STRING())
                                .field("province", DataTypes.STRING())
                ).inAppendMode()
                .createTemporaryTable("credit_apply");
        Table t1 = tableEnvironment.sqlQuery("select * from credit_apply");
        DataStream<Row> ds1 = tableEnvironment.toAppendStream(t1,Row.class);
        ds1.print();
        t1.printSchema();

//
//        tableEnvironment.registerTableSink("credit_apply_count", new SpendReportTableSink());
//
//1595902214964

//        tableEnvironment
//                .from("credit_apply")
//                .window(Tumble.over("1.minutes").on("rowtime").as("w"))
//                .groupBy("w, a")
//                .select("province,w.rowtime, qryCreditId.count ")
//                .insertInto("credit_apply_count");
//        Table t3 = tableEnvironment.sqlQuery("select * from credit_apply_count");
//        DataStream<Row> ds3 = tableEnvironment.toAppendStream(t3,Row.class);
//        ds3.print();

        bsEnv.execute("createTemporaryTable credit_apply ");
    }
}
