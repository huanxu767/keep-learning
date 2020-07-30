package com.xh.flink.table;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.slf4j.LoggerFactory;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/connect.html
 * Flink的Table API和SQL程序可以连接到其他外部系统，以读取和写入批处理表和流式表。
 * 表源提供对存储在外部系统（例如数据库，键值存储，消息队列或文件系统）中的数据的访问。表接收器将表发送到外部存储系统。根据源和接收器的类型，它们支持不同的格式，例如CSV，Parquet或ORC。
 * 本页介绍如何声明内置表源和/或表接收器以及如何在Flink中注册它们。注册源或接收器后，可以通过Table API和SQL语句对其进行访问。
 *
 * ./kafka-console-producer.sh --broker-list dev-dw1:9092,dev-dw2:9092,dev-dw3:9092,dev-dw4:9092,dev-dw5:9092, --topic flink-test-table-input
 *
 * {"userId":1,"day":"7","begintime":12873874382,"data":[{"package":"3231","activetime":33333}]}
 */
public class ConnectorKafkaDemo {
    private static final String BOOTSTRAP_SERVERS = "dev-dw1:9092,dev-dw2:9092,dev-dw3:9092,dev-dw4:9092,dev-dw5:9092";

    public static void main(String[] args) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.ERROR);

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(fsEnv, fsSettings);

        tableEnvironment
                // declare the external system to connect to
                .connect(
                        new Kafka()
                                .version("universal")// required: valid connector versions are "0.8", "0.9", "0.10", "0.11", and "universal"
                                .topic("flink-test-table-input")
//                                .startFromLatest()
                                .startFromEarliest()
                                .property("group.id","g1")
                                .property("zookeeper.connect", "dev-dw1,dev-dw2,dev-dw3,dev-dw4,dev-dw5")
                                .property("bootstrap.servers", BOOTSTRAP_SERVERS)
                )
                .withFormat(
                        new Json()
                                .failOnMissingField(true)// optional: flag whether to fail if a field is missing or not, false by default
                                .deriveSchema()
                ).withSchema(
                        new Schema()
                                .field("userId", DataTypes.BIGINT())
                                .field("day", DataTypes.STRING())
                                .field("begintime", DataTypes.FLOAT())
                                 .field("data", ObjectArrayTypeInfo.getInfoFor(Row[].class,Types.ROW(
                                         new String[] {"package","activetime"},
                                         new TypeInformation[]{Types.STRING(),Types.LONG()}
                                 )))


        )
                // declare a format for this system
//                .withFormat(
//                        new Avro()
//                                .avroSchema(
//                                        "{" +
//                                                "  \"namespace\": \"org.myorganization\"," +
//                                                "  \"type\": \"record\"," +
//                                                "  \"name\": \"UserMessage\"," +
//                                                "    \"fields\": [" +
//                                                "      {\"name\": \"timestamp\", \"type\": \"string\"}," +
//                                                "      {\"name\": \"user\", \"type\": \"long\"}," +
//                                                "      {\"name\": \"message\", \"type\": [\"string\", \"null\"]}" +
//                                                "    ]" +
//                                                "}"
//                                )
//                )

                // declare the schema of the table
//                .withSchema(
//                        new Schema()
//                                .field("rowtime", DataTypes.TIMESTAMP(3))
//                                .rowtime(new Rowtime()
//                                        .timestampsFromField("timestamp")
//                                        .watermarksPeriodicBounded(60000)
//                                )
//                                .field("user", DataTypes.BIGINT())
//                                .field("message", DataTypes.STRING())
//                )

                // create a table with given name
                .inAppendMode()
                .createTemporaryTable("MyUserTable");
        Table t1 = tableEnvironment.sqlQuery(" select userId from MyUserTable");
        DataStream<Row> ds1 = tableEnvironment.toAppendStream(t1,Row.class);
        ds1.print();

        Table t2 = tableEnvironment.sqlQuery(" select count(*) from MyUserTable");
        DataStream<Tuple2<Boolean, Row>> ds2 = tableEnvironment.toRetractStream(t2,Row.class);
        ds2.print();
        try {
            fsEnv.execute("kafka_json");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
