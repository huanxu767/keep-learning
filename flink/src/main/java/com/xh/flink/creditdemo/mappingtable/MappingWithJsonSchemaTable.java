package com.xh.flink.creditdemo.mappingtable;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 将kafka数据映射table
 *
 *  https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/connectors/kafka.html
 *  ./kafka-console-producer.sh --broker-list dev-dw1:9092,dev-dw2:9092,dev-dw3:9092,dev-dw4:9092,dev-dw5:9092 --topic orders
 *  {"amount":1759.0,"product":"名4","ts":"2020-07-29 17:40:10","user_id":"1758"}
 */
public class MappingWithJsonSchemaTable {

    private static final String create_kafka_table = "";

    private static final String create_mysql_sink =   "create table dw_orders_count (\n" +
            "window_start STRING," +
            "order_num BIGINT, " +
            "total_amount INT, " +
            "unique_products BIGINT," +
            "PRIMARY KEY (window_start) NOT ENFORCED " +
            ") with(" +
            " 'connector' = 'jdbc'," +
            " 'url' = 'jdbc:mysql://localhost:3306/xdb'," +
            " 'username' = 'root'," +
            " 'password' = 'xuhuan'," +
            " 'table-name' = 'dw_orders_count'," +
            " 'format.schema'" +
            ")";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // default

        tEnv.executeSql(create_kafka_table);
        tEnv.executeSql(create_mysql_sink);

        String q1 = "SELECT * from orders where product = '名0' ";
        Table table1 = tEnv.sqlQuery(q1);
        tEnv.createTemporaryView("orders_view", table1);

//        Table table2 = tEnv.sqlQuery("select * from orders_view");
//        tEnv.toAppendStream(table2, Row.class).print();


        String query = "SELECT\n" +
        "  CAST(TUMBLE_START(ts, INTERVAL '60' SECOND) AS STRING) window_start,\n" +
        "  COUNT(*) order_num,\n" +
        "  SUM(amount) total_amount,\n" +
        "  COUNT(DISTINCT product) unique_products\n" +
        "FROM orders_view\n" +
        "GROUP BY TUMBLE(ts, INTERVAL '60' SECOND)";
        Table result = tEnv.sqlQuery(query);
        tEnv.toAppendStream(result, Row.class).print();


//
//
//        Table table2 = tEnv.sqlQuery("select count(*) from orders_view");
//        tEnv.toRetractStream(table2, Row.class).print();
//        String query = "SELECT\n" +
//                "  CAST(TUMBLE_START(ts, INTERVAL '30' SECOND) AS STRING) window_start,\n" +
//                "  COUNT(*) order_num,\n" +
//                "  SUM(amount) total_amount,\n" +
//                "  COUNT(DISTINCT product) unique_products\n" +
//                "FROM orders\n" +
//                "GROUP BY TUMBLE(ts, INTERVAL '30' SECOND)";
//
//        System.out.println(query);
//
//        Table result = tEnv.sqlQuery(query);
//        result.executeInsert("dw_orders_count");
//        tEnv.toAppendStream(result, Row.class).print();

        env.execute("Streaming Window SQL Job");

    }
}
