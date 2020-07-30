package com.xh.flink.creditdemo.mappingtable;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;


/**
 * 将kafka数据映射table
 *
 * https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/connectors/kafka.html
 * ./kafka-console-producer.sh --broker-list dev-dw1:9092,dev-dw2:9092,dev-dw3:9092,dev-dw4:9092,dev-dw5:9092 --topic order
 */
public class Mapping3Table {

    private static final String create_kafka_table = "CREATE TABLE orders (\n" +
            "  user_id INT,\n" +
            "  product STRING,\n" +
            "  amount INT,\n" +
            "  ts TIMESTAMP(3) ,\n" +
            "  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND\n" +
            ") WITH (\n" +
            " 'connector' = 'kafka',\n" +
            " 'topic' = 'orders',\n" +
            " 'properties.bootstrap.servers' = 'dev-dw1:9092,dev-dw2:9092,dev-dw3:9092,dev-dw4:9092,dev-dw5:9092',\n" +
            " 'properties.group.id' = 'testGroup',\n" +
            " 'format' = 'json',\n" +
            " 'scan.startup.mode' = 'earliest-offset',\n"+
            " 'json.fail-on-missing-field' = 'false',\n" +
            " 'json.ignore-parse-errors' = 'true'\n" +
            ")";

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
            " 'table-name' = 'dw_orders_count'" +
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

        // run a SQL query on the table and retrieve the result as a new Table
//        String query  =" select count(*) from user_behavior";
        String query = "SELECT\n" +
                "  CAST(TUMBLE_START(ts, INTERVAL '30' SECOND) AS STRING) window_start,\n" +
                "  COUNT(*) order_num,\n" +
                "  SUM(amount) total_amount,\n" +
                "  COUNT(DISTINCT product) unique_products\n" +
                "FROM orders\n" +
                "GROUP BY TUMBLE(ts, INTERVAL '30' SECOND)";
        Table result = tEnv.sqlQuery(query);
        result.executeInsert("dw_orders_count");
        tEnv.toAppendStream(result, Row.class).print();



        String query2  =" select * from user_behavior where product = ''";


        env.execute("Streaming Window SQL Job");

    }
}
