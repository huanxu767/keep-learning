package com.xh.flink.table;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 *
 * https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/formats/canal.html
 * https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/formats/json.html#data-type-mapping
 *
 * {"data": [{"id": "111","name": "scooter","description": "Big 2-wheel scooter","weight": "5.18","ts": "2020-07-29 17:36:30.000"}],"database": "inventory","es": 1589373560000,"id": 9,"isDdl": false,"mysqlType": {"id": "INTEGER","name": "VARCHAR(255)","description": "VARCHAR(512)","weight": "FLOAT"},"old": [{"weight": "5.15"}],"pkNames": ["id"],"sql": "","sqlType": {"id": 4,"name": 12,"description": 12,"weight": 7},"table": "products","ts": 1589373560798,"type": "UPDATE"}
 *
 */
public class CanalSQLExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String ddl = "CREATE TABLE topic_products (\n" +
                "  id BIGINT,\n" +
                "  name STRING,\n" +
                "  description STRING,\n" +
                "  weight DECIMAL(10, 2),\n" +
                "  ts TIMESTAMP(3) " +
//                ",\n" +
//                "  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',"+
                " 'properties.bootstrap.servers' = 'dev-dw1:9092,dev-dw2:9092,dev-dw3:9092,dev-dw4:9092,dev-dw5:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
//                " 'canal-json.ignore-parse-errors' = 'false'," +
                " 'scan.startup.mode' = 'earliest-offset',\n"+
                " 'topic' = 'products_binlog',\n" +
                " 'format' = 'canal-json'\n" +
                ")";
        tEnv.executeSql(ddl);

        // run a SQL query on the table and retrieve the result as a new Table
        String query = "SELECT * from topic_products";
        Table result1 = tEnv.sqlQuery(query);
        tEnv.toRetractStream(result1, Row.class).print();

        String query2 = "SELECT name, AVG(weight),count(id) FROM topic_products GROUP BY name";
        Table result2 = tEnv.sqlQuery(query2);
        tEnv.toRetractStream(result2, Row.class).print();

        String query3 = "SELECT\n" +
                "  CAST(TUMBLE_START(ts, INTERVAL '30' SECOND) AS STRING) window_start,\n" +
                "  COUNT(*) order_num\n" +
                "FROM topic_products\n" +
                "GROUP BY TUMBLE(ts, INTERVAL '30' SECOND)";
        Table result3 = tEnv.sqlQuery(query3);
        tEnv.toAppendStream(result3, Row.class).print();

        // after the table program is converted to DataStream program,
        // we must use `env.execute()` to submit the job.
        env.execute("Streaming Window SQL Job");
    }

}