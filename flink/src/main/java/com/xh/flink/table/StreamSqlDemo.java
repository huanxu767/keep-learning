package com.xh.flink.table;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.xh.flink.pojo.SqlPojo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * sql的
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/common.html
 */
public class StreamSqlDemo {

    public static void main(String[] args) throws Exception {

        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.ERROR);

        bachQuery();
//        blinkStreamQuery();

    }

    static void bachQuery() throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(env);

        List<Tuple3<Integer,Integer,String>> list = IntStream.range(1,100)
                .mapToObj(i -> Tuple3.of(i,i%20,i+"xx"))
                .collect(Collectors.toList());
        DataSource<Tuple3<Integer,Integer,String>> dataset = env.fromCollection(list);

        Table table = fbTableEnv.fromDataSet(dataset,"id,age,name");
        fbTableEnv.createTemporaryView("h_user",table);
        Table resultTable = fbTableEnv.sqlQuery("select * from h_user where id < 10 and name like '5x%'");
        TupleTypeInfo<Tuple3<Integer, Integer,String>> tupleTypeInfo = new TupleTypeInfo<>(Types.INT(),Types.INT(),Types.STRING());
        DataSet<Tuple3<Integer,Integer,String>> dataSet = fbTableEnv.toDataSet(resultTable,tupleTypeInfo);
        dataSet.print();
    }

    static void blinkStreamQuery() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);

        List<Tuple3<Integer,Integer,String>> list = IntStream.range(1,100)
                .mapToObj(i -> Tuple3.of(i,i%20,i+"xx"))
                .collect(Collectors.toList());
        DataStream<Tuple3<Integer,Integer,String>> dataStream = env.fromCollection(list);

        // Convert a DataStream or DataSet into a Table
        // Convert the DataStream into a Table with default fields "f0", "f1"
        // Table table1 = tableEnv.fromDataStream(stream);

        // Convert the DataStream into a Table with fields "id", "age" ，"name"
        Table table = bsTableEnv.fromDataStream(dataStream,"id,age,name");


        // convert the Table into an append DataStream of Row by specifying the class
//        DataStream<Row> dsRow = bsTableEnv.toAppendStream(table, Row.class);
//        dsRow.print();
        bsTableEnv.createTemporaryView("h_user",table);

        // scan registered Orders table
//        Table userTable = bsTableEnv.from("h_user");
        Table resultTable = bsTableEnv.sqlQuery("select * from h_user where id < 10");

        TupleTypeInfo<Tuple3<Integer, Integer,String>> tupleTypeInfo = new TupleTypeInfo<>(Types.INT(),Types.INT(),Types.STRING());
//        DataStream<Tuple2<Boolean, Tuple3<Integer, Integer, String>>> dataSetTuple2 = bsTableEnv.toRetractStream(resultTable,tupleTypeInfo);
        DataStream<Tuple3<Integer, Integer,String>> dataSetTuple3 = bsTableEnv.toAppendStream(resultTable,tupleTypeInfo);
        dataSetTuple3.print();
        bsTableEnv.execute("tale_job");

    }


    /**
     * 创建一个 TableEnvironment
     * 4种
     */
    static void createTableEnvironment(){
        // **********************
        // FLINK STREAMING QUERY
        // **********************
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
        // or TableEnvironment fsTableEnv = TableEnvironment.create(fsSettings);

        // ******************
        // FLINK BATCH QUERY
        // ******************
        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);

        // **********************
        // BLINK STREAMING QUERY
        // **********************
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        // or TableEnvironment bsTableEnv = TableEnvironment.create(bsSettings);

        // ******************
        // BLINK BATCH QUERY
        // ******************
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);
    }
}
