package com.xh.flink.table;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.xh.flink.pojo.Tourist;
import com.xh.flink.pojo.TouristOrder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.slf4j.LoggerFactory;

import java.sql.Date;



/**
 * Desc: Convert DataSets to Tables(Use Batch SQL API)
 */
public class TouristSQL {

    private final static String SQL_1 = "SELECT avg(age) as age,count(*) as total FROM tourist ";

    private final static String SQL_2 =  "SELECT a.id,a.name ,count(*) as total " +
            "FROM tourist a,tourist_order b " +
            "where a.id = b.touristId " +
            "group by a.id,a.name";

    private final static String SQL_3 = "SELECT * FROM tourist_order where createTime >= '2020-07-27' ";

    public static void main(String[] args) throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.ERROR);

        /**
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        env.setParallelism(1);


        DataSet<Tourist> touristDataSet = env.fromElements(
                new Tourist("1","Mike", 1),
                new Tourist("2","Jack", 2),
                new Tourist("3","Leo", 2));
        DataSet<TouristOrder> touristOrderDataSet = env.fromElements(
                new TouristOrder("order1","1", 120,Date.valueOf("2020-06-27")),
                new TouristOrder("order2","1", 130,Date.valueOf("2020-07-27")),
                new TouristOrder("order3","2", 130,Date.valueOf("2020-08-27")),
                new TouristOrder("order4","2", 130,Date.valueOf("2020-06-27")),
                new TouristOrder("order5","2", 240, Date.valueOf("2020-07-27")));

        Table touristTable = tEnv.fromDataSet(touristDataSet,"id,name,age");
        tEnv.createTemporaryView("tourist",touristTable);

        Table touristOrderTable = tEnv.fromDataSet(touristOrderDataSet,"id,touristId,money,createTime");
        tEnv.createTemporaryView("tourist_order",touristOrderTable);


        // 查询客户总人数、平均年龄
        Table avgTable = tEnv.sqlQuery(SQL_1);
        TupleTypeInfo<Tuple2<Integer, Long>> tupleTypeInfo = new TupleTypeInfo<>(Types.INT(),Types.LONG());
        DataSet<Tuple2<Integer, Long>> r1= tEnv.toDataSet(avgTable,tupleTypeInfo);
        r1.print();
        System.out.println("--------");

        // 查询客户购票次数
        Table orderCountTable = tEnv.sqlQuery(SQL_2);
        TupleTypeInfo<Tuple3<String,String,Long>> tupleTypeInfo2 = new TupleTypeInfo<>(Types.STRING(),Types.STRING(),Types.LONG());
        DataSet<Tuple3<String,String,Long>> r2 = tEnv.toDataSet(orderCountTable, tupleTypeInfo2);
        r2.print();
        System.out.println("--------");

        // 查 >=2020-07-27月的订单
        Table tb3 = tEnv.sqlQuery(SQL_3);
        DataSet<TouristOrder> r3 = tEnv.toDataSet(tb3, TouristOrder.class);
        r3.print();
        System.out.println("--------");
        **/
    }

    /**
     * Simple example for demonstrating the use of the Table API for a Word Count in Java.
     *
     * <p>This example shows how to:
     *  - Convert DataSets to Tables
     *  - Apply group, aggregate, select, and filter operations
     */
    public static class WordCountTable {

        // *************************************************************************
        //     PROGRAM
        // *************************************************************************

        public static void main(String[] args) throws Exception {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            /**
            BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

            DataSet<WC> input = env.fromElements(
                    new WC("Hello", 1),
                    new WC("Ciao", 1),
                    new WC("Hello", 1));

            Table table = tEnv.fromDataSet(input);

            Table filtered = table
                    .groupBy($("word"))
                    .select($("word"), $("frequency").sum().as("frequency"))
                    .filter($("frequency").isEqual(2));

            DataSet<WC> result = tEnv.toDataSet(filtered, WC.class);

            result.print();
             **/
        }

        // *************************************************************************
        //     USER DATA TYPES
        // *************************************************************************

        /**
         * Simple POJO containing a word and its respective count.
         */
        public static class WC {
            public String word;
            public long frequency;

            // public constructor to make it a Flink POJO
            public WC() {}

            public WC(String word, long frequency) {
                this.word = word;
                this.frequency = frequency;
            }

            @Override
            public String toString() {
                return "WC " + word + " " + frequency;
            }
        }
    }
}
