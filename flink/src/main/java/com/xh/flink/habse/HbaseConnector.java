package com.xh.flink.habse;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 操作HBase
 *
 * 继承RichSourceFunction重写父类方法（flink streaming） 实现自定义TableInputFormat接口（flink streaming和flink dataSet）
 * Flink上将数据写入HBase也有两种方式：
 *
 * 继承RichSinkFunction重写父类方法（flink streaming） 实现OutputFormat接口（flink streaming和flink dataSet）
 */
public class HbaseConnector {

    private final static String TABLE_NAME = "flink_demo";

    public static void main(String[] args) throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.ERROR);


//        Configuration conf= HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum","dev-dw1,dev-dw2,dev-dw3,dev-dw4,dev-dw5");
//        conf.set("mapreduce.output.fileoutputformat.outputdir","/tmp");
//        conf.set(TableOutputFormat.OUTPUT_TABLE,TABLE_NAME);
//        HbaseTemplate hbaseTemplate = new HbaseTemplate(conf);
//        createTable();
//        writeToHBase();
        DataSet<Tuple2<String, String>> ll = read();
        ll.print();
    }

    static void writeToHBase() throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //准备数据
        List<Tuple4<String,String,String,String>> data =  Stream.of(Tuple4.of("1000","xu1","23","nanjin1"),
                Tuple4.of("1001","xu2","24","nanjin2"),
                Tuple4.of("1002","xu3","25","nanjin3"),
                Tuple4.of("1003","xu4","26","nanjin4")).collect(Collectors.toList());
        DataSet<Tuple4<String,String,String,String>> dataSet = env.fromCollection(data);
        //转换成hbase操作
//        DataSet<Tuple2<Text,Mutation>> hbaseResult = convertResultToMutation(dataSet);
//        hbaseResult.output(new TableOutputFormat<>());
//        TableOutputFormat
//        Job job = Job.getInstance(conf);
//        hbaseResult.output();
//        hbaseResult.output(new WriteHbase());
//        hbaseResult.print();
    }

//    static DataSet<Tuple2<Text, Mutation>> convertResultToMutation(DataSet<Tuple4<String,String,String,String>> user){
//        return user.map(new MapFunction<Tuple4<String, String, String, String>, Tuple2<Text, Mutation>>() {
//            @Override
//            public Tuple2<Text, Mutation> map(Tuple4<String, String, String, String> value) throws Exception {
//                Text text = new Text(value.f0);
//                Put put = new Put(Bytes.toBytes(value.f0));
//                if(StringUtils.isNotEmpty(value.f1)){
//                    put.addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("name"), Bytes.toBytes(value.f1));
//                }
//                if(StringUtils.isNotEmpty(value.f2)) {
//                    put.addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("age"), Bytes.toBytes(value.f2));
//                }
//                if(StringUtils.isNotEmpty(value.f3)) {
//                    put.addColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("addr"), Bytes.toBytes(value.f3));
//                }
//                return Tuple2.of(text,put);
//            }
//        });
//    }

    static DataSet<Tuple2<String, String>> read() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /**
        DataSet<Tuple2<String, String>> hbaseInput =  env.createInput(new TableInputFormat<Tuple2<String, String>>(){

            private HTable hTable;
            private void createTable() throws IOException {
                Configuration conf= HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum","dev-dw1,dev-dw2,dev-dw3,dev-dw4,dev-dw5");
                Connection con = ConnectionFactory.createConnection(conf);
                this.table = (HTable) con.getTable(TableName.valueOf(getTableName()));
            }

            @Override
            public void configure(org.apache.flink.configuration.Configuration parameters) {
//                super.configure(parameters);
                try {
                    createTable();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (table != null){
                    scan = getScanner();
                }
            }

            @Override
            protected Scan getScanner() {
                Scan scan = new Scan();
                scan.setLimit(5);
                return scan;
            }
            @Override
            protected String getTableName() {
                return "flink_demo";
            }
            @Override
            protected Tuple2<String, String> mapResultToTuple(Result result) {

                Tuple2<String,String> tup = new Tuple2<String,String>();
                System.out.println(Bytes.toString(result.getRow()) + "--------" + Bytes.toString(result.getValue("foo".getBytes(), "f1".getBytes())));
                tup.setField(Bytes.toString(result.getRow()),0);
                tup.setField(Bytes.toString(result.getValue("foo".getBytes(), "f1".getBytes())), 1);
                return tup;
            }
        });
        return hbaseInput;
            **/
        return null;
    }


}
