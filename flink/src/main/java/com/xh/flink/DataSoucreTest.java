package com.xh.flink;


import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.xh.flink.pojo.Person;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.StringValue;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DataSoucreTest {

    private final static String FILE_PATH = "file:///Users/xuhuan/IdeaProjects/learn-one-learn/flink/src/main/resources/files/a.txt";

    private final static String FILE_T_PATH = "file:///Users/xuhuan/IdeaProjects/learn-one-learn/flink/src/main/resources/files/csv/";


    private final static String CSV_FILE_PATH = "file:///Users/xuhuan/IdeaProjects/learn-one-learn/flink/src/main/resources/files/csv/c.csv";

    public static void main(String[] args) throws Exception {


//        Log4jLoggerFactory log4jLoggerFactory = (Log4jLoggerFactory) LoggerFactory.getILoggerFactory();
//        Logger logger = log4jLoggerFactory.getLogger("root");
//        logger.isInfoEnabled();


        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.ERROR);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //普通文件
        DataSet<String> dateSet1 = env.readTextFile(FILE_PATH);
        System.out.println("----readTextFile---");
        System.out.println(dateSet1.collect());
        DataSet<StringValue> dateSet2 = env.readTextFileWithValue(FILE_PATH);
        System.out.println("----readTextFileWithValue---");
        System.out.println(dateSet2.collect());

        //CSV 默认lineDelimiter \n ,fieldDelimiter ,
        DataSet<Tuple3<Long, String, Double>> csvDataSet =
                env.readCsvFile(CSV_FILE_PATH)
                .lineDelimiter("\n").fieldDelimiter(",")
                .types(Long.class,String.class,Double.class);
        System.out.println("----readCsvFile---");
        System.out.println(csvDataSet.collect());

        //CSV 返回pojo includeFields(101)提取部分数据
        //读整个目录 + 递归
        Configuration configuration = new Configuration();
        configuration.setBoolean("recursive.file.enumeration", true);

        DataSet<Person> personDataSet =
                env.readCsvFile(FILE_T_PATH)
                        .pojoType(Person.class,"id","name","score").withParameters(configuration);
        System.out.println("----readCsvFile return Pojo---");
        System.out.println(personDataSet.collect());

        DataSet<String> fromElementsDataSet = env.fromElements("a b c","e f g","a b c");
        System.out.println(fromElementsDataSet);
        System.out.println("------fromElementsDataSet--------");


        List list = new ArrayList<>();
        list.add("a");list.add("b");list.add("c");
        DataSet<String> fromCollectionDataSet = env.fromCollection(list);
        System.out.println("------fromElementsDataSet--------");
        System.out.println(fromCollectionDataSet.collect());




    }

}
