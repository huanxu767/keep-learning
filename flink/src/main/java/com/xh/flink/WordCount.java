package com.xh.flink;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.List;

public class WordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.readTextFile("file:///Users/xuhuan/Desktop/softs/project/learn-one-learn/flink/src/main/resources/files/");

        List<String> str = text.collect();
        for (String a :
                str) {
            System.out.println(a);
        }

        DataSet<Tuple2<String, Integer>> counts =
                // 把每一行文本切割成二元组，每个二元组为: (word,1)
                text.flatMap(new Tokenizer())
                        // 根据二元组的第“0”位分组，然后对第“1”位求和
                        .groupBy(0)
                        .sum(1);
        //未生效
        counts.writeAsCsv("file:///Users/xuhuan/Desktop/softs/project/learn-one-learn/flink/src/main/resources/files/output/32.csv", "\n", " ");

    }
    // 自定义函数
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 统一大小写并把每一行切割为单词
            String[] tokens = value.toLowerCase().split("\\W+");

            // 消费二元组
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

}
