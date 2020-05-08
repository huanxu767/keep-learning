package com.xh.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 手写体 haha
 */
public class WordCountMy {

    public static void main(String[] args) throws Exception {
       ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
       DataSet<String> dataSet = env.fromElements("a b c","e f g","a b c");
       DataSet<Tuple2<String, Integer>> counts =
               dataSet.flatMap(new myFlagMapFunction()).groupBy(0).sum(1);
       System.out.println(counts.collect());
    }

    static class myFlagMapFunction implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String temp :s.split(" ")) {
                collector.collect(new Tuple2<>(temp,1));
            }
        }
    }
}
