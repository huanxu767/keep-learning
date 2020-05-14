package com.xh.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class lambdaTest {


    public static void main(String[] args) throws Exception {

        t1();
//        t3();
//        t2();
//        Tuple2 tuple2 = new Tuple2();
//        tuple2.setField("1",1);
//        tuple2.setField("2",0);
//
//        Tuple3 tuple3 = new Tuple3();
//        tuple3.setField("1",1);
//        tuple3.setField("2",2);
//        tuple3.setField("3",0);
//        System.out.println(tuple2);
//        System.out.println(tuple3);

//        mapPartitionTest();
    }


    private static void mapPartitionTest() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Long> dataSet = env.generateSequence(1,20);
        dataSet.mapPartition(new MyMapPartitionFunction()).print();
    }

    private static void t1() throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1,2,3).map(i -> i * i).print();
    }

    private static void t2() throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> input = env.fromElements(1,2,3);
        input.flatMap(
                (Integer number, Collector<String> out) ->{
                    StringBuilder builder = new StringBuilder();
                    for(int i = 0; i < number; i++) {
                        builder.append("a");
                        out.collect(builder.toString());
                    }
                }
        )
        // 显式提供类型信息
        .returns(Types.STRING).print();
    }

    private static void t3() throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 使用显式的 ".returns(...)"
//        env.fromElements(1, 2, 3)
//                // 没有关于 Tuple2 字段的信息
//                .map(i -> Tuple2.of(i, i))
//                .returns(Types.TUPLE(Types.INT, Types.INT))
//                .print();

        // 使用类来替代
        env.fromElements(1,2,3)
                .map(new MyTuple2Mapper())
                .print();

        // 或者在这个例子中用 Tuple 的子类来替代
        env.fromElements(1, 2, 3)
                .map(i -> new DoubleTuple(i, i))
                .print();

    }


    public static class MyTuple2Mapper implements MapFunction<Integer, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Integer i) {
            return Tuple2.of(i, i);
        }
    }


    public static class MyMapPartitionFunction implements MapPartitionFunction<Long,Long>{

        @Override
        public void mapPartition(Iterable<Long> iterable, Collector<Long> collector) throws Exception {
            long count = 0;
            for (Long value:iterable){
                count++;
            }
            collector.collect(count);
        }
    }


    public static class DoubleTuple extends Tuple2<Integer, Integer> {
        public DoubleTuple(int f0, int f1) {
            this.f0 = f0;
            this.f1 = f1;
        }
    }

}
