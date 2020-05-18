package com.xh.flink;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.xh.flink.pojo.Foo;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.LoggerFactory;

public class lambdaTest {


    public static void main(String[] args) throws Exception {

        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.ERROR);


//        t1();
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
//        fliterTest();
//        distinctTest();
//        unionTest();
//        innerJoinTest();
        leftOutJoinTest();
    }



    private static void mapPartitionTest() throws Exception {
//        mapPartition：是一个分区一个分区拿出来的 好处就是以后我们操作完数据了需要存储到mysql中，
//        这样做的好处就是几个分区拿几个连接，如果用map的话，就是多少条数据拿多少个mysql的连接
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Long> dataSet = env.generateSequence(1,20).setParallelism(2);
        dataSet.mapPartition(new MyMapPartitionFunction()).print();
    }

    private static void distinctTest() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer,String>> dataSet = env.fromElements(Tuple2.of(1,"a"),Tuple2.of(2,"b"),Tuple2.of(2,"c"));
        dataSet.distinct(0).print();
    }

    private static void unionTest() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer,String>> dataSet1 = env.fromElements(Tuple2.of(1,"a"),Tuple2.of(2,"b"),Tuple2.of(3,"c"));
        DataSet<Tuple2<Integer,String>> dataSet2 = env.fromElements(Tuple2.of(11,"a"),Tuple2.of(22,"b"),Tuple2.of(33,"c"));
        DataSet<Tuple2<Integer,String>> dataSet3 = env.fromElements(Tuple2.of(111,"a"),Tuple2.of(222,"b"),Tuple2.of(333,"c"));
        DataSet<Tuple2<Integer,String>> unionDataSet = dataSet1.union(dataSet2).union(dataSet3);
        unionDataSet.print();
    }

    private static void innerJoinTest() throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer,String>> dataSet1 = env.fromElements(Tuple2.of(1,"a"),Tuple2.of(2,"b"),Tuple2.of(3,"c"));
        DataSet<Tuple2<Integer,String>> dataSet2 = env.fromElements(Tuple2.of(1,"aa"),Tuple2.of(2,"bb"),Tuple2.of(3,"cc"),Tuple2.of(4,"dd"));
        DataSet<Tuple2<Tuple2<Integer,String>,Tuple2<Integer,String>>> dataSet = dataSet1.join(dataSet2).where(0).equalTo(0);
        dataSet.print();
        System.out.println("------MyJoinFunction------");
        DataSet<Foo> fooDataSet = dataSet1.join(dataSet2).where(0).equalTo(0).with(new MyJoinFunction());
        fooDataSet.print();
    }

    private static void leftOutJoinTest() throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer,String>> dataSet1 = env.fromElements(Tuple2.of(1,"a"),Tuple2.of(22,"b"),Tuple2.of(3,"c"));
        DataSet<Tuple2<Integer,String>> dataSet2 = env.fromElements(Tuple2.of(1,"aa"),Tuple2.of(2,"bb"),Tuple2.of(3,"cc"),Tuple2.of(4,"dd"));
        // 右链接 与 全联接 相似
        DataSet<Foo> leftOutJoinDataSet = dataSet1.leftOuterJoin(dataSet2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Foo>() {
            @Override
            public Foo join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {

                Foo foo = new Foo();
                foo.setId(first.f0);
                foo.setName(first.f1);
                if(second != null){
                    foo.setOther(second.f1);
                }
                return foo;
            }
        });
        leftOutJoinDataSet.print();
    }


    public static void fliterTest() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Long> dataSet = env.generateSequence(1,20);
        dataSet.filter(i -> i%5==0).print();

    }

    /**
     * one to one
     * @throws Exception
     */
    private static void mapTest() throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1,2,3).map(i -> i * i).print();
    }

    /**
     * one to many
     * @throws Exception
     */
    private static void flatMapTest() throws Exception{
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

    /**
     * 用类指明
     * @throws Exception
     */
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


    public static class MyJoinFunction implements JoinFunction<Tuple2<Integer,String>,Tuple2<Integer,String>, Foo>{

        @Override
        public Foo join(Tuple2<Integer, String> integerStringTuple2, Tuple2<Integer, String> integerStringTuple22) throws Exception {
            Foo foo = new Foo();
            foo.setId(integerStringTuple2.f0);
            foo.setName(integerStringTuple2.f1);
            foo.setOther(integerStringTuple22.f1);
            return foo;
        }
    }

    public static class MyMapPartitionFunction implements MapPartitionFunction<Long,Long>{

        @Override
        public void mapPartition(Iterable<Long> iterable, Collector<Long> collector) throws Exception {
            int flag = (int)(Math.random() * 100 + 1);
            long count = 0;
            for (Long value:iterable){
                System.out.println(flag + "value:" + value);
                count++;
            }
            System.out.println(flag + "mapPartition:");

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
