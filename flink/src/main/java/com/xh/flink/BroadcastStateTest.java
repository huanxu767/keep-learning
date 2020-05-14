package com.xh.flink;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BroadcastStateTest {
    public static void main(String[] args) throws Exception {

        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.ERROR);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //需要广播的数据
        List<Tuple2<String,String>> broadcastData = Stream.of(Tuple2.of("1","wang"),Tuple2.of("2","zha"),Tuple2.of("3","xu")).collect(Collectors.toList());
        //
        DataSet<Tuple2<String,String>> dataSet = env.fromCollection(broadcastData);
        DataSet<HashMap<String,String>>  broadcastDateSet = dataSet.map(new MapFunction<Tuple2<String, String>, HashMap<String,String>>() {
            @Override
            public HashMap<String, String> map(Tuple2<String, String> value) throws Exception {
                HashMap<String, String> map = new HashMap();
                map.put(value.f0,value.f1);
                return map;
            }
        });

        List<Tuple2<String,Integer>> operatorData = Stream.of(Tuple2.of("1",100),Tuple2.of("2",200),Tuple2.of("3",300)).collect(Collectors.toList());
        DataSet<Tuple2<String,Integer>> operatorDataSet = env.fromCollection(operatorData);

        DataSet<String> result = operatorDataSet.map(new RichMapFunction<Tuple2<String, Integer>, String>() {
            private List<HashMap<String,String>> broadCastList = new ArrayList<>();
            private HashMap<String,String> allMap = new HashMap<>();
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.broadCastList = getRuntimeContext().getBroadcastVariable("broadcastXhName");
                for (HashMap map: broadCastList){
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(Tuple2<String, Integer> val) throws Exception {
                String name = allMap.get(val.f0);
                return name + ":" + val.f1;
            }
        }).withBroadcastSet(broadcastDateSet,"broadcastXhName");
        result.print();
    }
}
