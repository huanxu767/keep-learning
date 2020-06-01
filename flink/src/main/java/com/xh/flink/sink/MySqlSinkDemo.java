package com.xh.flink.sink;

import com.xh.flink.pojo.MysqlUser;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MySqlSinkDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<MysqlUser> list = IntStream.range(1,100).mapToObj(i -> MysqlUser.builder().id(new Long(i)).age(i+1).name(i+"xiaoming").build()).collect(Collectors.toList());

        DataStream<MysqlUser> dataStream = env.fromCollection(list);
        dataStream.addSink(new MysqlSinkFunction());
        env.execute();
    }
}
