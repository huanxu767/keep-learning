package com.xh.flink.habse;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.xh.flink.pojo.HUser;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FlinkHbaseTest {
    public static void main(String[] args) throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.ERROR);

//        write();
//        writeStreamByOutputFormat();
//        writeDataSetByOutputFormat();
        read();

    }

    /**
     * Stream-RichSinkFunction方式写
     * @throws Exception
     */
    static void write() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<HUser> list = IntStream.range(3000,3400).mapToObj(i -> HUser.builder().rowKey(i+"").name(i+"xx").build()).collect(Collectors.toList());

        DataStreamSource dataStream = env.fromCollection(list);
        dataStream.addSink(new HBaseRichSinkFunction());
        env.execute("flinkHbase-Stream-demo");
    }

    /**
     * Stream RichSourceFunction 读HBase
     * @throws Exception
     */
    static void read() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<HUser> dataStreamSource = env.addSource(new MyRichSourceFunction());
        dataStreamSource.print();
        env.execute("read-Stream-demo");
    }

    /**
     * Stream-output方式写
     * @throws Exception
     */
    static void writeStreamByOutputFormat() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<HUser> list = new ArrayList<>();
        IntStream.range(4000,5000).forEach(i->{
            list.add(HUser.builder().rowKey(i+"").name(i + "stream中文").build());
        });
        DataStreamSource dataStream = env.fromCollection(list);
        dataStream.writeUsingOutputFormat(new MyOutputFormat());
        env.execute("flinkHbase-Stream-output-demo");
    }

    /**
     * DataSet-output方式写
     * @throws Exception
     */
    static void writeDataSetByOutputFormat() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<HUser> list = new ArrayList<>();
        IntStream.range(6000,7000).forEach(i->{
            list.add(HUser.builder().rowKey(i+"").name(i + "dataset中文").build());
        });
        DataSource<HUser> dataSet = env.fromCollection(list);
        dataSet.output(new MyOutputFormat());
        env.execute("flinkHbase-Stream-output-demo");
    }

}
