package com.xh.flink.kafka;


import com.xh.flink.creditdemo.config.CreditConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class FlinkSinkToKafka {

    private static final String READ_TOPIC = "student";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", CreditConfig.BOOTSTRAP_SERVERS);
        props.put("zookeeper.connect", CreditConfig.ZOOKEEPER_CONNECT);
        props.put("group.id", "student-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        DataStreamSource<String> student = env.addSource(new FlinkKafkaConsumer<>(
                READ_TOPIC,   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1);
        student.print();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "student-write");

        student.addSink(new FlinkKafkaProducer<>(
                "localhost:9092",
                "student-write",
                new SimpleStringSchema()
        )).name("flink-connectors-kafka")
                .setParallelism(5);

        env.execute("flink learning connectors kafka");
    }
}