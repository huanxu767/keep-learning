package com.xh.flink.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class FlinkKafkaConsumerTest {

    private static final String BOOTSTRAP_SERVERS = "dev-dw1:9092,dev-dw2:9092,dev-dw3:9092,dev-dw4:9092,dev-dw5:9092";
    private static final String TOPIC = "flink_test_topic";


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // checkpoint every 5000 msecs

        Properties props = new Properties();

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flink_kafka_test_group");
        // only required for Kafka 0.8
//        properties.setProperty("zookeeper.connect", "localhost:2181");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
//        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<>("flink_test_topic",new SimpleStringSchema(),props));

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(TOPIC,new SimpleStringSchema(),props);
//        consumer.setStartFromEarliest();     // start from the earliest record possible
        consumer.setStartFromLatest();       // start from the latest record
//        consumer.setStartFromTimestamp(...); // start from specified epoch timestamp (milliseconds)
//        consumer.setStartFromGroupOffsets(); // the default behaviour

        DataStream<String> stream = env.addSource(consumer);
        System.out.println(consumer.toString());
        stream.print();
        env.execute();

    }

}
