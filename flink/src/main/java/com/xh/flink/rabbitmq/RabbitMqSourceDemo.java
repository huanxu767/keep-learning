package com.xh.flink.rabbitmq;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RabbitMqSourceDemo {

    private static final String READ_TOPIC = "important_table_change";
    private final static String USER_NAME = "admin";
    private final static String PASSWORD = "admin123";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("mq-cluster.hbfintech.com").setPort(5672).setUserName(USER_NAME).setPassword(PASSWORD).setVirtualHost("/")
                .build();


        final DataStream<String> stream = env
                .addSource(new RMQSource<String>(
                        connectionConfig,            // config for the RabbitMQ connection
                        READ_TOPIC,                 // name of the RabbitMQ queue to consume
                        true,                        // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema()))   // deserialization schema to turn messages into Java objects
                .setParallelism(1);              // non-parallel source is only required for exactly-once

        stream.print();

        env.execute("flink learning connectors rabbit-mq");
    }
}