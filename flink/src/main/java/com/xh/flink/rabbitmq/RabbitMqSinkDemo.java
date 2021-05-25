package com.xh.flink.rabbitmq;


import com.xh.flink.creditdemo.config.CreditConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.Properties;

public class RabbitMqSinkDemo {

    private static final String READ_TOPIC = "student";
    private final static String USER_NAME = "guest";
    private final static String PASSWORD = "guest";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("localhost").setPort(5672).setUserName(USER_NAME).setPassword(PASSWORD).setVirtualHost("/")
                .build();

        //
        DataStream dataStream = env.fromElements("a","b","c");


        dataStream.addSink(new RMQSink<String>(
                connectionConfig,            // config for the RabbitMQ connection
                "xh_flink_rabbit_mq_test",                 // name of the RabbitMQ queue to send messages to
                new SimpleStringSchema()));

        env.execute("flink learning connectors rabbit-mq");
    }
}