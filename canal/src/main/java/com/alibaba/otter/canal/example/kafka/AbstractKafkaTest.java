package com.alibaba.otter.canal.example.kafka;

import com.alibaba.otter.canal.example.BaseCanalClientTest;

/**
 * Kafka 测试基类
 *
 * @author machengyuan @ 2018-6-12
 * @version 1.0.0
 */
public abstract class AbstractKafkaTest extends BaseCanalClientTest {

    public static String  topic     = "canal_kafka_topic";
    public static Integer partition = null;
    public static String  groupId   = "g4";
    public static String  servers   = "dev-dw1:9092,dev-dw2:9092,dev-dw3:9092,dev-dw4:9092,dev-dw5:9092";
    public static String  zkServers = "dev-dw1:2181,dev-dw2:2181,dev-dw3:2181,dev-dw4:2181,dev-dw5:2181";

    public void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
        }
    }
}
