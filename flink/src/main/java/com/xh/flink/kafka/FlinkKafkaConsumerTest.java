package com.xh.flink.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.xh.flink.utils.TimeUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FlinkKafkaConsumerTest {

//    private static final String BOOTSTRAP_SERVERS = "dev-dw1:9092,dev-dw2:9092,dev-dw3:9092,dev-dw4:9092,dev-dw5:9092";
    private static final String BOOTSTRAP_SERVERS = "dw4:9092,dw5:9092,dw6:9092,dw7:9092,dw8:9092";


    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // checkpoint every 5000 msecs

        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flink_kafka_test_group");

//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "transactions");


        // only required for Kafka 0.8
//        properties.setProperty("zookeeper.connect", "localhost:2181");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
//        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<>("flink_test_topic",new SimpleStringSchema(),props));

        List<String> topicList = new ArrayList<>();
//        topicList.add("canal_binlog_dataware_pro_topic");
//        topicList.add("canal_binlog_brms_topic");
//        topicList.add("canal_binlog_hb_nuggets_topic");
//        topicList.add("canal_binlog_alchemy_pro_topic");
//        topicList.add("canal_binlog_debit_factoring_pro_topic");
//        topicList.add("canal_binlog_nbcb_pro_topic");
//        topicList.add("canal_binlog_shanghang_pro_topic");
//        topicList.add("canal_binlog_everestcenter_pro_topic");
//        topicList.add("canal_binlog_lebei_pro_topic");
//        topicList.add("canal_binlog_pledgeapi_pro_topic");
//        topicList.add("canal_binlog_pledge_pro_topic");
//        topicList.add("canal_binlog_premium_pro_topic");
//        topicList.add("canal_binlog_sxb_pro_topic");
//        topicList.add("canal_binlog_debitceb_pro_topic");
//        topicList.add("canal_binlog_fintech_topic");
//        topicList.add("canal_binlog_debitceb_pro_topic");

        topicList.add("canal_binlog_brms_model_topic");


        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topicList,new SimpleStringSchema(),props);
//        consumer.setStartFromEarliest();     // start from the earliest record possible
//        consumer.setStartFromLatest();       // start from the latest record
        consumer.setStartFromTimestamp(TimeUtils.getTodayStart()); // start from specified epoch timestamp (milliseconds)
//        consumer.setStartFromGroupOffsets(); // the default behaviour

        DataStream<String> stream = env.addSource(consumer);
        System.out.println(consumer.toString());
        stream.print();
        env.execute();

    }

}
