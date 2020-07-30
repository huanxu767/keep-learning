package com.xh.flink.creditdemo.source;

import com.xh.flink.creditdemo.config.CreditConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;


/**
 * cd /opt/cloudera/parcels/CDH-6.2.1-1.cdh6.2.1.p0.1425774/lib/kafka/bin
 * ./kafka-console-consumer.sh --bootstrap-server dev-dw1:9092,dev-dw2:9092,dev-dw3:9092,dev-dw4:9092,dev-dw5:9092 --from-beginning --topic flink_credit_apply_topic
 * ./kafka-topics.sh --zookeeper dev-dw1:2181,dev-dw1:2181,dev-dw3:2181 --delete --topic flink_credit_apply_topic
 */
public class ProduceOrdersData {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//                // 尝试重启的次数
//                Time.of(10,TimeUnit.SECONDS) // 间隔
//        ));
        DataStreamSource<String> dataStreamSource = env.addSource(new OrdersApplySource()).setParallelism(1);
        FlinkKafkaProducer myProducer = new FlinkKafkaProducer(CreditConfig.BOOTSTRAP_SERVERS, "orders",new SimpleStringSchema());
        myProducer.setWriteTimestampToKafka(true);
        dataStreamSource.addSink(myProducer);
        env.execute();
    }
}
