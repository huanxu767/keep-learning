package com.xh.flink.binlogkafkaflinkhbase;


import com.xh.flink.binlog.Dml;
import com.xh.flink.binlog.DmlDeserializationSchema;
import com.xh.flink.binlogkafkaflinkhbase.support.*;
import com.xh.flink.config.GlobalConfig;
import lombok.SneakyThrows;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;



import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class BinlogToHBase {


    public static final MapStateDescriptor<String, Flow> flowStateDescriptor = new MapStateDescriptor<String, Flow>(
            "flowBroadCastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<Flow>() {})
    );



    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // checkpoint every 5000 msecs
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //语义保证
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //checkpoint最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);
        //checkpoint 超时时间
        checkpointConfig.setCheckpointTimeout(10000L);
        //启动外部持久化检查点
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        /**
         * restart 策略
         */
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(30, TimeUnit.SECONDS)));

//        env.getConfig().setAutoWatermarkInterval(1000);

//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //定期发送
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "to_hbase");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer(GlobalConfig.TOPIC,new SimpleStringSchema(),props);
//        DataStream<String> dmlStream = env.addSource(consumer);
//
//        consumer.setStartFromGroupOffsets();
//
//        dmlStream.print();


        FlinkKafkaConsumer<Dml> consumer = new FlinkKafkaConsumer("",new DmlDeserializationSchema(),props);
        consumer.setStartFromGroupOffsets();

        DataStream<Dml> dmlStream = env.addSource(consumer);
        dmlStream.print();

        KeyedStream<Dml, String> keyedMessage = dmlStream.keyBy((a) -> a.getDatabase() + a.getTable());
//         读取配置流
        BroadcastStream<Flow> broadcast = env.addSource(new FlowSource()).broadcast(flowStateDescriptor);

        DataStream<Tuple2<Dml, Flow>> connectedStream = keyedMessage.connect(broadcast).process(new DbusProcessFunction()).setParallelism(1);
        connectedStream.addSink(new BinlogToHBaseSink());
        env.execute("hbase increments ");
    }

}
