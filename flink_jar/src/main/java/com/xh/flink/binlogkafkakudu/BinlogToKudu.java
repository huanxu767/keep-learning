package com.xh.flink.binlogkafkakudu;


import com.xh.flink.binlog.Dml;
import com.xh.flink.binlog.DmlDeserializationSchema;
import com.xh.flink.config.GlobalConfig;
import com.xh.flink.binlogkafkakudu.config.KuduMapping;
import com.xh.flink.binlogkafkakudu.db.FlowKuduSource;
import com.xh.flink.binlogkafkakudu.function.KuduMappingProcessFunction;
import com.xh.flink.binlogkafkakudu.sink.BinlogToKuduSink;
import com.xh.flink.utils.TimeUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class BinlogToKudu {

    private static final Logger log = LoggerFactory.getLogger(BinlogToKudu.class);


    public static final MapStateDescriptor<String, KuduMapping> flowStateDescriptor = new MapStateDescriptor<String, KuduMapping>(
            "flowKuduBroadCastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<KuduMapping>() {})
    );


    @SneakyThrows
    public static void main(String[] args) {

        int beginDay = 0;
        if(args != null && args.length >= 1){
            beginDay = Integer.parseInt(args[0]);
        }

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

        env.getConfig().setAutoWatermarkInterval(1000);

        //定期发送
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "to_kudu");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer<Dml> consumer = new FlinkKafkaConsumer(GlobalConfig.TOPIC,new DmlDeserializationSchema(),props);
//        consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());

//        consumer.setStartFromEarliest();     // 尽可能从最早的记录开始
//        consumer.setStartFromLatest();       // 从最新的记录开始
        //指定启动时间当天凌晨 配合sqoop 批量导入
        consumer.setStartFromTimestamp(TimeUtils.getDateStart(beginDay)); // 从指定的时间开始（毫秒）
//        consumer.setStartFromGroupOffsets(); // 默认的方法
        DataStream<Dml> dmlStream = env.addSource(consumer);
//        dmlStream.print();
//        dmlStream.filter((a) -> (a.getDatabase() + a.getTable()).equals("brmscmpay_credit_apply")).print();
        KeyedStream<Dml, String> keyedMessage = dmlStream.keyBy((a) -> a.getDatabase() + a.getTable());
//        keyedMessage.print();
//         读取配置流
        BroadcastStream<KuduMapping> broadcast = env.addSource(new FlowKuduSource()).broadcast(flowStateDescriptor);

        DataStream<Tuple2<Dml, KuduMapping>> connectedStream = keyedMessage.connect(broadcast).process(new KuduMappingProcessFunction()).setParallelism(1);

        connectedStream.print();
        connectedStream.addSink(new BinlogToKuduSink());
        env.execute("kudu increments ");
    }

}
