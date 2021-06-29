package com.xh.flink.binlogkafkakudu;


import com.xh.flink.binlog.Dml;
import com.xh.flink.binlog.DmlDeserializationSchema;
import com.xh.flink.binlogkafkakudu.config.ImportantTableDO;
import com.xh.flink.binlogkafkakudu.db.ImportantTableSource;
import com.xh.flink.binlogkafkakudu.function.GroupByTableWindowFunction;
import com.xh.flink.binlogkafkakudu.function.ImportantTableProcessFunction;
import com.xh.flink.binlogkafkakudu.function.KuduMappingProcessFunction;
import com.xh.flink.binlogkafkakudu.function.TopKWindowFunction;
import com.xh.flink.binlogkafkakudu.sink.BinlogDDLToMysqlSink;
import com.xh.flink.binlogkafkakudu.sink.BinlogToKuduSink;
import com.xh.flink.config.GlobalConfig;
import com.xh.flink.binlogkafkakudu.config.KuduMapping;
import com.xh.flink.utils.TimeUtils;
import lombok.SneakyThrows;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class BinlogToKudu {

    private static final Logger log = LoggerFactory.getLogger(BinlogToKudu.class);

    public static final MapStateDescriptor<String, ImportantTableDO> importantTableFlowStateDescriptor = new MapStateDescriptor<String, ImportantTableDO>(
            "flowImportantTableBroadCastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<ImportantTableDO>() {})
    );

    private static long i = 0;

    /**
     * 主方法
     * 参数1 从第几天开始
     *
         canal_binlog_dataware_pro_topic 暂不同步
     --beginTime '2021-06-25 00:00:00' --topic canal_binlog_brms_topic,canal_binlog_brms_model_topic --jobName brms_brms_model_cdc
     --beginTime '2021-06-24 00:00:00' --topic canal_binlog_alchemy_pro_topic,canal_binlog_debit_factoring_pro_topic,canal_binlog_nbcb_pro_topic,canal_binlog_shanghang_pro_topic --jobName alchemy_debitfactoring_nbcb_shanghang_cdc
     --beginTime '2021-06-24 00:00:00' --topic canal_binlog_lebei_pro_topic,canal_binlog_pledgeapi_pro_topic,canal_binlog_pledge_pro_topic --jobName leibei_pledgeapi_pledge_cdc
     --beginTime '2021-06-24 00:00:00' --topic canal_binlog_premium_pro_topic,canal_binlog_sxb_pro_topic,canal_binlog_everestcenter_pro_topic --jobName premium_sxb_everestcenter_cdc
     --beginTime '2021-06-25 12:00:00' --topic canal_binlog_fintech_topic,canal_binlog_debitceb_pro_topic,canal_binlog_hb_nuggets_topic --jobName fintech_debitceb_nuggets_cdc
     * @param args
     */
    @SneakyThrows
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String beginTime = parameterTool.get("beginTime",null);
        String[] topic = parameterTool.get("topic","canal_binlog_brms_topic").split(",");
        String jobName = parameterTool.get("jobName","kong");
        System.out.println("beginTime="+beginTime +";topics="+Arrays.toString(topic) +";jobName="+jobName);
        StreamExecutionEnvironment env = initEnv();


        FlinkKafkaConsumer<Dml> consumer = readFromKafka(beginTime,topic);
        DataStream<Dml> d = env.addSource(consumer);
//        // 拆分binlog流 分为DDL & DML binlog Stream
        KeyedStream<Dml, Boolean> binlogStream = d.keyBy(a -> a.getIsDdl());
        //广播 重要表配置 流
        BroadcastStream<ImportantTableDO> importantTableDOBroadcastStream = env.addSource(new ImportantTableSource()).broadcast(importantTableFlowStateDescriptor);

        DataStream<Dml> ddlBinlogStream = binlogStream.filter( a -> a.getIsDdl() == true).setParallelism(1);
        KeyedStream<Dml, String> ddlKeyedMessage = ddlBinlogStream.keyBy((a) -> a.getDatabase() + a.getTable());
        //重要库变化流 融合 重要表配置流 删选出 重要表变化流
        DataStream<String> importantTableChangeStream = ddlKeyedMessage.connect(importantTableDOBroadcastStream).process(new ImportantTableProcessFunction());
        sinkDDL(importantTableChangeStream,ddlBinlogStream);

//      dml处理
        DataStream<Dml> dmlBinlogStream = binlogStream.filter( a -> a.getIsDdl() == false).setParallelism(1);
        KeyedStream<Dml, String> dmlKeyedMessageStream = dmlBinlogStream.keyBy((a) -> a.getDatabase() + a.getTable());

//         读取配置流
        DataStream<Tuple2<Dml, KuduMapping>> connectedDmlStream = dmlKeyedMessageStream.connect(importantTableDOBroadcastStream).process(new KuduMappingProcessFunction());
//        DataStream<List<Tuple2<Dml, KuduMapping>>> globalResults =
                connectedDmlStream.keyBy(a -> a.f0.getDatabase() + "." + a.f0.getTable() )
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new GroupByTableWindowFunction()).addSink(new BinlogToKuduSink()).setParallelism(1);
//        globalResults.addSink(new BinlogToKuduSink()).setParallelism(1);
        env.execute(jobName);
    }

    private static void sinkDDL(DataStream<String> importantTableChangeStream, DataStream<Dml> ddlBinlogStream) {
        // 变化流写入rabbitmq
        sinkRabbitMq(importantTableChangeStream);
        //DDL变化记录写入MYSQL
        ddlBinlogStream.addSink(new BinlogDDLToMysqlSink());
    }

    private static void sinkRabbitMq(DataStream<String> importantTableChangeStream) {
        importantTableChangeStream.print("importantTableChangeStream:");
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(GlobalConfig.MQ_URL).setPort(5672).setUserName(GlobalConfig.MQ_USER_NAME)
                .setPassword(GlobalConfig.MQ_PASSWORD)
                .setVirtualHost("/")
                .build();

        importantTableChangeStream.addSink(new RMQSink<String>(
                connectionConfig,
                GlobalConfig.MQ_NOTIFY_TOPIC,
                new SimpleStringSchema()));
    }

    /**
     * 初始化环境
     * @return
     */
    static StreamExecutionEnvironment initEnv(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // checkpoint every 5000 msecs
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //语义保证
//        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //checkpoint最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);
        //checkpoint 超时时间
        checkpointConfig.setCheckpointTimeout(10000L);
        //启动外部持久化检查点
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        /**
         * restart 策略
         */
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(30, TimeUnit.SECONDS)));

        env.getConfig().setAutoWatermarkInterval(1000);
        return env;
    }

    /**
     * 读kafka中binlog
     * @param beginTime
     * @param topicArray
     * @return
     */
    static FlinkKafkaConsumer<Dml> readFromKafka(String beginTime,String[] topicArray) throws ParseException {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "to_kudu");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        List topicList = new ArrayList<String>();
        for (String topic:topicArray) {
            topicList.add(topic);
        }
        FlinkKafkaConsumer<Dml> consumer = new FlinkKafkaConsumer(topicList,new DmlDeserializationSchema(),props);
//        consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());

//        consumer.setStartFromEarliest();     // 尽可能从最早的记录开始
//        consumer.setStartFromLatest();       // 从最新的记录开始
        //指定启动时间当天凌晨 配合sqoop 批量导入
        consumer.setStartFromTimestamp(TimeUtils.getDateStart(beginTime)); // 从指定的时间开始（毫秒）
//        consumer.setStartFromGroupOffsets(); // 默认的方法
        return consumer;
    }

}
