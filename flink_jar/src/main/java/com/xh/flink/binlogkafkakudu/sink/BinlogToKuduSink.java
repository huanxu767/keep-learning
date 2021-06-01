package com.xh.flink.binlogkafkakudu.sink;

import com.xh.flink.binlog.Dml;

import com.xh.flink.binlogkafkakudu.config.ImportantTableDO;
import com.xh.flink.config.GlobalConfig;
import com.xh.flink.binlogkafkakudu.config.KuduMapping;
import com.xh.flink.binlogkafkakudu.service.KuduSyncService;
import com.xh.flink.binlogkafkakudu.support.KuduTemplate;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class BinlogToKuduSink extends RichSinkFunction<Tuple2<Dml, KuduMapping>> {
    private static final Logger logger = LoggerFactory.getLogger(BinlogToKuduSink.class);

    private KuduTemplate kuduTemplate;
    private KuduSyncService kuduSyncService;

    @Override
    public void open(Configuration parameters) throws Exception {

        kuduTemplate = new KuduTemplate(GlobalConfig.KUDU_MASTER);
        kuduSyncService = new KuduSyncService(kuduTemplate);
    }

    @Override
    public void close() throws IOException {
        kuduTemplate.closeKuduClient();
    }

    public void invoke(Tuple2<Dml, KuduMapping> tuple2, Context context) throws Exception {
        kuduSyncService.sync(tuple2.f1,tuple2.f0);
    }

}
