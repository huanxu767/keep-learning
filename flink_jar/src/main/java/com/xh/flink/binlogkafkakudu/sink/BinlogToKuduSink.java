package com.xh.flink.binlogkafkakudu.sink;

import com.xh.flink.binlog.Dml;

import com.xh.flink.binlogkafkakudu.service.KuduSyncService;
import com.xh.flink.binlogkafkakudu.support.KuduTemplate;
import com.xh.flink.config.GlobalConfig;
import com.xh.flink.binlogkafkakudu.config.KuduMapping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;


public class BinlogToKuduSink extends RichSinkFunction<List<Tuple2<Dml, KuduMapping>>> {
    private static final Logger logger = LoggerFactory.getLogger(BinlogToKuduSink.class);

    private KuduClient kuduClient;
    private KuduSession session;
    private KuduTemplate kuduTemplate;
    private KuduSyncService kuduSyncService;
    private KuduTable kuduTable;
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open");
        kuduClient = new KuduClient.KuduClientBuilder(GlobalConfig.KUDU_MASTER).defaultOperationTimeoutMs(60000)
                .defaultSocketReadTimeoutMs(60000)
                .defaultAdminOperationTimeoutMs(60000)
                .build();

        session = kuduClient.newSession(); // 创建写session,kudu必须通过session写入
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(GlobalConfig.OPERATION_BATCH);

    }

    @Override
    public void close() throws IOException {
        System.out.println("close");
        if (kuduClient != null) {
            try {
                session.close();
                kuduClient.close();
            } catch (Exception e) {
                logger.error("ShutdownHook Close KuduClient Error! error message {}", e.getMessage());
            }
        }
    }

    public void invoke(List<Tuple2<Dml, KuduMapping>> list, Context context) throws Exception {

        String tableName = list.get(0).f1.getTargetTable();
        this.kuduTable = kuduClient.openTable(tableName);
        this.kuduTemplate = new KuduTemplate(kuduClient,session,tableName);
        kuduSyncService = new KuduSyncService(kuduTemplate);

        Long begin = System.currentTimeMillis();
        System.out.println(list.size());
        int uncommit = 0;
//        int j = list.size() > 100?100:list.size();
        for (int i = 0; i < list.size(); i++) {
            Tuple2<Dml, KuduMapping> tuple2 = list.get(i);
            uncommit = uncommit + 1;
            kuduSyncService.sync(tuple2.f1, tuple2.f0);
            if (uncommit > GlobalConfig.OPERATION_BATCH / 3 * 2) {
                System.out.println(list.size() + ":1_flush");
                List<OperationResponse> delete_option = session.flush();
                if (delete_option.size() > 0) {
                    OperationResponse response = delete_option.get(0);
                    if (response.hasRowError()) {
                        logger.error("delete row fail table name is :{} ", tuple2.f0.getTable());
                        logger.error("error list is :{}", response.getRowError().getMessage());
                    }
                }
                uncommit = 0;
            }
        }
        System.out.println(list.size() + ":2_flush");
        List<OperationResponse> delete_option = session.flush();
        if (delete_option.size() > 0) {
            OperationResponse response = delete_option.get(0);
            if (response.hasRowError()) {
                logger.error("error list is :{}", response.getRowError().getMessage());
            }
        }
        Long en = System.currentTimeMillis();
        System.out.println( list.size() +":执行时间" + (en-begin)/1000);
    }

}
