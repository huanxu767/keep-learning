package com.xh.flink.binlogkafkaflinkhbase;

import com.xh.flink.binlogkafkaflinkhbase.support.Dml;
import com.xh.flink.binlogkafkaflinkhbase.support.Flow;
import com.xh.flink.binlogkafkaflinkhbase.support.GlobalConfig;
import com.xh.flink.binlogkafkaflinkhbase.support.HbaseTemplate;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class BinlogToHBaseSink extends RichSinkFunction<Tuple2<Dml, Flow>> {
    private static final Logger logger = LoggerFactory.getLogger(BinlogToHBaseSink.class);

    private HbaseTemplate hbaseTemplate;
    private HbaseSyncService hbaseSyncService;

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", GlobalConfig.ZOOKEEPER);
        hbaseTemplate = new HbaseTemplate(configuration);
        hbaseSyncService = new HbaseSyncService(hbaseTemplate);
    }

    @Override
    public void close() throws IOException {
        hbaseTemplate.close();
    }

    /**
     * dataSourceKey: defaultDS            # 对应application.yml中的datasourceConfigs下的配置
     * destination: example                # 对应tcp模式下的canal instance或者MQ模式下的topic
     * groupId:                            # 对应MQ模式下的groupId, 只会同步对应groupId的数据
     * hbaseMapping:                       # mysql--HBase的单表映射配置
     *   mode: STRING                      # HBase中的存储类型, 默认统一存为String, 可选: #PHOENIX  #NATIVE   #STRING
     *                                     # NATIVE: 以java类型为主, PHOENIX: 将类型转换为Phoenix对应的类型
     *   destination: example              # 对应 canal destination/MQ topic 名称
     *   database: mytest                  # 数据库名/schema名
     *   table: person                     # 表名
     *   hbaseTable: MYTEST.PERSON         # HBase表名
     *   family: CF                        # 默认统一Column Family名称
     *   uppercaseQualifier: true          # 字段名转大写, 默认为true
     *   commitBatch: 3000                 # 批量提交的大小, ETL中用到
     *   #rowKey: id,type                  # 复合字段rowKey不能和columns中的rowKey并存
     *                                     # 复合rowKey会以 '|' 分隔
     *   columns:                          # 字段映射, 如果不配置将自动映射所有字段,
     *                                     # 并取第一个字段为rowKey, HBase字段名以mysql字段名为主
     *     id: ROWKE
     *     name: CF:NAME
     *     email: EMAIL                    # 如果column family为默认CF, 则可以省略
     *     type:                           # 如果HBase字段和mysql字段名一致, 则可以省略
     *     c_time:
     *     birthday:
     */

    public void invoke(Tuple2<Dml, Flow> tuple2, Context context) throws Exception {
        hbaseSyncService.sync(tuple2.f1,tuple2.f0);
    }

}
