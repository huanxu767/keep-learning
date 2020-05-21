package com.xh.flink.habse;

import com.xh.flink.pojo.Foo;
import com.xh.flink.pojo.HUser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 * 写入HBase
 * 继承RichSinkFunction重写父类方法
 *
 * 写入hbase时500条flush一次, 批量插入, 使用的是writeBufferSize
 */
public class HBaseRichSinkFunction extends RichSinkFunction<HUser>{
    private static final Logger logger = LoggerFactory.getLogger(HBaseRichSinkFunction.class);
    private static final String TABLE_NAME = "flink_demo";

    private static org.apache.hadoop.conf.Configuration configuration;
    private static Connection connection = null;
    private static BufferedMutator mutator;
    private static int count = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","dev-dw1,dev-dw2,dev-dw3,dev-dw4,dev-dw5");

        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(TABLE_NAME));
        params.writeBufferSize(2 * 1024 * 1024);
        mutator = connection.getBufferedMutator(params);
    }

    @Override
    public void close() throws IOException {
        if (mutator != null) {
            mutator.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(HUser user, Context context) throws Exception {
        String RowKey = user.getRowKey();
        Put put = new Put(RowKey.getBytes());
        put.addColumn("foo".getBytes(), Bytes.toBytes("f1"), Bytes.toBytes(user.getName()));
        mutator.mutate(put);
        //每满5条刷新一下数据
        if (count >= 5){
//            mutator.flush();
            count = 0;
        }
        count = count + 1;
    }
}