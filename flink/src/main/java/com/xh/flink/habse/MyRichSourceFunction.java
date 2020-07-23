package com.xh.flink.habse;

import com.xh.flink.pojo.Foo;
import com.xh.flink.pojo.HUser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;


public class MyRichSourceFunction extends RichSourceFunction<HUser> {
    private static final Logger logger = LoggerFactory.getLogger(MyRichSourceFunction.class);

    private Connection conn = null;
    private Table table = null;
    // 不加transient 会报错 The object probably contains or references non serializable fields
    private Scan scan = null;


    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open");
        super.open(parameters);

        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","dw1,dw2,dw3");
        conn = ConnectionFactory.createConnection(configuration);
        table = conn.getTable(TableName.valueOf(HUser.TABLE_NAME));
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("4000"));
        scan.setLimit(50);
        this.scan = scan;
    }

    @Override
    public void run(SourceContext<HUser> ctx) throws Exception {
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> iterator = rs.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            String rowKey = Bytes.toString(result.getRow());
            String fn = Bytes.toString(result.getValue("foo".getBytes(), Bytes.toBytes("f1")));
            ctx.collect(HUser.builder().rowKey(rowKey).name(fn).build());
        }

    }

    @Override
    public void cancel() {
        try {
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            logger.error("Close HBase Exception:", e.toString());
        }

        System.out.println("close");
    }
}
