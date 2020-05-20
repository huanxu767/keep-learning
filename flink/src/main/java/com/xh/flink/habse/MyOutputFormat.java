package com.xh.flink.habse;

import com.xh.flink.pojo.HUser;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class MyOutputFormat  implements OutputFormat<HUser>{
    private Connection con = null;
    private Table table = null;

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        System.out.println("open");
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum","dev-dw1,dev-dw2,dev-dw3,dev-dw4,dev-dw5");

        this.con = ConnectionFactory.createConnection(config);
        this.table = con.getTable(TableName.valueOf(HUser.TABLE_NAME));
    }

    @Override
    public void writeRecord(HUser hUser) throws IOException {
        Put put = new Put(Bytes.toBytes(hUser.getRowKey())); // 指定行
        // 参数分别:列族、列、值
        put.addColumn(Bytes.toBytes("foo"), Bytes.toBytes("f1"), Bytes.toBytes(hUser.getName()));
        table.put(put);
    }

    @Override
    public void close() throws IOException {
        System.out.println("close");
        con.close();
    }
}
