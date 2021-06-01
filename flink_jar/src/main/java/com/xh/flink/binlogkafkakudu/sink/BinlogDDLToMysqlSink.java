package com.xh.flink.binlogkafkakudu.sink;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import com.xh.flink.binlog.Dml;
import com.xh.flink.binlogkafkakudu.config.KuduMapping;
import com.xh.flink.binlogkafkakudu.service.KuduSyncService;
import com.xh.flink.binlogkafkakudu.support.KuduTemplate;
import com.xh.flink.config.DbSource;
import com.xh.flink.config.GlobalConfig;
import com.xh.flink.utils.JdbcUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;


public class BinlogDDLToMysqlSink extends RichSinkFunction<Dml> {

    private static final Logger logger = LoggerFactory.getLogger(BinlogDDLToMysqlSink.class);

    PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = JdbcUtil.getConnection(DbSource.getDbConfig(GlobalConfig.INFINITY_DB));
        String sql = "insert into f_ddl_change_log(binlog_id,table_name,db_name,`type`,`sql`,es,ts) values(?,?, ?, ?, ?, ?, ?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception{
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(Dml dml, Context context) throws Exception {
        //组装数据，执行插入操作
        ps.setLong(1, dml.getId());
        ps.setString(2, dml.getTable());
        ps.setString(3, dml.getDatabase());
        ps.setString(4, dml.getType());
        ps.setString(5,dml.getSql());
        ps.setLong(6, dml.getEs());
        ps.setLong(7, dml.getTs());
        try {
            ps.executeUpdate();
        }catch (MySQLIntegrityConstraintViolationException e){

        }
    }

}
