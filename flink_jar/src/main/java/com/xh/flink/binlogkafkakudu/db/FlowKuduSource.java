package com.xh.flink.binlogkafkakudu.db;

import com.xh.flink.binlogkafkakudu.config.KuduMapping;
import com.xh.flink.binlogkafkakudu.config.KuduMappingDO;
import com.xh.flink.config.DbSource;
import com.xh.flink.config.GlobalConfig;
import com.xh.flink.utils.JdbcUtil;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 读取kudu配置表
 */
public class FlowKuduSource extends RichSourceFunction<KuduMapping> {

    private static final long serialVersionUID = -7580855252299522434L;

    private volatile boolean isRunning = true;

    private String query = "select * from rt_kudu_mapping where status = 1";


    @Override
    public void run(SourceContext<KuduMapping> ctx) throws Exception {
        // 定时读取数据库的flow表，生成Flow数据
        while (isRunning) {
            Connection connection = null;
            Statement statement = null;
            ResultSet resultSet = null;

            try {
                connection = JdbcUtil.getConnection(DbSource.getDbConfig(GlobalConfig.CANAL_DB));
                statement = connection.createStatement();
                resultSet = statement.executeQuery(query);
                KuduMappingDO kuduMappingDO = new KuduMappingDO();

                while (resultSet.next()) {
//                    System.out.println(resultSet.getString("table"));
                    kuduMappingDO.setId(resultSet.getLong("id"));
                    kuduMappingDO.setDatabase(resultSet.getString("database"));
                    kuduMappingDO.setTable(resultSet.getString("table"));

                    kuduMappingDO.setTargetTable(resultSet.getString("target_table"));
                    kuduMappingDO.setOriginalKuduTableRelationId(resultSet.getString("original_kudu_table_relation_id"));
                    kuduMappingDO.setOriginalTableColumn(resultSet.getString("original_table_column"));
                    //KuduMappingDO 转为KuduMapping
                    ctx.collect(new KuduMapping(kuduMappingDO));
                }
            } finally {
                JdbcUtil.close(resultSet, statement, connection);
            }
            // 隔一段时间读取，可以使更新的配置生效
            Thread.sleep(60 * 1000L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
