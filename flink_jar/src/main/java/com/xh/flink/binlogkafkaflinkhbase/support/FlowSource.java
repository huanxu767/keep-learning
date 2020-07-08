package com.xh.flink.binlogkafkaflinkhbase.support;

import com.xh.flink.config.DbSource;
import com.xh.flink.config.GlobalConfig;
import com.xh.flink.utils.JdbcUtil;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author buildupchao
 * @date 2020/02/03 15:32
 * @since JDK 1.8
 */
public class FlowSource extends RichSourceFunction<Flow> {

    private static final long serialVersionUID = -7580855252299522434L;

    private volatile boolean isRunning = true;

    private String query = "select * from data_bus_flow";


    @Override
    public void run(SourceContext<Flow> ctx) throws Exception {
        // 定时读取数据库的flow表，生成Flow数据
        while (isRunning) {
            Connection connection = null;
            Statement statement = null;
            ResultSet resultSet = null;

            try {
                connection = JdbcUtil.getConnection(DbSource.getDbConfig(GlobalConfig.CANAL_DB));
                statement = connection.createStatement();
                resultSet = statement.executeQuery(query);
                Flow flow = new Flow();

                while (resultSet.next()) {
                    flow.setFlowId(resultSet.getInt("flowId"));
                    flow.setMode(resultSet.getInt("mode"));
                    flow.setDatabaseName(resultSet.getString("databaseName"));
                    flow.setTableName(resultSet.getString("tableName"));
                    flow.setHbaseTable(resultSet.getString("hbaseTable"));
                    flow.setFamily(resultSet.getString("family"));
                    flow.setUppercaseQualifier(resultSet.getBoolean("uppercaseQualifier"));
                    flow.setCommitBatch(resultSet.getInt("commitBatch"));
                    flow.setStatus(resultSet.getInt("status"));
                    flow.setRowKey(resultSet.getString("rowKey"));
                    flow.setExcludeColumns(resultSet.getString("excludeColumns"));
                    ctx.collect(flow);
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
