package com.xh.flink.binlogkafkakudu.db;

import com.xh.flink.binlogkafkakudu.config.ImportantTableDO;
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
 * 读取重要表配置
 */
public class ImportantTableSource extends RichSourceFunction<ImportantTableDO> {

    private volatile boolean isRunning = true;

    private static final String IMPORTANT_TABLE_SQL =
            "select * from infinity_pro.f_important_table where valid = 1 and sync_data_status = 1";


//
//    private static final String IMPORTANT_TABLE_SQL =
//            "select * from infinity_pro.f_important_table " +
//                    "where valid = 1 and sync_data_status = 1 and db_name = 'fintech' and table_name in ('debit_cfc_loan_detail','member_person_info')";

    @Override
    public void run(SourceContext<ImportantTableDO> ctx) throws Exception {
        // 定时读取数据库的flow表，生成Flow数据
        while (isRunning) {
            Connection connection = null;
            Statement statement = null;
            ResultSet rs = null;

            try {
                connection = JdbcUtil.getConnection(DbSource.getDbConfig(GlobalConfig.INFINITY_DB));
                statement = connection.createStatement();
                rs = statement.executeQuery(IMPORTANT_TABLE_SQL);
                ImportantTableDO importantTableDO = new ImportantTableDO();

                while (rs.next()) {
                    importantTableDO.setId(rs.getLong("id"));
                    importantTableDO.setDbName(rs.getString("db_name"));
                    importantTableDO.setTableName(rs.getString("table_name"));
                    importantTableDO.setSyncTargetTable(rs.getString("sync_target_table"));
                    importantTableDO.setSyncColumn(rs.getString("sync_column"));
                    importantTableDO.setSyncPrimaryKey(rs.getString("sync_primary_key"));
                    importantTableDO.setSyncStatus(rs.getInt("sync_data_status"));
                    //KuduMappingDO 转为KuduMapping
                    ctx.collect(importantTableDO);
                }
            } finally {
                JdbcUtil.close(rs, statement, connection);
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
