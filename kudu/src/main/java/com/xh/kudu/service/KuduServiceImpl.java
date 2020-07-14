package com.xh.kudu.service;

import com.xh.kudu.dao.DbOperation;
import com.xh.kudu.dao.DbOperationImpl;
import com.xh.kudu.pojo.DbSource;
import com.xh.kudu.pojo.TransmissionTableConfig;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
public class KuduServiceImpl implements KuduService {

    private DbOperation dbOperation = new DbOperationImpl();

    /**
     * 将源数据中全部表配置录入
     *
     * @return
     */
    public boolean initTransmissionTableConfig(String dbKey) throws SQLException {
        //取源数据 所有表
        List<String> mysqlTables = dbOperation.queryMysqlTables(DbSource.getDbConfig(dbKey));
        //取当前配置传输的表
        List<TransmissionTableConfig> dbTables = dbOperation.queryTransmissionTable(dbKey);
        //剔除已配置的表
        List<String> noConfigTables = mysqlTables.stream().filter(
                a -> dbTables.stream().filter(b -> a.equals(b.getTable())).count() == 0
        ).collect(Collectors.toList());
        noConfigTables.stream().forEach(a -> System.out.println(a));
        //初始化 尚未配置的表
        dbOperation.insertTableConfig(dbKey,noConfigTables);
        return false;

    }

    public static void main(String[] args) throws SQLException {
        KuduService kuduService = new KuduServiceImpl();
        kuduService.initTransmissionTableConfig("fintech");
        System.out.println();
    }


}
