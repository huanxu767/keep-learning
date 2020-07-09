package com.xh.kudu.dao;

import com.xh.kudu.utils.DbConfig;

import java.sql.SQLException;
import java.util.List;

public interface DbOperation {

    /**
     * 查询指定表所有列
     * @param tableName
     * @return
     */
    List<String> describeHiveTable(String dbName,String tableName) throws SQLException;

    /**
     * 查询mysql库中表的列
     * @param dbConfig
     * @param tableName
     * @return
     * @throws SQLException
     */
    List<String> queryMysqlColumns(DbConfig dbConfig,String tableName) throws SQLException;


}
