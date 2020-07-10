package com.xh.kudu.dao;

import com.xh.kudu.utils.DbConfig;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface DbOperation {

    /**
     * 查询hive指定库所有表名
     * @param dbKey
     * @return
     */
    List<String> queryHiveMetaStoreTables(String dbKey) throws SQLException;

    /**
     * 查询HIVE指定表所有列 单表
     * 速度很慢，暂时不用了，修改成直接读取hive 元数据，速度快
     * @param dbName
     * @param tableName
     * @return
     */
    @Deprecated
    List<String> describeHiveTable(String dbName,String tableName) throws SQLException;


    /**
     * 查询HIVE指定表所有列 多表
     * 速度很慢，暂时不用了，修改成直接读取hive 元数据，速度快
     * @param dbName
     * @param tableList
     * @return
     */
    @Deprecated
    Map<String,List<String>> describeHiveTable(String dbName,List<String> tableList) throws SQLException;


    /**
     * 查询hive 元数据 表的列 单表
     * @param hiveDb
     * @param tableList
     * @return
     * @throws SQLException
     */
    Map<String,List<String>> queryHiveMetaStoreColumns(String hiveDb, List<String> tableList)  throws SQLException;

    /**
     * 查询mysql库中表的列 单表
     * @param dbConfig
     * @param tableName
     * @return
     * @throws SQLException
     */
    List<String> queryMysqlColumns(DbConfig dbConfig,String tableName) throws SQLException;

    /**
     * 查询mysql库中表的列 多表
     * @param dbConfig
     * @param tableList
     * @return
     */
    Map<String,List<String>> queryMysqlColumns(DbConfig dbConfig, List<String> tableList) throws SQLException;


}
