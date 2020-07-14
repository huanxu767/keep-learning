package com.xh.kudu.dao;

import com.xh.kudu.pojo.DbConfig;
import com.xh.kudu.pojo.TransmissionTableConfig;

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
     * @param ignoreTextBlob 是否忽视text blob 字段
     * @return
     */
    Map<String,List<String>> queryMysqlColumns(DbConfig dbConfig, List<String> tableList,boolean ignoreTextBlob) throws SQLException;


    /**
     * 查询mysql库中表结合
     * @param dbConfig
     * @return
     */
    List<String> queryMysqlTables(DbConfig dbConfig) throws SQLException;


    /**
     * 查询主键 单个
     * @param tableName
     * @return
     */
    String queryPk(DbConfig dbConfig,String tableName) throws SQLException;


    /**
     * 查询主键 批量
     * @param dbConfig
     * @param tableList
     * @return
     * @throws SQLException
     */
    Map<String,String> queryPk(DbConfig dbConfig,List<String> tableList) throws SQLException;

    /**
     * 取已配置的表
     * @param dbKey 库名
     * @return
     */
    List<TransmissionTableConfig> queryTransmissionTable(String dbKey) throws SQLException;

    /**
     * 取已配置的表 ，应该同步单尚未同步的表
     * @param dbKey
     * @return
     */
    List<TransmissionTableConfig> queryUnTransmissionTable(String dbKey) throws SQLException;

    /**
     * 插入传输配置表
     * @param dbKey 库名
     * @param tables 表民集合
     * @return
     */
    int insertTableConfig(String dbKey,List<String> tables) throws SQLException;


    /**
     * 更新传输配置表
     * @param dbKey
     * @param tables
     * @param hasTransmission
     * @param structuralDifferences
     * @return
     * @throws SQLException
     */
    int updateTableConfig(String dbKey,String tables,int hasTransmission,String structuralDifferences) throws SQLException;

}
