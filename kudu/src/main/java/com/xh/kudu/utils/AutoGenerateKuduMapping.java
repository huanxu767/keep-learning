package com.xh.kudu.utils;


import com.xh.kudu.dao.DbOperation;
import com.xh.kudu.dao.DbOperationImpl;
import com.xh.kudu.pojo.DbConfig;
import com.xh.kudu.pojo.DbSource;
import com.xh.kudu.pojo.GlobalConfig;
import com.xh.kudu.pojo.TransmissionTableConfig;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 自动生成KuduMapping
 * 1 指定数据源 库 表
 * 2 组装KuduMapping
 * 3 插入KuduMapping 表
 */
public class AutoGenerateKuduMapping {

    /**
     * 是否忽视大字段
     * 有 4 种 TEXT 类型：TINYTEXT、TEXT、MEDIUMTEXT 和 LONGTEXT。对应的这 4 种 BLOB 类型
     */
    private static boolean IGNORE_TEXT_BLOB = true;

    /**
     *
     * 初始化整个库中的表
     * 以hive库表为参考
     * 绝大部分情况，mysql表很少删除表、字段，但是会频繁新增表，故绝大部分情况 hive 表中字段 <= mysql表字段，hive中表 <= mysql表
     *
     * @param dbKey
     */
    public static void initDb(String dbKey) throws SQLException, KuduException {

        DbOperation dbOperation = new DbOperationImpl();

        //1 查询配置表 rt_transmission_table_config 应同步但尚未同步的
        List<TransmissionTableConfig> transmissionTableConfigs = dbOperation.queryUnTransmissionTable(dbKey);
        List<String> transmissionTableConfigList = transmissionTableConfigs.stream().map(a -> a.getTable()).collect(Collectors.toList());;

        //2 检查 这些表 hive中是否存在
        //2.1 获取hive 指定库所有表
        List<String> hiveTableNameList = dbOperation.queryHiveMetaStoreTables(dbKey);
        System.out.println(hiveTableNameList);
        //2.2 tableNameList 是否包含 transmissionTableConfigs
        List<String> transmissionTables = transmissionTableConfigs.stream().map(a -> a.getTable()).collect(Collectors.toList());
        System.out.println(transmissionTables);

        List<String> notInHiveTables = transmissionTableConfigList.stream().filter(s -> !hiveTableNameList.contains(s)).collect(Collectors.toList());
        if(!notInHiveTables.isEmpty()){
            System.err.println("quit with notInHiveTables:" + notInHiveTables);
            return;
        }

        //3 检查 这些表 mysql 与 hive中字段是否一致
        boolean flag = validTableColumn(dbKey,transmissionTableConfigList);
        System.out.println("valid table columns :" + flag);
        if(!flag){
            System.err.println("quit,because the field is inconsistent");
            return;
        }

        //3 检查 没有主键的表
        Map<String,String> tablePrimaryKey = checkTablePrimaryKey(dbKey,transmissionTableConfigList);
        if(tablePrimaryKey == null){
            System.out.println("quit,the table without primary key");
            return;
        }

        // 检查完毕 ，开始同步
        for (String tableName:transmissionTableConfigList){
            //查询table主键
            transformTable(dbKey,tableName,tablePrimaryKey.get(tableName));
        }

    }

    /**
     * 检查表中是否存在主键并取出主键
     * @param dbKey
     * @param tableNameList
     * @return
     * @throws SQLException
     */
    private static Map<String,String> checkTablePrimaryKey(String dbKey,List<String> tableNameList) throws SQLException {
        Map<String,String> map;
        DbOperation dbOperation = new DbOperationImpl();
        List<String> noPkTables = new ArrayList<>();
        boolean flag = true;
        map = dbOperation.queryPk(DbSource.getDbConfig(dbKey),tableNameList);

        for (String tableName:tableNameList){
            if(StringUtils.isEmpty(map.get(tableName))) {
                flag = false;
                System.err.println(tableName + "无主键");
                noPkTables.add(tableName);
            }
        }
        return flag?map:null;
    }
    /**
     * 批量校验 mysql中表 与 hive 中表 字段是否一致
     * tableNameList移除结构不一致的表
     * @param  dbName
     * @param tableNameList
     */
    private static boolean validTableColumn(String dbName,List<String> tableNameList) throws SQLException {
        boolean flag = true;
        //hive中本不应该存在的
        List<String> hiveShouldNotExistedTables = new ArrayList<>();
        //存在但列不一致的
        List<String> columnsNotConsistentTables = new ArrayList<>();

        DbOperation dbOperation = new DbOperationImpl();
        // 获取hive中表字段
        Map<String,List<String>> hiveMap = dbOperation.queryHiveMetaStoreColumns(dbName,tableNameList);
        System.out.println(hiveMap);

        //获取mysql表字段
        Map<String,List<String>> mysqlColumnsMap = dbOperation.queryMysqlColumns(DbSource.getDbConfig(dbName),tableNameList,IGNORE_TEXT_BLOB);
        System.out.println(mysqlColumnsMap);


        for (String tableName:tableNameList){
            List<String> hiveColumns= hiveMap.get(tableName);
            List<String> mysqlColumns= mysqlColumnsMap.get(tableName);
            boolean rowFlag = CollectionUtils.isEqualCollection(hiveColumns,mysqlColumns);
            if(!rowFlag){
                flag = false;
                if(mysqlColumns == null || mysqlColumns.size() == 0){
                    hiveShouldNotExistedTables.add(tableName);
                }else{
                    columnsNotConsistentTables.add(tableName);
                }
                System.out.println("---------" + tableName + "----------" + rowFlag);
                System.out.println(hiveColumns);
                System.out.println(mysqlColumns);
            }
        }
        System.out.println("hive中本不应该存在的：" + hiveShouldNotExistedTables);
        System.out.println("存在但列不一致的：");
        columnsNotConsistentTables.stream().forEach(a -> System.out.println("'"+a+"',"));
        return flag;
    }

    public static void transformTable(String dbKey,String table,String pk) throws SQLException {

        DbOperation dbOperation = new DbOperationImpl();


        String targetTable = "impala::kudu_" + dbKey + "." + table;
        // 验证表是否已经配置
        boolean existFlag = isExist(targetTable);
        if(existFlag){
            System.err.println("该表已配置：" + targetTable);
            //存在
            return;
        }

        String columns = getColumns(dbKey,table);
        if (StringUtils.isEmpty(columns)){
            System.err.println("不存在该表" + dbKey + " " + table);
        }
        // 2 组装插入数据
        int updateSqlResult = configKuduMapping(dbKey,table,columns,targetTable);
        System.out.println(targetTable + " " + updateSqlResult);

        // 3 校验创建并初始化kudu

        int hasTransmission = 0;
        String structuralDifferences = null;
        try {
            createKudu(dbKey,table,targetTable,pk,8);
            hasTransmission = 1;
            structuralDifferences = "success";
        } catch (Exception e) {
            hasTransmission = 2;
            structuralDifferences = e.getMessage();
            e.printStackTrace();
        }

        dbOperation.updateTableConfig(dbKey,table,hasTransmission,structuralDifferences);

    }

    /**
     * 通过impala验证、创建、初始化kudu表
     * @param dbKey
     * @param table
     * @param targetTable
     * @return
     */
    private static void createKudu(String dbKey, String table, String targetTable,String primaryKey,int partition) throws KuduException, SQLException {

        //通过kudu判断表是否存在 impala 无法直接判断表是否存在，通过报错比较粗暴
        boolean tableExistsFlag = validKuduTableIsExist(targetTable);
        if (tableExistsFlag){
            System.out.println(targetTable + "表存在");
            return;
        }
        //表不存在，通过impala 创建
        createKuduByImpala(dbKey,table,primaryKey,partition);
    }


    /**
     * 配置kuduMapping表
     * @param db
     * @param table
     * @param columns
     * @param targetTable
     * @return
     */
    private static int configKuduMapping(String db,String table,String columns,String targetTable) {
        int i = 0;
        //默认情况 主键为第一列
        String id = columns.split(",")[0];

        String sql = "INSERT INTO `rt_kudu_mapping` (`database`,`table`,`original_kudu_table_relation_id`,`original_table_column`,`target_table`)" +
                " VALUES (?,?,?,?,?)";
        DbConfig dbConfig = DbSource.getDbConfig(GlobalConfig.CANAL_DB);
        Connection connection = JdbcUtil.getConnection(dbConfig);
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1,db);
            preparedStatement.setString(2,table);
            preparedStatement.setString(3,id);
            preparedStatement.setString(4,columns);
            preparedStatement.setString(5,targetTable);
            System.out.println(preparedStatement.toString());
            i = preparedStatement.executeUpdate();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {
            JdbcUtil.close(preparedStatement,connection);
        }
        return i;
    }

    /**
     * 获取指定库表的列
     * @param targetTableName
     * @return
     */
    private static boolean isExist(String targetTableName){
        boolean existFlag = false;
        DbConfig dbConfig = DbSource.getDbConfig(GlobalConfig.CANAL_DB);
        Connection connection = JdbcUtil.getConnection(dbConfig);
        String sql = "select count(*) from rt_kudu_mapping where target_table = ?";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1,targetTableName);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                existFlag = resultSet.getInt(1) > 0;
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {
            JdbcUtil.close(resultSet,preparedStatement,connection);
        }
        return existFlag;
    }


    /**
     * 获取指定库表的列
     * @param dbKey
     * @param tableName
     * @return
     */
    private static String getColumns(String dbKey,String tableName){
        StringBuilder columns = new StringBuilder();
        // 1.1 获取brms库链接
        DbConfig dbConfig = DbSource.getDbConfig(dbKey);
        // 1.2 获取数据库链接
        Connection connection = JdbcUtil.getConnection(dbConfig);
        // 1.3 查询表结构
        String sql = "select CONCAT(COLUMN_NAME,':',COLUMN_NAME) column_str" +
                "        from information_schema.columns " +
                "        where table_schema = ? and table_name = ?";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1,dbConfig.getDatabase());
            preparedStatement.setString(2,tableName);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                columns.append(resultSet.getString(1) + ",");
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {
            JdbcUtil.close(resultSet,preparedStatement,connection);
        }
        return columns.substring(0,columns.length()-1);
    }

    /**
     * true 存在
     * @param targetTable
     * @return
     */
    private static boolean validKuduTableIsExist(String targetTable) throws KuduException {
        KuduClient client = new KuduClient.KuduClientBuilder(GlobalConfig.KUDU_MASTER).build();
        boolean tableExistsFlag = false;
        try {
            tableExistsFlag = client.tableExists(targetTable);
        }finally {
            try {
                client.shutdown();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
        return tableExistsFlag;
    }

    /**
     * 通过impala创建kudu表
     * @param dbKey
     * @param table
     * @param primaryKey
     * @param partition
     */
    private static boolean createKuduByImpala(String dbKey,String table,String primaryKey,int partition) throws SQLException{
        boolean flag = false;
        String relatedKuduDb = "kudu_" + dbKey;
        String sql = "create table " + relatedKuduDb +"." + table + " " +
                "   PRIMARY KEY ("+primaryKey+") " +
                "   PARTITION BY HASH("+primaryKey+") PARTITIONS " + partition + " STORED AS KUDU" +
                "   AS SELECT * FROM " + dbKey+"."+table;
        System.out.println(sql);
        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            connection = JdbcUtil.getImpalaConnection(DbSource.getDbConfig(GlobalConfig.SOURCE_IMPALA));
            ps = connection.prepareStatement(sql);
            flag = ps.execute();
        } finally {
            JdbcUtil.close(rs,ps,connection);
        }
        return flag;
    }

}
