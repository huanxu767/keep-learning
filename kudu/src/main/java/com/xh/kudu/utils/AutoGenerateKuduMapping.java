package com.xh.kudu.utils;


import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.springframework.util.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 自动生成KuduMapping
 * 1 指定数据源 库 表
 * 2 组装KuduMapping
 * 3 插入KuduMapping 表
 */
public class AutoGenerateKuduMapping {

    public static void main(String[] args) throws KuduException {
        String dbKey = "brms";

        String[] tables = new String[]{
                "cmpay_borrow_apply","cmpay_borrow_report",
                "cmpay_credit_apply_operator_info","cmpay_credit_apply_user_info",
                "cmpay_credit_apply_bank_info","cmpay_credit_report",

        };
        for (int i = 0; i < tables.length; i++) {
            tranformTable(dbKey,tables[i]);
        }

    }

    public static void tranformTable(String dbKey,String table) throws KuduException {
        String targetTable = "impala::kudu_" + dbKey + "." + table;
        // 验证表是否已经配置
        boolean existFlag = isExist(targetTable);
        if(existFlag){
            System.out.println("该表已配置：" + targetTable);
            //存在
            return;
        }

        String columns = getColumns(dbKey,table);
        if (StringUtils.isEmpty(columns)){
            System.out.println("不存在该表" + dbKey + " " + table);
        }
        // 2 组装插入数据
        int updateSqlResult = configKuduMapping(dbKey,table,columns,targetTable);
        System.out.println(targetTable + " " + updateSqlResult);

        // 3 校验创建并初始化kudu
        createKudu(dbKey,table,targetTable,"id",8);
        // 4 暂停flink、
    }

    /**
     * 通过impala验证、创建、初始化kudu表
     * @param dbKey
     * @param table
     * @param targetTable
     * @return
     */
    private static void createKudu(String dbKey, String table, String targetTable,String primaryKey,int partition) throws KuduException {

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

        String sql = "INSERT INTO `kudu_mapping` (`database`,`table`,`original_kudu_table_relation_id`,`original_table_column`,`target_table`)" +
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
        String sql = "select count(*) from kudu_mapping where target_table = ?";
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
        String columns = null;
        // 1.1 获取brms库链接
        DbConfig dbConfig = DbSource.getDbConfig(dbKey);
        // 1.2 获取数据库链接
        Connection connection = JdbcUtil.getConnection(dbConfig);
        // 1.3 查询表结构
        String sql = "select GROUP_CONCAT(CONCAT(COLUMN_NAME,':',COLUMN_NAME)) column_str" +
                "        from information_schema.columns " +
                "        where table_schema = ? and table_name = ?";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1,dbConfig.getDbName());
            preparedStatement.setString(2,tableName);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                columns = resultSet.getString(1);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {
            JdbcUtil.close(resultSet,preparedStatement,connection);
        }
        return columns;
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
    private static boolean createKuduByImpala(String dbKey,String table,String primaryKey,int partition){
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
            connection = JdbcUtil.getImpalaConnection(GlobalConfig.CONNECTION_URL);
            ps = connection.prepareStatement(sql);
            flag = ps.execute();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(rs,ps,connection);
        }
        return flag;
    }

}
