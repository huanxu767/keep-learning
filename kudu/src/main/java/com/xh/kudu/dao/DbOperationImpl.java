package com.xh.kudu.dao;

import com.xh.kudu.utils.DbConfig;
import com.xh.kudu.utils.DbSource;
import com.xh.kudu.utils.GlobalConfig;
import com.xh.kudu.utils.JdbcUtil;
import org.apache.commons.collections4.CollectionUtils;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 操作表数据
 */
public class DbOperationImpl implements DbOperation{

    @Override
    public List<String> queryHiveMetaStoreTables(String dbKey) throws SQLException {
        List<String> list = new ArrayList<>();
        // 1.2 获取数据库链接
        Connection con = JdbcUtil.getConnection(DbSource.getDbConfig(GlobalConfig.SOURCE_METASTORE));
        // 1.3 查询表结构
        String sql = "select tab.tbl_name name " +
                "from DBS db ,TBLS tab " +
                "where db.DB_ID = tab.DB_ID and db.name = ? ";
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement(sql);
            ps.setString(1,dbKey);
            rs = ps.executeQuery();
            while (rs.next()){
                list.add(rs.getString("name"));
            }
        }finally {
            JdbcUtil.close(rs,ps,con);
        }
        return list;
    }

    /**
     * 查询指定表所有列
     * @param tableName
     * @return
     */
    public List<String> describeHiveTable(String dbName,String tableName) throws SQLException{
        List<String> list = new ArrayList<>();
        String sql = "describe " + dbName + "." + tableName;
        Connection con = JdbcUtil.getImpalaConnection(DbSource.getDbConfig(GlobalConfig.SOURCE_IMPALA));
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()){
                list.add(rs.getString("name"));
            }
        }finally {
            JdbcUtil.close(rs,ps,con);
        }
        return list;
    }

    /**
     * 查询HIVE指定表所有列 多表
     * @param dbName
     * @param tableList
     * @return
     */
    @Override
    public Map<String, List<String>> describeHiveTable(String dbName, List<String> tableList) throws SQLException {
        Map<String,List<String>> resultMap = new HashMap<>();

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection con = null;
        try{
            for(String tableName:tableList){
                System.out.println("hive:" + tableName);
                List<String> list = new ArrayList<>();
                String sql = "describe " + dbName + "." + tableName;
                con = JdbcUtil.getImpalaConnection(DbSource.getDbConfig(GlobalConfig.SOURCE_IMPALA));
                ps = con.prepareStatement(sql);
                rs = ps.executeQuery();
                while (rs.next()){
                    list.add(rs.getString("name"));
                }
                resultMap.put(tableName,list);
            }
        }finally {
            JdbcUtil.close(rs,ps,con);
        }
        return resultMap;
    }

    @Override
    public Map<String, List<String>> queryHiveMetaStoreColumns(String hiveDb, List<String> tableList) throws SQLException {
        Map<String,List<String>> resultMap = new HashMap<>();

        Connection con = JdbcUtil.getConnection(DbSource.getDbConfig(GlobalConfig.SOURCE_METASTORE));
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {

            for(String tableName:tableList){
                List<String> list = new ArrayList<>();
                // 1.3 查询表结构
                String sql = "select col.COLUMN_NAME name" +
                        " from DBS db ,TBLS tab,SDS sds,COLUMNS_V2 col " +
                        " where db.DB_ID = tab.DB_ID  " +
                        "   and sds.SD_ID = tab.SD_ID " +
                        "   and sds.CD_ID = col.CD_ID " +
                        "   and db.name = ? and tab.tbl_name = ? " +
                        " order by col.INTEGER_IDX asc;";
                ps = con.prepareStatement(sql);
                ps.setString(1,hiveDb);
                ps.setString(2,tableName);
                rs = ps.executeQuery();
                while (rs.next()){
                    list.add(rs.getString("name"));
                }
                resultMap.put(tableName,list);

            }
        }finally {
            JdbcUtil.close(rs,ps,con);
        }
        return resultMap;
    }


    /**
     *查询mysql库中表的列 批量
     * @param dbConfig
     * @param tableList
     * @return
     */
    public Map<String,List<String>> queryMysqlColumns(DbConfig dbConfig, List<String> tableList) throws SQLException{
        Map<String,List<String>> resultMap = new HashMap<>();
        // 1.2 获取数据库链接
        Connection con = JdbcUtil.getConnection(dbConfig);
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {

            for(String tableName:tableList){
                List<String> list = new ArrayList<>();

                // 1.3 查询表结构
                String sql = "select COLUMN_NAME name" +
                        "        from information_schema.columns " +
                        "        where table_schema = ? and table_name = ?";
                ps = con.prepareStatement(sql);
                ps.setString(1,dbConfig.getDatabase());
                ps.setString(2,tableName);
                rs = ps.executeQuery();
                while (rs.next()){
                    list.add(rs.getString("name"));
                }
                resultMap.put(tableName,list);

            }
        }finally {
            JdbcUtil.close(rs,ps,con);
        }
        return resultMap;
    }

    /**
     * 查询mysql库中表的列 单表
     * @param dbConfig
     * @param tableName
     * @return
     * @throws SQLException
     */
    @Override
    public List<String> queryMysqlColumns(DbConfig dbConfig, String tableName) throws SQLException {
        List<String> list = new ArrayList<>();
        // 1.2 获取数据库链接
        Connection con = JdbcUtil.getConnection(dbConfig);
        // 1.3 查询表结构
        String sql = "select COLUMN_NAME name" +
                "        from information_schema.columns " +
                "        where table_schema = ? and table_name = ?";
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement(sql);
            ps.setString(1,dbConfig.getDatabase());
            ps.setString(2,tableName);
            rs = ps.executeQuery();
            while (rs.next()){
                list.add(rs.getString("name"));
            }
        }finally {
            JdbcUtil.close(rs,ps,con);
        }
        return list;
    }

    public static void main(String[] args) throws SQLException {
        String bdName = "fintech";
        String tableName = "product_repay";
        DbOperationImpl dbOperation = new DbOperationImpl();
        List<String> hiveList = dbOperation.describeHiveTable(bdName,tableName);
        System.out.println(hiveList);

        List<String> mysqlList = dbOperation.queryMysqlColumns(DbSource.getDbConfig(bdName),tableName);
        System.out.println(mysqlList);

        boolean flag = CollectionUtils.isEqualCollection(hiveList,mysqlList);
        System.out.println(flag);



    }

}
