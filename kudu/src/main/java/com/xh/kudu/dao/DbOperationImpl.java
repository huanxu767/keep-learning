package com.xh.kudu.dao;

import com.xh.kudu.pojo.DbConfig;
import com.xh.kudu.pojo.DbSource;
import com.xh.kudu.pojo.GlobalConfig;
import com.xh.kudu.pojo.TransmissionTableConfig;
import com.xh.kudu.utils.JdbcUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.xh.kudu.utils.JdbcUtil.close;

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
                "where db.DB_ID = tab.DB_ID and db.name = ? order by tab.tbl_name ";
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
            close(rs,ps,con);
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
            close(rs,ps,con);
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
            close(rs,ps,con);
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
                String sql = "select lower(col.COLUMN_NAME) name" +
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
            close(rs,ps,con);
        }
        return resultMap;
    }


    /**
     *查询mysql库中表的列 批量
     * @param dbConfig
     * @param tableList
     * @return
     */
    public Map<String,List<String>> queryMysqlColumns(DbConfig dbConfig, List<String> tableList,boolean ignoreTextBlob) throws SQLException{
        Map<String,List<String>> resultMap = new HashMap<>();
        // 1.2 获取数据库链接
        Connection con = JdbcUtil.getConnection(dbConfig);
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {

            for(String tableName:tableList){
                List<String> list = new ArrayList<>();

                // 1.3 查询表结构
                String sql = "select LOWER(column_name) name" +
                        "        from information_schema.columns " +
                        "        where table_schema = ? and table_name = ? ";
                if(ignoreTextBlob){
                    sql += " and data_type not like '%text%' and data_type not like '%blob%' ";
                }
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
            close(rs,ps,con);
        }
        return resultMap;
    }

    @Override
    public List<String> queryMysqlTables(DbConfig dbConfig) throws SQLException {
        List<String> tables = new ArrayList<>();
        // 1.2 获取数据库链接
        Connection con = JdbcUtil.getConnection(dbConfig);
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            // 1.3 查询表结构
            String sql = "select table_name name from information_schema.tables where table_schema = ? order by table_name ";
            ps = con.prepareStatement(sql);
            ps.setString(1,dbConfig.getDatabase());
            rs = ps.executeQuery();
            while (rs.next()){
                tables.add(rs.getString("name"));
            }
        }finally {
            close(rs,ps,con);
        }
        return tables;
    }

    @Override
    public String queryPk(DbConfig dbConfig,String tableName) throws SQLException {
        String pk = null;
        String sql = "SELECT LOWER(column_name) pk FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` " +
                " WHERE table_schema = ? and table_name= ?  AND constraint_name='PRIMARY' ";
        Connection con = JdbcUtil.getConnection(dbConfig);
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement(sql);
            ps.setString(1,dbConfig.getDatabase());
            ps.setString(2,tableName);
            rs = ps.executeQuery();
            while (rs.next()){
                pk = rs.getString("pk");
            }
        }finally {
            close(rs,ps,con);
        }
        return pk;
    }

    @Override
    public Map<String, String> queryPk(DbConfig dbConfig, List<String> tableList) throws SQLException {
        Map<String, String> map = new HashMap<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection con = null;
        try {
            for(String tableName:tableList){
                String pk = null;
                String sql = "SELECT LOWER(column_name) pk FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` " +
                        " WHERE table_schema = ? and table_name= ?  AND constraint_name='PRIMARY'";
                con = JdbcUtil.getConnection(dbConfig);
                ps = con.prepareStatement(sql);
                ps.setString(1,dbConfig.getDatabase());
                ps.setString(2,tableName);
                rs = ps.executeQuery();
                while (rs.next()){
                    pk = rs.getString("pk");
                }
                if(StringUtils.isNotBlank(pk)){
                    map.put(tableName,pk);
                }
            }
        }finally {
            close(rs,ps,con);
        }
        return map;
    }

    /**
     * 取已配置的表
     * @param dbKey
     * @return
     */
    @Override
    public List<TransmissionTableConfig> queryTransmissionTable(String dbKey) throws SQLException {

        List<TransmissionTableConfig> list = new ArrayList<>();
        String sql = "select * from rt_transmission_table_config order by `table` ";
        Connection con = JdbcUtil.getConnection(DbSource.getDbConfig(GlobalConfig.CANAL_DB));
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()){
                TransmissionTableConfig tableConfig = TransmissionTableConfig.
                        builder().database(rs.getString("database"))
                        .table(rs.getString("table"))
                        .transmissionFlag(rs.getBoolean("transmission_flag"))
                        .hasTransmission(rs.getBoolean("has_transmission"))
                        .structuralDifferences(rs.getString("structural_differences"))
                        .build();
                list.add(tableConfig);
            }
        }finally {
            close(rs,ps,con);
        }
        return list;
    }

    @Override
    public List<TransmissionTableConfig> queryUnTransmissionTable(String dbKey) throws SQLException{
        List<TransmissionTableConfig> list = new ArrayList<>();
        String sql = "select * from rt_transmission_table_config where transmission_flag = 1 and has_transmission = 0 and `database` = ? order by `table` ";
        Connection con = JdbcUtil.getConnection(DbSource.getDbConfig(GlobalConfig.CANAL_DB));
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement(sql);
            ps.setString(1,dbKey);
            rs = ps.executeQuery();
            while (rs.next()){
                TransmissionTableConfig tableConfig = TransmissionTableConfig.
                        builder().database(rs.getString("database"))
                        .table(rs.getString("table"))
                        .transmissionFlag(rs.getBoolean("transmission_flag"))
                        .hasTransmission(rs.getBoolean("has_transmission"))
                        .structuralDifferences(rs.getString("structural_differences"))
                        .build();
                list.add(tableConfig);
            }
        }finally {
            close(rs,ps,con);
        }
        return list;
    }

    @Override
    public int insertTableConfig(String dbKey, List<String> tables) throws SQLException {
        String sql = "insert into rt_transmission_table_config(`database`,`table`) values (?,?) ";
        if(tables.size() == 0 ){
            return 0;
        }
        Connection con = null;
        PreparedStatement ps = null;
        int i = 0;
        try {
            con = JdbcUtil.getConnection(DbSource.getDbConfig(GlobalConfig.CANAL_DB));
            for (String table:tables){
                ps = con.prepareStatement(sql);
                ps.setString(1,dbKey);
                ps.setString(2,table);
                ps.executeUpdate();
            }
            i++;
        }finally {
            close(ps,con);
        }
        return i;
    }

    @Override
    public int updateTableConfig(String dbKey, String table, int hasTransmission, String structuralDifferences) throws SQLException {
        String sql = "update rt_transmission_table_config set has_transmission = ? , " +
                " structural_differences = ? where `database` = ? and `table` = ? ";
        PreparedStatement ps = null;
        Connection con = null;
        int i = 0;
        try {
            con = JdbcUtil.getConnection(DbSource.getDbConfig(GlobalConfig.CANAL_DB));
            ps = con.prepareStatement(sql);
            ps.setInt(1,hasTransmission);
            ps.setString(2,structuralDifferences);
            ps.setString(3,dbKey);
            ps.setString(4,table);
            i = ps.executeUpdate();
        }finally {
            close(ps,con);
        }
        return i;
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
        String sql = "select LOWER(column_name) name" +
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
            close(rs,ps,con);
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
