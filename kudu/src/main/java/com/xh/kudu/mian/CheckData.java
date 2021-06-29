package com.xh.kudu.mian;

import com.xh.kudu.pojo.DbSource;
import com.xh.kudu.pojo.ImportantTableDO;
import com.xh.kudu.utils.JdbcUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
/**
 * 读取重要表配置
 */
public class CheckData{

//     "and db_name in ('fintech','brms','sxb_pro','shanghang_pro','hb_nuggets','lebei_pro'" +
//             ",'nbcb_pro','pledgeapi_pro','pledge_pro','debit_factoring_pro','everestcenter_pro','alchemy_pro','debitceb_pro')  " +
    private static final String IMPORTANT_TABLE_SQL =
                    "select * from infinity_pro.f_important_table " +
                    "where valid = 1 and sync_data_status = 1 " +
                            "and db_name in ('fintech','brms','sxb_pro','shanghang_pro','hb_nuggets','lebei_pro'" +
                            ",'nbcb_pro','pledgeapi_pro','pledge_pro','debit_factoring_pro','everestcenter_pro','alchemy_pro','debitceb_pro')  " +
                    "order by db_name desc ";

//    private static final String IMPORTANT_TABLE_SQL =
//            "select * from infinity_pro.f_important_table " +
//                    "where valid = 1 and sync_data_status = 1 and db_name = 'brms'";
//    table:pledge_pro.unicom_merchant

    public static void main(String[] args) throws Exception {
        List<ImportantTableDO> list = queryImportantTable();
        for (int i = 0; i < list.size(); i++) {
            ImportantTableDO importantTableDO = list.get(i);
            System.out.print("table:"+importantTableDO.getDbName() +"." +importantTableDO.getTableName()+";");
            long maxId = queryMaxId(importantTableDO);
//            long maxId = 6087863l;
            //查看主键类型
            long mysqlTotal = countFromMysql(importantTableDO,maxId);
            Long[] kuduTotal = countFromImpala(importantTableDO,maxId);
            System.out.println(
                    "maxId:" + maxId +","+kuduTotal[1]+";total:"+mysqlTotal +","+kuduTotal[0] +";" +
                    "totalflag:"+(mysqlTotal == kuduTotal[0]) +";" +
                    "maxIdIsSame:" + (maxId == kuduTotal[1]));
        }
    }




    public static List<ImportantTableDO> queryImportantTable() throws Exception {
        // 定时读取数据库的flow表，生成Flow数据
        List<ImportantTableDO> list = new ArrayList<>();
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;

        try {
            connection = JdbcUtil.getConnection(DbSource.getDbConfig("infinity_pro"));
            statement = connection.createStatement();
            rs = statement.executeQuery(IMPORTANT_TABLE_SQL);
            while (rs.next()) {
                ImportantTableDO importantTableDO = new ImportantTableDO();
                importantTableDO.setId(rs.getLong("id"));
                importantTableDO.setDbName(rs.getString("db_name"));
                importantTableDO.setTableName(rs.getString("table_name"));
                importantTableDO.setSyncTargetTable(rs.getString("sync_target_table"));
                importantTableDO.setSyncColumn(rs.getString("sync_column"));
                importantTableDO.setSyncPrimaryKey(rs.getString("sync_primary_key"));
                importantTableDO.setSyncStatus(rs.getInt("sync_data_status"));
                //KuduMappingDO 转为KuduMapping
                list.add(importantTableDO);
            }
        } finally {
            JdbcUtil.close(rs, statement, connection);
        }
        return list;
    }

    public static Long queryMaxId(ImportantTableDO  importantTableDO) throws Exception {
        String maxIdSql = "select max("+importantTableDO.getSyncPrimaryKey()+") max_id from " + importantTableDO.getTableName() ;
        long maxId = 0;
        // 定时读取数据库的flow表，生成Flow数据
        List<ImportantTableDO> list = new ArrayList<>();
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;

        try {
            connection = JdbcUtil.getConnection(DbSource.getDbConfig(importantTableDO.getDbName()));
            statement = connection.createStatement();
            rs = statement.executeQuery(maxIdSql);
            while (rs.next()) {
                maxId = rs.getLong("max_id");
            }
        } finally {
            JdbcUtil.close(rs, statement, connection);
        }
        return maxId;
    }


    public static Long countFromMysql(ImportantTableDO  importantTableDO,long maxId) throws Exception {
        String countSql = "" +
                " select count(*) total_num from "+importantTableDO.getTableName() +
                " where "+importantTableDO.getSyncPrimaryKey()+" <= " + maxId;
        // 定时读取数据库的flow表，生成Flow数据
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        long total = 0;
        try {
            connection = JdbcUtil.getConnection(DbSource.getDbConfig(importantTableDO.getDbName()));
            statement = connection.createStatement();
            rs = statement.executeQuery(countSql);
            while (rs.next()) {
                total = rs.getLong("total_num");
            }
        } finally {
            JdbcUtil.close(rs, statement, connection);
        }
        return total;
    }

    public static Long[] countFromImpala(ImportantTableDO  tab,long maxId){
//        String countSql = "" +
//                " select count(*) total_num,max("+tab.getSyncPrimaryKey()+") max_id " +
//                " from kudu_"+ tab.getDbName() +"." +tab.getTableName() +
//                " where "+tab.getSyncPrimaryKey()+" <=  '" + maxId  +"'";

        String countSql = "" +
                " select count(*) total_num,max("+tab.getSyncPrimaryKey()+") max_id " +
                " from kudu_"+ tab.getDbName() +"." +tab.getTableName() +
                " where "+tab.getSyncPrimaryKey()+" <=  " + maxId  +"";
        // 定时读取数据库的flow表，生成Flow数据
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        long total = 0;
        long calMaxId = 0;
        try {
            connection = JdbcUtil.getConnection(DbSource.getDbConfig("impala"));
            statement = connection.createStatement();
            rs = statement.executeQuery(countSql);
            while (rs.next()) {
                total = rs.getLong("total_num");
                calMaxId = rs.getLong("max_id");
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            JdbcUtil.close(rs, statement, connection);
        }
        Long[] re = {total,calMaxId};
        return re;
    }

}
