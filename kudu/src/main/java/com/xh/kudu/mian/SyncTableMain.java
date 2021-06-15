package com.xh.kudu.mian;

import com.xh.kudu.dao.DbOperation;
import com.xh.kudu.dao.DbOperationImpl;
import com.xh.kudu.pojo.DbSource;
import com.xh.kudu.pojo.GlobalConfig;
import com.xh.kudu.service.KuduService;
import com.xh.kudu.service.KuduServiceImpl;
import com.xh.kudu.utils.JdbcUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
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

import static com.xh.kudu.utils.JdbcUtil.close;

public class SyncTableMain {


    public static void main(String[] args) throws SQLException, KuduException {

        String bdName = "debit_factoring_pro";
        String table = "debit_factoring_repay_record";
        int partition = 8;

        // 检查hive 与mysql 字段是否一致
        DbOperationImpl dbOperation = new DbOperationImpl();
        List<String> hiveList = dbOperation.describeHiveTable(bdName,table);
        System.out.println(hiveList);
        List<String> columns = dbOperation.queryMysqlColumns(DbSource.getDbConfig(bdName),table,true);
        System.out.println(columns);
        boolean flag = CollectionUtils.isEqualCollection(hiveList,columns);
        System.out.println(flag);
        if(!flag){
            System.out.println("quit,column not the same");
            return;
        }
        //检查 是否有主键
        String pk = checkTablePrimaryKey(bdName,table);
        if(pk == null){
            System.out.println("quit,the table without primary key");
            return;
        }
        System.out.println(pk);
        //检查 是否已经创建kudu表
        String targetTable = "impala::kudu_" + bdName + "." + table;
        createKudu(bdName,table,targetTable,pk,partition);
        updateTableConfig(bdName,table,pk,StringUtils.join(columns,","),targetTable);
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
     * 检查表中是否存在主键并取出主键
     * @param dbKey
     * @param table
     * @return
     * @throws SQLException
     */
    private static String checkTablePrimaryKey(String dbKey,String table) throws SQLException {
        DbOperation dbOperation = new DbOperationImpl();
        String pk = dbOperation.queryPk(DbSource.getDbConfig(dbKey),table);
        return pk;
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

    public static int updateTableConfig(String dbKey, String table, String syncPrimaryKey, String syncColumn, String syncTargetTable) throws SQLException {
        String sql = "update infinity_pro.f_important_table set sync_data_status = ?,sync_primary_key = ? , " +
                " sync_column = ? ,sync_target_table = ? where `db_name` = ? and `table_name` = ? ";
        PreparedStatement ps = null;
        Connection con = null;
        int i = 0;
        try {
            con = JdbcUtil.getConnection(DbSource.getDbConfig(GlobalConfig.SOURCE_BRMS));
            ps = con.prepareStatement(sql);
            ps.setInt(1,1);
            ps.setString(2,syncPrimaryKey);
            ps.setString(3,syncColumn);
            ps.setString(4,syncTargetTable);
            ps.setString(5,dbKey);
            ps.setString(6,table);
            i = ps.executeUpdate();
        }finally {
            close(ps,con);
        }
        return i;
    }
}
