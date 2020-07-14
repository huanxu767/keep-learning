package com.xh.kudu.pojo;


import com.xh.kudu.utils.JdbcUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 配置数据源
 */
public class DbSource {

    private static Map<String, DbConfig> configMap = new HashMap<>();

    private static final String DATA_SOURCE = "select * from rt_data_source";

    private DbSource(){

    }
    static {
        System.out.println("init dbSource");
        // 获取
        configMap.put(GlobalConfig.CANAL_DB,DbConfig.builder().database("canal_manager").connectionUrl("jdbc:mysql://dw1:3306/canal_manager?useUnicode=true&characterEncoding=utf8").userName("root").password("xuhuan").build());
        Connection connection = JdbcUtil.getConnection(configMap.get(GlobalConfig.CANAL_DB));
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(DATA_SOURCE);
            rs = ps.executeQuery();
            while (rs.next()){
                DbConfig dbConfig = new DbConfig();
                dbConfig.setDatabaseKey(rs.getString("database_key"));
                dbConfig.setType(rs.getString("type"));
                dbConfig.setDatabase(rs.getString("database"));
                dbConfig.setConnectionUrl(rs.getString("connection_url"));
                dbConfig.setUserName(rs.getString("username"));
                dbConfig.setPassword(rs.getString("password"));
                configMap.put(dbConfig.getDatabaseKey(),dbConfig);
            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {
            JdbcUtil.close(rs,ps,connection);
        }

    }

    public static DbConfig getDbConfig(String key){
        return configMap.get(key);
    }


    public static void main(String[] args) {
        DbConfig dbConfig = DbSource.getDbConfig("brms");
        System.out.println(dbConfig);
    }
}


