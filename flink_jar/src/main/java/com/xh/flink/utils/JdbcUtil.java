package com.xh.flink.utils;

import com.xh.flink.binlogkafkaflinkhbase.support.GlobalConfig;

import java.sql.*;

/**
 * @author buildupchao
 * @date 2020/02/03 10:42
 * @since JDK 1.8
 */
public class JdbcUtil {

    private static String url;
    private static String user;
    private static String password;
    private static String driverClass;

    static {
        url = GlobalConfig.DB_URL;
        user = GlobalConfig.USER_NAME;
        password = GlobalConfig.PASSWORD;
        driverClass = GlobalConfig.DRIVER_CLASS;
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() {
        try {
            Connection connection = DriverManager.getConnection(url, user, password);
            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static void close(ResultSet resultSet, Statement statement, Connection connection) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }
}
