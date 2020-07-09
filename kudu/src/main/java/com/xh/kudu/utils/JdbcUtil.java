package com.xh.kudu.utils;


import java.sql.*;


public class JdbcUtil {



    public static Connection getConnection(DbConfig dbConfig,String driveClass) {

        try {
            Class.forName(driveClass);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        try {
            Connection connection = DriverManager.getConnection(dbConfig.getConnectionUrl(), dbConfig.getUserName(), dbConfig.getPassword());
            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static Connection getImpalaConnection(DbConfig dbConfig) {
        try {
            Class.forName(GlobalConfig.IMPALA_DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            Connection connection = DriverManager.getConnection(dbConfig.getConnectionUrl());
            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static Connection getConnection(DbConfig dbConfig) {
        return getConnection(dbConfig,GlobalConfig.MYSQL_DRIVER_CLASS);
    }

    public static void close(Statement statement, Connection connection) {
        close(null,statement,connection);
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
