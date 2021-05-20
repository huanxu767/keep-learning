package com.xh.flink.sink;

import com.xh.flink.pojo.MysqlUser;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlSinkFunction extends RichSinkFunction<MysqlUser> {
    PreparedStatement ps;
    private Connection connection;
    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("-----------1open-----------");
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into flink_sql_sink(id, name, age) values(?, ?, ?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
        System.out.println("-----------1close-----------");
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(MysqlUser value, Context context) throws Exception {
        //组装数据，执行插入操作
        ps.setLong(1, value.getId());
        ps.setString(2, value.getName());
        ps.setInt(3, value.getAge());
        ps.executeUpdate();
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/xdb?useUnicode=true&characterEncoding=UTF-8", "root", "xuhuan");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}
