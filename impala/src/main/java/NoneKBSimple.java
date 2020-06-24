import java.sql.*;

/**
 * package: com.cloudera.impala
 * describe: 该事例主要讲述通过JDBC连接非Kerberos环境下的Impala
 * creat_user: Fayson
 * email: htechinfo@163.com
 * 公众号：Hadoop实操
 * creat_date: 2017/11/21
 * creat_time: 下午7:32
 */
public class NoneKBSimple {

    private static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
    private static String CONNECTION_URL = "jdbc:impala://impal-api-internal.hbfintech.com:21050/brms;auth=noSasl";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("通过JDBC连接非Kerberos环境下的Impala");
        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            connection = DriverManager.getConnection(CONNECTION_URL);
            ps = connection.prepareStatement("select * from impala_kudu.my_external_table");
            rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getInt(1) + "-------" + rs.getString(2));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) rs.close();
                if (ps != null) ps.close();
                if (connection != null) connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}