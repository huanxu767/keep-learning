import java.sql.*;


public class NoneKBSimple {

    private static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
    private static String CONNECTION_URL = "jdbc:impala://impal-api-internal.hbfintech.com:21050/brms;auth=noSasl";
//    private static String CONNECTION_URL = "jdbc:impala://172.20.0.203:21050/brms;auth=noSasl";


    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        insert();
    }


    private void query(){
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
            JDBCTutorialUtilities.closeConnection(connection);
        }
    }


    /**
     * 通过impala插入kudu
     */
    private static void insert(){
        Connection connection = null;
        PreparedStatement ps = null;
        try{
            connection = DriverManager.getConnection(CONNECTION_URL);
            long t1 = System.currentTimeMillis();
            for (int i = 10000; i < 20000; i++) {
                ps = connection.prepareStatement("insert into impala_kudu.my_external_table VALUES ("+i+",'dazhiruoyu','2017','sexy')");
                int j = ps.executeUpdate();
                System.out.println(i);

            }
            long t2 = System.currentTimeMillis();
            System.out.println( (t2-t1) / 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JDBCTutorialUtilities.closeConnection(connection);
        }
    }
}