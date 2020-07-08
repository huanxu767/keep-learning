import com.xh.flink.binlogkafkaflinkhbase.support.Flow;
import com.xh.flink.config.DbSource;
import com.xh.flink.config.GlobalConfig;
import com.xh.flink.utils.JdbcUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author buildupchao
 * @date 2020/02/03 15:32
 * @since JDK 1.8
 */
public class FlowSourceTest {



    private static String query = "select * from data_bus_flow";

    public static void main(String[] args) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            connection = JdbcUtil.getConnection(DbSource.getDbConfig(GlobalConfig.CANAL_DB));
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            Flow flow = new Flow();

            while (resultSet.next()) {
                flow.setFlowId(resultSet.getInt("flowId"));
                flow.setMode(resultSet.getInt("mode"));
                flow.setDatabaseName(resultSet.getString("databaseName"));
                flow.setTableName(resultSet.getString("tableName"));
                flow.setHbaseTable(resultSet.getString("hbaseTable"));
                flow.setFamily(resultSet.getString("family"));
                flow.setUppercaseQualifier(resultSet.getBoolean("uppercaseQualifier"));
                flow.setCommitBatch(resultSet.getInt("commitBatch"));
                flow.setStatus(resultSet.getInt("status"));
                flow.setRowKey(resultSet.getString("rowKey"));
                flow.setExcludeColumns(resultSet.getString("excludeColumns"));
                System.out.println(flow.toString());
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            JdbcUtil.close(resultSet, statement, connection);
        }
    }

}
