import com.xh.flink.binlogkafkakudu.config.ImportantTableDO;
import com.xh.flink.config.DbSource;
import com.xh.flink.config.GlobalConfig;
import com.xh.flink.utils.JdbcUtil;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;

/**
 * 读取重要表配置
 */
public class InsertMysqlTableSource {


    public static void main(String[] args) {

        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        connection = JdbcUtil.getConnection(DbSource.getDbConfig(GlobalConfig.INFINITY_DB));

        try {
            for (int i = 0; i < 50000; i++) {
                System.out.println(i);
                statement = connection.createStatement();
                statement.execute("insert into `brms_model`.a VALUES(null,1,'hbb',160.0,null,null,2)");
                statement.execute("insert into `brms_model`.a VALUES(null,1,'hbb',160.0,null,null,2)");
                statement.execute("insert into `brms_model`.b VALUES(null,1,'hbb',160.0,null,null)");

            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            JdbcUtil.close(rs, statement, connection);
        }

    }

}
