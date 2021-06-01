package kudu;

import com.xh.flink.binlogkafkakudu.config.ImportantTableDO;
import com.xh.flink.binlogkafkakudu.db.ImportantTableSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KuduSourceTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<ImportantTableDO> dataStream = env.addSource(new ImportantTableSource());
        dataStream.print();
        try {
            env.execute("kudu increments ");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
