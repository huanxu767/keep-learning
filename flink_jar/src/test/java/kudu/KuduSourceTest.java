package kudu;

import com.xh.flink.binlogkafkakudu.config.KuduMapping;
import com.xh.flink.binlogkafkakudu.db.FlowKuduSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KuduSourceTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<KuduMapping> dataStream = env.addSource(new FlowKuduSource());
        System.out.println("------------");
        dataStream.print();
        System.out.println("------------");
        try {
            env.execute("kudu increments ");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
