package com.xh.flink.stream.fraud;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/getting-started/walkthroughs/datastream_api.html#executing-the-job
 */
public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        //执行环境
        //第一行设置您的StreamExecutionEnvironment。执行环境是您为Job设置属性，创建源以及最终触发Job执行的方式。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建源
        //源将来自外部系统（例如Apache Kafka，Rabbit MQ或Apache Pulsar）的数据摄取到Flink Jobs中。
        //本演练使用可生成无限量信用卡交易流的源来处理。
        // 每笔交易都包含一个帐户ID（accountId），timestamp交易发生的时间戳（）和美元金额（amount）。name源的附件仅用于调试目的，因此，如果出现问题，我们将知道错误的出处。
        DataStream<Transaction> transactions = env
                .addSource(new TransactionSource())
                .name("transactions");

        //划分事件和检测欺诈
        //该transactions流包含了大量的用户，
        //例如它需要在并行多我欺诈检测任务要处理的大量交易。由于欺诈是按帐户进行的，因此您必须确保欺诈帐户操作员的同一并行任务处理同一帐户的所有交易。
        //为确保同一物理任务处理特定键的所有记录，您可以使用来对流进行分区DataStream#keyBy。
        //该process()调用添加了一个运算符，该运算符将函数应用于流中的每个分区元素。通常keyBy，在这种情况下FraudDetector，在键上下文中执行a 之后，立即说出运算符。
        DataStream<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
//                .process(new FraudDetector())
                .process(new FraudByStateAndTimeDetector())
                .name("fraud-detector");
        //输出结果
        //接收器将a写入DataStream外部系统；例如Apache Kafka，Cassandra和AWS Kinesis。该AlertSink记录每个Alert用日志记录级别信息，
        // 而不是将其写入持久存储，所以你可以很容易地看到你的结果。
        alerts.addSink(new AlertSink())
                .name("send-alerts");

        //执行工作
        //Flink应用程序是延迟构建的，并仅在完全形成后才交付给集群以执行。致电StreamExecutionEnvironment#execute以开始执行我们的工作并为其命名。
        env.execute("Fraud Detection");
    }
}