package com.xh.flink.binlogkafkaflinkhbase.support;

import java.io.Serializable;

public class GlobalConfig implements Serializable {

    /**
     * MySQL配置
     */
    public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
//    public static final String DB_URL = "jdbc:mysql://dev-dw5:3306/canal_xh_test?useUnicode=true&characterEncoding=utf8";
//    public static final String USER_NAME = "canal";
//    public static final String PASSWORD = "canal";

    public static final String DB_URL = "jdbc:mysql://dw1:3306/canal_manager?useUnicode=true&characterEncoding=utf8";
    public static final String USER_NAME = "canal";
    public static final String PASSWORD = "hb6du8xnC";

    /**
     * 批量提交size
     */
    public static final int BATCH_SIZE = 2;

    /**
     * Kafka相关配置
     */
//    public static final String BOOTSTRAP_SERVERS = "dev-dw1:9092,dev-dw2:9092,dev-dw3:9092,dev-dw4:9092,dev-dw5:9092";
    public static final String BOOTSTRAP_SERVERS = "dw4:9092,dw5:9092,dw6:9092,dw7:9092,dw8:9092";

    public static final String ZOOKEEPER_ZNODE_PARENT = "/hbase";
    // HBase zookeeper
    public static final String ZOOKEEPER = "dw1,dw2,dw3";
    public static final String TOPIC = "canal_binlog_brms_topic";

}
