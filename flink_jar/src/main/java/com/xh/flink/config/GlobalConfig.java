package com.xh.flink.config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class GlobalConfig implements Serializable {


    public static final String CANAL_DB = "canal_manager";

    public static final String BRMS_DB = "brms";


    //  MySQL DriveClass
    public static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    //  Impala DriveClass
    public static final String IMPALA_DRIVER_CLASS = "com.cloudera.impala.jdbc41.Driver";
    // 本地
//    public static final String DB_URL = "jdbc:mysql://dev-dw1:3306/canal_manager?useUnicode=true&characterEncoding=utf8";

    public static final String CONNECTION_URL = "jdbc:impala://impal-api-internal.hbfintech.com:21050/brms;auth=noSasl";

    public static final String KUDU_MASTER = "dw1:7051";

    /**
     * Kafka相关配置
     */
//    public static final String BOOTSTRAP_SERVERS = "dev-dw1:9092,dev-dw2:9092,dev-dw3:9092,dev-dw4:9092,dev-dw5:9092";
    public static final String BOOTSTRAP_SERVERS = "dw4:9092,dw5:9092,dw6:9092,dw7:9092,dw8:9092";

    public static final String ZOOKEEPER_ZNODE_PARENT = "/hbase";

    // HBase zookeeper
    public static final String ZOOKEEPER = "dw1,dw2,dw3";
    public static final String BRMS_TOPIC = "canal_binlog_brms_topic";
    public static final String DATAWARE_TOPIC = "canal_binlog_dataware_topic";

    public static List<String> TOPIC = null;
    static {
        TOPIC = new ArrayList<>();
        TOPIC.add(GlobalConfig.BRMS_TOPIC);
        TOPIC.add(GlobalConfig.DATAWARE_TOPIC);
    }




}
