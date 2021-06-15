package com.xh.flink.config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class GlobalConfig implements Serializable {


    public static final String CANAL_DB = "canal_manager";
    public static final String INFINITY_DB = "infinity_pro";
    public static final String BRMS_DB = "brms";


    //  MySQL DriveClass
    public static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    //  Impala DriveClass
    public static final String IMPALA_DRIVER_CLASS = "com.cloudera.impala.jdbc41.Driver";

    public static final String CONNECTION_URL = "jdbc:impala://impal-api-internal.hbfintech.com:21050/brms;auth=noSasl";

    public static final String KUDU_MASTER = "dw1:7051";

    public static final String BOOTSTRAP_SERVERS = "dw4:9092,dw5:9092,dw6:9092,dw7:9092,dw8:9092";

    public static final String ZOOKEEPER_ZNODE_PARENT = "/hbase";

    public static final String ZOOKEEPER = "dw1,dw2,dw3";

//    public final static String MQ_USER_NAME = "guest";
//    public final static String MQ_PASSWORD = "guest";
    public final static String MQ_USER_NAME = "admin";
    public final static String MQ_PASSWORD = "admin123";
        public final static String MQ_URL = "mq-cluster.hbfintech.com";
//    public final static String MQ_URL = "127.0.0.1";
    public final static String MQ_NOTIFY_TOPIC = "important_table_change";
    public final static int OPERATION_BATCH = 10000;







}
