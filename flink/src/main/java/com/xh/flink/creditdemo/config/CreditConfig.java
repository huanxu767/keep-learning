package com.xh.flink.creditdemo.config;

public class CreditConfig {

    public static final String BOOTSTRAP_SERVERS = "dev-dw1:9092,dev-dw2:9092,dev-dw3:9092,dev-dw4:9092,dev-dw5:9092";
    public static final String ZOOKEEPER_CONNECT = "dev-dw1,dev-dw2,dev-dw3,dev-dw4,dev-dw5";

    public static final String CREDIT_APPLY_TOPIC = "flink_credit_apply_topic";

    public static final String GROUP_ID = "credit_statistics_group";

}
