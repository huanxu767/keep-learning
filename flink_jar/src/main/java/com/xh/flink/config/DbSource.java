package com.xh.flink.config;



import com.xh.flink.pojo.DbConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * 配置数据源
 */
public class DbSource {

    private static Map<String, DbConfig> configMap = new HashMap<>();

    static {
        configMap.put(GlobalConfig.BRMS_DB,DbConfig.builder().dbName("brms")
                .url("jdbc:mysql://rm-bp15g76ei364nnl2x606.mysql.rds.aliyuncs.com:3306/brms?useUnicode=true&characterEncoding=utf8")
                .userName("hbbrms").password("Hy77499981").build());
        configMap.put(GlobalConfig.INFINITY_DB,DbConfig.builder().dbName("infinity_pro")
                .url("jdbc:mysql://rm-bp15g76ei364nnl2x606.mysql.rds.aliyuncs.com:3306/infinity_pro?useUnicode=true&characterEncoding=utf8")
                .userName("hbbrms").password("Hy77499981").build());
        configMap.put(GlobalConfig.CANAL_DB,DbConfig.builder().dbName("canal_manager")
                .url("jdbc:mysql://dw1:3306/canal_manager?useUnicode=true&characterEncoding=utf8")
                .userName("root").password("xuhuan").build());
    }

    public static DbConfig getDbConfig(String key){
        return configMap.get(key);
    }

}


