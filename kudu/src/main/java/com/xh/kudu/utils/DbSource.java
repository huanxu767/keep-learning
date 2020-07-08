package com.xh.kudu.utils;


import java.util.HashMap;
import java.util.Map;

/**
 * 配置数据源
 */
public class DbSource {

    private static Map<String, DbConfig> configMap = new HashMap<>();

    static {
        configMap.put(GlobalConfig.BRMS_DB,DbConfig.builder().dbName("brms_test").url("jdbc:mysql://172.20.0.87:3306/brms_test?useUnicode=true&characterEncoding=utf8").userName("brms_admin").password("brms_admin123").build());
        configMap.put(GlobalConfig.CANAL_DB,DbConfig.builder().dbName("canal_manager").url("jdbc:mysql://dw1:3306/canal_manager?useUnicode=true&characterEncoding=utf8").userName("root").password("xuhuan").build());
    }

    public static DbConfig getDbConfig(String key){
        return configMap.get(key);
    }

}


