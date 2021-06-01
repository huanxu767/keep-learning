package com.xh.kudu.mian;

import com.xh.kudu.pojo.GlobalConfig;
import com.xh.kudu.service.KuduService;
import com.xh.kudu.service.KuduServiceImpl;
import com.xh.kudu.utils.AutoGenerateKuduMapping;
import org.apache.kudu.client.KuduException;

import java.sql.SQLException;

public class InitTransmissionTableConfig {
    public static void main(String[] args) throws SQLException, KuduException {
        String dbKey = GlobalConfig.SOURCE_DATAWARE_PRO;

        initTransmissionTableConfig(dbKey);
            // 初始化指定库
        AutoGenerateKuduMapping.initDb(dbKey);
    }


    private static void initTransmissionTableConfig(String dbKey) throws SQLException {
        KuduService kuduService = new KuduServiceImpl();
        kuduService.initTransmissionTableConfig(dbKey);
    }
}
