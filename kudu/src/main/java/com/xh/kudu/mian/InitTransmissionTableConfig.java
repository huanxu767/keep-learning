package com.xh.kudu.mian;

import com.xh.kudu.service.KuduService;
import com.xh.kudu.service.KuduServiceImpl;

import java.sql.SQLException;

public class InitTransmissionTableConfig {
    public static void main(String[] args) throws SQLException {
        KuduService kuduService = new KuduServiceImpl();
        kuduService.initTransmissionTableConfig("brms");
    }
}
