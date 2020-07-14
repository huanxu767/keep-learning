package com.xh.kudu.service;

import java.sql.SQLException;

/**
 *
 */
public interface KuduService {


    /**
     * 将源数据中全部表配置录入
     *
     * @return
     */
    boolean initTransmissionTableConfig(String dbKey) throws SQLException;


}
