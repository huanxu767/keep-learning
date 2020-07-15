package com.xh.kudu.service;

import org.apache.kudu.client.KuduException;

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


    /**
     * 清空kudu表
     *
     * @return
     */
     void dropTables(String dbKey) throws SQLException, KuduException;
}
