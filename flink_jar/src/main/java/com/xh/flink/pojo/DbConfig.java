package com.xh.flink.pojo;

import lombok.Builder;
import lombok.Data;

/**
 * Mysql数据库链接
 */
@Builder
@Data
public class DbConfig{
    private String dbName = "";
    private String url = "";
    private String userName = "";
    private String password = "";
}