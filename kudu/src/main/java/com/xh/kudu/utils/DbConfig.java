package com.xh.kudu.utils;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Mysql数据库链接
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DbConfig {
    private String databaseKey = "";
    private String type = "";
    private String database = "";
    private String connectionUrl = "";
    private String userName = "";
    private String password = "";
}