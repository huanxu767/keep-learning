package com.xh.flink.binlog;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class TableChangeMsg implements Serializable {
    private Long logId;
    private String database;       // 数据库或schema
    private String table;          // 表名
    private String type;           // 类型: INSERT UPDATE DELETE
    private String sql;            // 执行的sql, dml sql为空
    private String changeTime;     // 同步时间

}
