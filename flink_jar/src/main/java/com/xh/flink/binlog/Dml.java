package com.xh.flink.binlog;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * DML操作转换对象
 *
 * @author xuhua
 */
@Data
public class Dml implements Serializable {

    private static final long serialVersionUID = 2611556444074013268L;

    private Long id;//binlog 编号
    private String destination;                            // 对应canal的实例或者MQ的topic
    private String groupId;                                // 对应mq的group id
    private String database;                               // 数据库或schema
    private String table;                                  // 表名
    private List<String> pkNames;
    private Boolean isDdl;
    private String type;                                   // 类型: INSERT UPDATE DELETE
    // binlog executeTime
    private Long es;                                     // 执行耗时
    // dml build timeStamp
    private Long ts;                                     // 同步时间
    private String sql;                                    // 执行的sql, dml sql为空
    private List<Map<String, Object>> data;                                   // 数据列表
    private List<Map<String, Object>> old;                                    // 旧数据列表, 用于update, size和data的size一一对应
}
