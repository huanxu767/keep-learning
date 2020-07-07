package com.xh.flink.binlogkafkakudu.config;


public class KuduMappingDO {

    private String database;                           // 数据库名或schema名
    private String table;                              // 表名
    private String targetTable;                        // 目标表名

    private String originalKuduTableRelationId;        // kudu表字段,主键。id:id 多个,分割
    private String originalTableColumn;                // 原始表与kudu表字段映射,逗号分隔。a:a1,b:b1

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public String getOriginalKuduTableRelationId() {
        return originalKuduTableRelationId;
    }

    public void setOriginalKuduTableRelationId(String originalKuduTableRelationId) {
        this.originalKuduTableRelationId = originalKuduTableRelationId;
    }

    public String getOriginalTableColumn() {
        return originalTableColumn;
    }

    public void setOriginalTableColumn(String originalTableColumn) {
        this.originalTableColumn = originalTableColumn;
    }
}
