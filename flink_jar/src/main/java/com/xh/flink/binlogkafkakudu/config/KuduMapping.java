package com.xh.flink.binlogkafkakudu.config;


import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;



public class KuduMapping {

    private String database;                           // 数据库名或schema名
    private String table;                              // 表名

    private boolean mapAll = false;                    // 映射所有字段
    private String targetTable;                        // 目标表名

    private int readBatch = 5000;
    private int commitBatch = 5000;                    // etl等批量提交大小

    // 需要转换
    private Map<String, String> targetPk; // 目标表主键字段
    private Map<String, String> targetColumns;         // 目标表字段映射

    // 目前暂时禁用，只允许明确指定列
    private Map<String, String> allMapColumns;



    public KuduMapping(ImportantTableDO importantTableDO) {
        this.database = importantTableDO.getDbName();
        this.table = importantTableDO.getTableName();
        this.targetTable = importantTableDO.getSyncTargetTable();
        this.targetPk = assemblePk(importantTableDO.getSyncPrimaryKey());
        this.targetColumns = assembleColumns(importantTableDO.getSyncColumn());

    }

    private Map<String, String> assembleColumns(String originalTableColumn) {
        Map<String,String> pkMap = new HashMap<>();
        String[] ar = originalTableColumn.trim().split(",");
        for (int i = 0; i < ar.length; i++) {
            pkMap.put(ar[i],ar[i]);
        }
        return pkMap;
    }

    private Map<String, String> assemblePk(String originalKuduTableRelationId) {
        Map<String,String> columnMap = new HashMap<>();
        String[] ar = originalKuduTableRelationId.trim().split(",");
        for (int i = 0; i < ar.length; i++) {
            columnMap.put(ar[i],ar[i]);
        }
        return columnMap;
    }


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

    public Map<String, String> getTargetPk() {
        return targetPk;
    }

    public void setTargetPk(Map<String, String> targetPk) {
        this.targetPk = targetPk;
    }

    public Boolean getMapAll() {
        return mapAll;
    }

    public void setMapAll(Boolean mapAll) {
        this.mapAll = mapAll;
    }


    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public Map<String, String> getTargetColumns() {
        if (targetColumns != null) {
            targetColumns.forEach((key, value) -> {
                if (StringUtils.isEmpty(value)) {
                    targetColumns.put(key, key);
                }
            });
        }
        return targetColumns;
    }

    public void setTargetColumns(Map<String, String> targetColumns) {
        this.targetColumns = targetColumns;
    }

    public int getReadBatch() {
        return readBatch;
    }

    public void setReadBatch(int readBatch) {
        this.readBatch = readBatch;
    }

    public int getCommitBatch() {
        return commitBatch;
    }

    public void setCommitBatch(int commitBatch) {
        this.commitBatch = commitBatch;
    }

    public Map<String, String> getAllMapColumns() {
        return allMapColumns;
    }

    public void setAllMapColumns(Map<String, String> allMapColumns) {
        this.allMapColumns = allMapColumns;
    }

    @Override
    public String toString() {
        return "KuduMapping{" +
                "database='" + database + '\'' +
                ", table='" + table + '\'' +
                ", mapAll=" + mapAll +
                ", targetTable='" + targetTable + '\'' +
                ", readBatch=" + readBatch +
                ", commitBatch=" + commitBatch +
                ", targetPk=" + targetPk +
                ", targetColumns=" + targetColumns +
                ", allMapColumns=" + allMapColumns +
                '}';
    }
}
