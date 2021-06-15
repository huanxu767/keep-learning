package com.xh.flink.binlogkafkakudu.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.xh.flink.binlog.Dml;
import com.xh.flink.binlogkafkakudu.config.KuduMapping;
import com.xh.flink.binlogkafkakudu.support.KuduTemplate;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

/**
 * @author xuhuan
 * @description kudu实时同步
 */
public class KuduSyncService {

    private static Logger logger = LoggerFactory.getLogger(KuduSyncService.class);

    private KuduTemplate kuduTemplate;

    public KuduSyncService(KuduTemplate kuduTemplate){
        this.kuduTemplate = kuduTemplate;
    }

    /**
     * 同步事件处理
     *
     * @param KuduMapping
     * @param dml
     */
    public void sync(KuduMapping KuduMapping, Dml dml) {
        if (KuduMapping != null) {
            String type = dml.getType();
            if (type != null && type.equalsIgnoreCase("INSERT")) {
                insert(KuduMapping, dml);
            } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                upsert(KuduMapping, dml);
            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                delete(KuduMapping, dml);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }
        }
    }

    /**
     * 删除事件
     *
     * @param kuduMapping
     * @param dml
     */
    private void delete(KuduMapping kuduMapping, Dml dml) {
        String configTable = kuduMapping.getTable();
        String configDatabase = kuduMapping.getDatabase();
        String table = dml.getTable();
        String database = dml.getDatabase();
        if (configTable.equals(table) && configDatabase.equals(database)) {
            List<Map<String, Object>> data = dml.getData();
            if (data == null || data.isEmpty()) {
                return;
            }
            // 判定主键映射
            String pkId = "";
            Map<String, String> targetPk = kuduMapping.getTargetPk();
            for (Map.Entry<String, String> entry : targetPk.entrySet()) {
                String mysqlID = entry.getKey().toLowerCase();
                String kuduID = entry.getValue();
                if (kuduID == null) {
                    pkId = mysqlID;
                } else {
                    pkId = kuduID;
                }
            }
            // 切割联合主键
            List<String> pkIds = Arrays.asList(pkId.split(","));
            try {
                int idx = 1;
                boolean completed = false;
                List<Map<String, Object>> dataList = new ArrayList<>();

                for (Map<String, Object> item : data) {
                    Map<String, Object> primaryKeyMap = new HashMap<>();
                    for (Map.Entry<String, Object> entry : item.entrySet()) {
                        String columnName = entry.getKey().toLowerCase();
                        Object value = entry.getValue();
                        if (pkIds.contains(columnName)) {
                            primaryKeyMap.put(columnName, value);
                        }
                    }
                    dataList.add(primaryKeyMap);
                    idx++;
                    if (idx % kuduMapping.getCommitBatch() == 0) {
                        kuduTemplate.delete(kuduMapping.getTargetTable(), dataList);
                        dataList.clear();
                        completed = true;
                    }
                }
                if (!completed) {
                    kuduTemplate.delete(kuduMapping.getTargetTable(), dataList);
                }
            } catch (KuduException e) {
                logger.error(e.getMessage());
                logger.error("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }
        }
    }

    /**
     * 更新插入事件
     *
     * @param kuduMapping
     * @param dml
     */
    private void upsert(KuduMapping kuduMapping, Dml dml) {
        String configTable = kuduMapping.getTable();
        String configDatabase = kuduMapping.getDatabase();
        String table = dml.getTable();
        String database = dml.getDatabase();
        if (configTable.equals(table) && configDatabase.equals(database)) {
            List<Map<String, Object>> data = dml.getData();
            if (data == null || data.isEmpty()) {
                return;
            }
            try {
                int idx = 1;
                boolean completed = false;
                List<Map<String, Object>> dataList = new ArrayList<>();

                for (Map<String, Object> entry : data) {
                    dataList.add(entry);
                    idx++;
                    if (idx % kuduMapping.getCommitBatch() == 0) {
                        kuduTemplate.upsert(kuduMapping.getTargetTable(), dataList);
                        dataList.clear();
                        completed = true;
                    }
                }
                if (!completed) {
                    kuduTemplate.upsert(kuduMapping.getTargetTable(), dataList);
                }
            } catch (KuduException e) {
                logger.error(e.getMessage());
                logger.error("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }
        }

    }

    /**
     * 插入事件
     *
     * @param kuduMapping
     * @param dml
     */
    private void insert(KuduMapping kuduMapping, Dml dml) {
        String configTable = kuduMapping.getTable();
        String configDatabase = kuduMapping.getDatabase();
        String table = dml.getTable();
        String database = dml.getDatabase();
        if (configTable.equals(table) && configDatabase.equals(database)) {
            List<Map<String, Object>> data = dml.getData();
            if (data == null || data.isEmpty()) {
                return;
            }
            try {
                int idx = 1;
                boolean completed = false;
                List<Map<String, Object>> dataList = new ArrayList<>();

                for (Map<String, Object> entry : data) {
                    dataList.add(entry);
                    idx++;
                    if (idx % kuduMapping.getCommitBatch() == 0) {
                        kuduTemplate.insert(kuduMapping.getTargetTable(), dataList);
                        dataList.clear();
                        completed = true;
                    }
                }
                if (!completed) {
                    kuduTemplate.insert(kuduMapping.getTargetTable(), dataList);
                }
            } catch (KuduException e) {
                logger.error(e.getMessage());
                logger.error("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }
        }
    }

}