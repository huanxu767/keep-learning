package com.xh.flink.binlogkafkaflinkhbase;


import java.util.*;

import com.xh.flink.binlogkafkaflinkhbase.support.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

/**
 * HBase同步操作业务
 */
public class HbaseSyncService {

    private static Logger logger = LoggerFactory.getLogger(HbaseSyncService.class);

    private HbaseTemplate hbaseTemplate;

    public HbaseSyncService(HbaseTemplate hbaseTemplate){
        this.hbaseTemplate = hbaseTemplate;
    }

    public void sync(Flow flow, Dml dml) {
        if (flow != null) {
            System.out.println(dml.toString());
            String type = dml.getType();
            if (type != null && type.equalsIgnoreCase("INSERT")) {
                insert(flow, dml);
            } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                update(flow, dml);
            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                delete(flow, dml);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("DML: {}", JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue));
            }
        }
    }

    /**
     * 插入操作
     *
     * @param flow 配置项
     * @param dml DML数据
     */
    private void insert(Flow flow, Dml dml) {
        List<Map<String, Object>> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        int i = 1;
        boolean complete = false;
        List<HRow> rows = new ArrayList<>();
        for (Map<String, Object> r : data) {
            HRow hRow = new HRow();
            // 拼接复合rowKey
            if (flow.getRowKey() != null) {
                String[] rowKeyColumns = flow.getRowKey().trim().split(",");
                String rowKeyVale = getRowKeys(rowKeyColumns, r);
                hRow.setRowKey(Bytes.toBytes(rowKeyVale));
            }

            convertData2Row(flow, hRow, r);
            if (hRow.getRowKey() == null) {
                throw new RuntimeException("empty rowKey");
            }
            rows.add(hRow);
            complete = false;
            if (i % flow.getCommitBatch() == 0 && !rows.isEmpty()) {
                hbaseTemplate.puts(flow.getHbaseTable(), rows);
                rows.clear();
                complete = true;
            }
            i++;
        }
        if (!complete && !rows.isEmpty()) {
            hbaseTemplate.puts(flow.getHbaseTable(), rows);
        }

    }

    /**
     * 将Map数据转换为HRow行数据
     *
     * @param flow hbase映射配置
     * @param hRow 行对象
     * @param data Map数据
     */
    private static void convertData2Row(Flow flow, HRow hRow, Map<String, Object> data) {
        int i = 0;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (flow.getExcludeColumns() != null && flow.getExcludeColumns().contains(entry.getKey())) {
                continue;
            }
            if (entry.getValue() != null) {
                byte[] bytes = typeConvert(flow, entry.getValue());
                String familyName = flow.getFamily();
                String qualifier = entry.getKey();
                if (flow.isUppercaseQualifier()) {
                    qualifier = qualifier.toUpperCase();
                }
                if (flow.getRowKey() == null && i == 0) {
                    hRow.setRowKey(bytes);
                } else {
                    hRow.addCell(familyName, qualifier, bytes);
                }
            }
            i++;
        }
    }

    /**
     * 更新操作
     *
     * @param flow 配置对象
     * @param dml dml对象
     */
    private void update(Flow flow, Dml dml) {
        List<Map<String, Object>> data = dml.getData();
        List<Map<String, Object>> old = dml.getOld();
        if (old == null || old.isEmpty() || data == null || data.isEmpty()) {
            return;
        }

        int index = 0;
        int i = 1;
        boolean complete = false;
        List<HRow> rows = new ArrayList<>();
        out: for (Map<String, Object> r : data) {
            byte[] rowKeyBytes;

            String[] rowKeyColumns = flow.getRowKey().trim().split(",");

            // 判断是否有复合主键修改
            for (String updateColumn : old.get(index).keySet()) {
                for (String rowKeyColumnName : rowKeyColumns) {
                    if (rowKeyColumnName.equalsIgnoreCase(updateColumn)) {
                        // 调用删除插入操作
                        deleteAndInsert(flow, dml);
                        continue out;
                    }
                }
            }

            String rowKeyVale = getRowKeys(rowKeyColumns, r);
            rowKeyBytes = Bytes.toBytes(rowKeyVale);

//            Map<String, MappingConfig.ColumnItem> columnItems = flow.getColumnItems();
            HRow hRow = new HRow(rowKeyBytes);
            for (String updateColumn : old.get(index).keySet()) {
                if (flow.getExcludeColumns() != null && flow.getExcludeColumns().contains(updateColumn)) {
                    continue;
                }
                String family = flow.getFamily();
                String qualifier = updateColumn;
                if (flow.isUppercaseQualifier()) {
                    qualifier = qualifier.toUpperCase();
                }
                Object newVal = r.get(updateColumn);

                if (newVal == null) {
                    hRow.addCell(family, qualifier, null);
                } else {
                    hRow.addCell(family, qualifier, typeConvert(flow, newVal));
                }
            }
            rows.add(hRow);
            complete = false;
            if (i % flow.getCommitBatch() == 0 && !rows.isEmpty()) {
                hbaseTemplate.puts(flow.getHbaseTable(), rows);
                rows.clear();
                complete = true;
            }
            i++;
            index++;
        }
        if (!complete && !rows.isEmpty()) {
            hbaseTemplate.puts(flow.getHbaseTable(), rows);
        }
    }

    private void delete(Flow flow, Dml dml) {
        List<Map<String, Object>> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }

        boolean complete = false;
        int i = 1;
        Set<byte[]> rowKeys = new HashSet<>();
        for (Map<String, Object> r : data) {
            byte[] rowKeyBytes;
            String[] rowKeyColumns = flow.getRowKey().trim().split(",");
            String rowKeyVale = getRowKeys(rowKeyColumns, r);
            rowKeyBytes = Bytes.toBytes(rowKeyVale);

            rowKeys.add(rowKeyBytes);
            complete = false;
            if (i % flow.getCommitBatch() == 0 && !rowKeys.isEmpty()) {
                hbaseTemplate.deletes(flow.getHbaseTable(), rowKeys);
                rowKeys.clear();
                complete = true;
            }
            i++;
        }
        if (!complete && !rowKeys.isEmpty()) {
            hbaseTemplate.deletes(flow.getHbaseTable(), rowKeys);
        }
    }

    private void deleteAndInsert(Flow flow, Dml dml) {
        List<Map<String, Object>> data = dml.getData();
        List<Map<String, Object>> old = dml.getOld();
        if (old == null || old.isEmpty() || data == null || data.isEmpty()) {
            return;
        }
        String[] rowKeyColumns = flow.getRowKey().trim().split(",");

        int index = 0;
        int i = 1;
        boolean complete = false;
        Set<byte[]> rowKeys = new HashSet<>();
        List<HRow> rows = new ArrayList<>();
        for (Map<String, Object> r : data) {
            // 拼接老的rowKey
            List<String> updateSubRowKey = new ArrayList<>();
            for (String rowKeyColumnName : rowKeyColumns) {
                for (String updateColumn : old.get(index).keySet()) {
                    if (rowKeyColumnName.equalsIgnoreCase(updateColumn)) {
                        updateSubRowKey.add(rowKeyColumnName);
                    }
                }
            }
            if (updateSubRowKey.isEmpty()) {
                throw new RuntimeException("没有更新复合主键的RowKey");
            }
            StringBuilder oldRowKey = new StringBuilder();
            StringBuilder newRowKey = new StringBuilder();
            for (String rowKeyColumnName : rowKeyColumns) {
                newRowKey.append(r.get(rowKeyColumnName).toString()).append("|");
                if (!updateSubRowKey.contains(rowKeyColumnName)) {
                    // 从data取
                    oldRowKey.append(r.get(rowKeyColumnName).toString()).append("|");
                } else {
                    // 从old取
                    oldRowKey.append(old.get(index).get(rowKeyColumnName).toString()).append("|");
                }
            }
            int len = newRowKey.length();
            newRowKey.delete(len - 1, len);
            len = oldRowKey.length();
            oldRowKey.delete(len - 1, len);
            byte[] newRowKeyBytes = Bytes.toBytes(newRowKey.toString());
            byte[] oldRowKeyBytes = Bytes.toBytes(oldRowKey.toString());

            rowKeys.add(oldRowKeyBytes);
            HRow row = new HRow(newRowKeyBytes);
            convertData2Row(flow, row, r);
            rows.add(row);
            complete = false;
            if (i % flow.getCommitBatch() == 0 && !rows.isEmpty()) {
                hbaseTemplate.deletes(flow.getHbaseTable(), rowKeys);

                hbaseTemplate.puts(flow.getHbaseTable(), rows);
                rowKeys.clear();
                rows.clear();
                complete = true;
            }
            i++;
            index++;
        }
        if (!complete && !rows.isEmpty()) {
            hbaseTemplate.deletes(flow.getHbaseTable(), rowKeys);
            hbaseTemplate.puts(flow.getHbaseTable(), rows);
        }
    }

    /**
     * 根据对应的类型进行转换
     *
     * @param flow hbase映射配置
     * @param value 值
     * @return 复合字段rowKey
     */
    private static byte[] typeConvert(Flow flow,
                                      Object value) {
        if (value == null) {
            return null;
        }
        byte[] bytes = null;
        if (HBaseStorageModeEnum.STRING.getCode() == flow.getMode()) {
            bytes = Bytes.toBytes(value.toString());
        }
        return bytes;
    }

    /**
     * 获取复合字段作为rowKey的拼接
     *
     * @param rowKeyColumns 复合rowK对应的字段
     * @param data 数据
     * @return
     */
    private static String getRowKeys(String[] rowKeyColumns, Map<String, Object> data) {
        StringBuilder rowKeyValue = new StringBuilder();
        for (String rowKeyColumnName : rowKeyColumns) {
            Object obj = data.get(rowKeyColumnName);
            if (obj != null) {
                rowKeyValue.append(obj.toString());
            }
            rowKeyValue.append("|");
        }
        int len = rowKeyValue.length();
        if (len > 0) {
            rowKeyValue.delete(len - 1, len);
        }
        return rowKeyValue.toString();
    }
}
