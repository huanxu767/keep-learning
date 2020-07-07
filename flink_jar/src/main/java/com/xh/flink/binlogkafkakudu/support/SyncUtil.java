package com.xh.flink.binlogkafkakudu.support;


import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.xh.flink.binlogkafkakudu.config.KuduMapping;
import org.apache.commons.lang.StringUtils;


/**
 * @author xuhuan
 * @description 工具
 */
public class SyncUtil {

    /**
     * 获取源数据库 元数据结构
     *
     * @param kuduMapping
     * @return
     */
    public static String getDbTableName(KuduMapping kuduMapping) {
        String result = "";
        if (StringUtils.isNotEmpty(kuduMapping.getTable())) {
            result += kuduMapping.getDatabase() + ".";
        }
        result += kuduMapping.getTable();
        return result;
    }

    /**
     * 过滤转换字段
     *
     * @param kuduMapping
     * @param columns
     * @return
     */
    public static Map<String, String> getColumnsMap(KuduMapping kuduMapping, List<String> columns) {
        Map<String, String> columnsMap;

        // 目前暂时禁用，只允许明确指定列
        if (kuduMapping.getMapAll()) {
            if (kuduMapping.getAllMapColumns() != null) {
                return kuduMapping.getAllMapColumns();
            }
            columnsMap = new LinkedHashMap<>();
            for (String srcColumn : columns) {
                boolean flag = true;
                if (kuduMapping.getTargetColumns() != null) {
                    for (Map.Entry<String, String> entry : kuduMapping.getTargetColumns().entrySet()) {
                        if (srcColumn.equals(entry.getValue())) {
                            columnsMap.put(entry.getKey(), srcColumn);
                            flag = false;
                            break;
                        }
                    }
                }
                if (flag) {
                    columnsMap.put(srcColumn, srcColumn);
                }
            }
            kuduMapping.setAllMapColumns(columnsMap);
        } else {
            // 目前暂时禁用，只允许明确指定列
            columnsMap = kuduMapping.getTargetColumns();
        }
        return columnsMap;
    }

}
