package com.xh.kudu.Thread;

import com.xh.kudu.dao.DbOperation;
import com.xh.kudu.dao.DbOperationImpl;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class HiveColumnsThread implements Runnable{

    private String dbName;
    private List<String> tableList;
    private Map<String,List<String>> columnMap;

    public HiveColumnsThread(String dbName, List<String> tableList) {
        this.dbName = dbName;
        this.tableList = tableList;
    }

    @SneakyThrows
    @Override
    public void run() {
        System.out.println("begin run hive column");

        DbOperation dbOperation = new DbOperationImpl();
        // 获取hive中表字段
        columnMap = dbOperation.describeHiveTable(dbName,tableList);

    }

}
