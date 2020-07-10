package com.xh.kudu.Thread;

import com.xh.kudu.dao.DbOperation;
import com.xh.kudu.dao.DbOperationImpl;
import com.xh.kudu.utils.DbConfig;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Map;

/**
 * mysql获取列
 */
public class MySqlColumnsThread implements Runnable{

    private DbConfig dbConfig;
    private List<String> tableList;
    private Map<String,List<String>> columnMap;

    public MySqlColumnsThread(DbConfig dbConfig, List<String> tableList) {
        this.dbConfig = dbConfig;
        this.tableList = tableList;
    }

    @SneakyThrows
    @Override
    public void run() {
        System.out.println("begin run mysql column");
        DbOperation dbOperation = new DbOperationImpl();
        // 获取hive中表字段
        columnMap = dbOperation.queryMysqlColumns(dbConfig,tableList);
    }

}
