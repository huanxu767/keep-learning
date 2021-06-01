package com.xh.flink.utils;

import com.xh.flink.binlog.Dml;
import com.xh.flink.binlog.TableChangeMsg;

public class DmlToTableChangeMsgUtil {

    public static TableChangeMsg transform(Dml dml) {

        return TableChangeMsg.builder().logId(dml.getId())
                .table(dml.getTable()).database(dml.getDatabase())
                .sql(dml.getSql()).type(dml.getType())
                .changeTime(TimeUtils.tsToString(dml.getTs())).build();
    }
}
