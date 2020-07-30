package com.xh.flink.test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.xh.flink.creditdemo.model.CreditApply;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {
    public static void main(String[] args) throws ParseException {

//        String fo = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

        String fo = "yyyy-MM-dd HH:mm:ss.SSS";
        SimpleDateFormat sf = new SimpleDateFormat(fo);
//        String fo = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

//        2019-01-01 10:30:00.0,379.64


//        System.out.println(SqlTimestamp.fromEpochMillis(System.currentTimeMillis()));


        CreditApply creditApply = CreditApply.builder()
                .qryCreditId("1")
                .name("名")
                .idCard("身份证")
                .province("江苏")
//                .timestamp(SqlTimestamp.fromEpochMillis(System.currentTimeMillis()).toString())
//                .rowtime(d)
//                .createTime(Date.valueOf("2020-07-24"))
//                .timestamp(sf.format(new Date()))
                .build();

        Gson gson = new GsonBuilder().serializeNulls().create();
        System.out.println("gson");
        System.out.println(gson.toJson(creditApply));
    }
}
