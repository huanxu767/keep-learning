package com.xh.hive.udf;


import org.apache.hadoop.hive.ql.exec.UDF;

public class My_length extends UDF {

    public static int evaluate(String data) {
       if(data == null){
           return 0;
       }else{
           return data.length();
       }
    }
}
