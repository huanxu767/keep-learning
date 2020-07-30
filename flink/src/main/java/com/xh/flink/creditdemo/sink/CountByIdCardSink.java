package com.xh.flink.creditdemo.sink;

import com.xh.flink.pojo.MysqlUser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import scala.Tuple2;

public class CountByIdCardSink extends RichSinkFunction<Tuple2<String,Long>> {

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("-----------1open-----------");
        super.open(parameters);

    }

    @Override
    public void close() throws Exception {


    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(Tuple2<String,Long> tuple2, Context context) throws Exception {

    }
}
