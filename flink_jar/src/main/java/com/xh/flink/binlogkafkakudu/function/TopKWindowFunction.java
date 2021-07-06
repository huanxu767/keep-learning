package com.xh.flink.binlogkafkakudu.function;

import com.xh.flink.binlog.Dml;
import com.xh.flink.binlogkafkakudu.config.KuduMapping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class TopKWindowFunction extends ProcessAllWindowFunction<Tuple2<Dml, KuduMapping>, List<Tuple2<Dml, KuduMapping>>, TimeWindow> {


    @Override
    public void process(Context context, Iterable<Tuple2<Dml, KuduMapping>> iterable, Collector<List<Tuple2<Dml, KuduMapping>>> collector) throws Exception {
        List<Tuple2<Dml, KuduMapping>> list = new ArrayList<>();
        int i = 0;
        for (Tuple2<Dml, KuduMapping> d : iterable){
            list.add(d);
            i++;
        }
        collector.collect(list);
    }
}
