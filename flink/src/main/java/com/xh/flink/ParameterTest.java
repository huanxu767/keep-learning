package com.xh.flink;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.LoggerFactory;

/**
 * 通过构造函数传参数
 */
public class ParameterTest {
    public static void main(String[] args) throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.ERROR);


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 通过构造方法传入参数
        env.fromElements(1,2,3,4,5).filter(new MyFilter(3)).print();
        //通过Configuration传递参数

        Configuration parameters = new Configuration();
        parameters.setInteger("limit",3);
        System.out.println("withParameters");
        env.fromElements(1,2,3,4,5).filter(new MyFilter(3)).withParameters(parameters).print();
        System.out.println("全局参数");
        //全局参数
        parameters.setString("globalKey","token123");
        env.getConfig().setGlobalJobParameters(parameters);
        env.fromElements("a","b","c").flatMap(new MyRickFlatMap()).print();
    }

    public static class MyFilter implements FilterFunction<Integer>{
        private Integer limit = 0;

        public MyFilter(Integer limit) {
            this.limit = limit;
        }

        @Override
        public boolean filter(Integer val) throws Exception {
            return val > limit;
        }
    }

    public static class MyRichFilterFunction extends RichFilterFunction<Integer> {
        private Integer limit = 0;

        @Override
        public void open(Configuration parameters) throws Exception {
            //Deprecated
//            this.limit = parameters.getInteger("limit",0);

            ConfigOption<Integer> limitConfig = ConfigOptions
                    .key("limit")
                    .intType()
                    .defaultValue(0);

            this.limit = parameters.getInteger(limitConfig);
        }

        @Override
        public boolean filter(Integer val) throws Exception {
            return val > limit;
        }
    }


    public static class MyRickFlatMap extends RichFlatMapFunction<String, Tuple2<String,String>>{
        private String token;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ExecutionConfig.GlobalJobParameters globalJobParameters = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            Configuration pa = (Configuration)globalJobParameters;
            ConfigOption<String> token = ConfigOptions
                    .key("globalKey")
                    .stringType()
                    .defaultValue("kong");
            this.token = parameters.getString(token);
        }
        @Override
        public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
            out.collect(Tuple2.of(value,token));
        }
    }
}
