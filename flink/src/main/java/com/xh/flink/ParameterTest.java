package com.xh.flink;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
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
        env.fromElements(1,2,3,4,5).filter(new MyFilter(3)).print();
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
}
