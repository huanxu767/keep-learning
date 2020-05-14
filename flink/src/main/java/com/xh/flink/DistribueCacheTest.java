package com.xh.flink;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;

public class DistribueCacheTest {

    private static final String FILE_PATH = "/Users/xuhuan/IdeaProjects/learn-one-learn/flink/src/main/resources/files/user.txt";
    private static final String FILE_NAME = "localfile";

    public static void main(String[] args) throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.ERROR);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //注册一个本地可执行文件
        env.registerCachedFile(FILE_PATH,FILE_NAME,true);

        DataSet<String> data = env.fromElements("xu","huan","foo","bar");
        data.map(new RichMapFunction<String, String>() {
            HashMap<String,String> allMap = new HashMap<>();
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File file = getRuntimeContext().getDistributedCache().getFile(FILE_NAME);
                List<String> lines = FileUtils.readLines(file);
                for (String line:lines){
                    String[] split = line.split(",");
                    allMap.put(split[0],split[1]);
                }

            }

            @Override
            public String map(String s) throws Exception {
                //查询用户信息
                String age = allMap.get(s);
                return s + ":" +age;
            }
        }).print();

    }
}
