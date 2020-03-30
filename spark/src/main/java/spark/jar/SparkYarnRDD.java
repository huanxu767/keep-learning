package spark.jar;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkYarnRDD {

    /**
     *
     * @param args
     */
    public static void main(String[] args) {

///
//        spark-submit \
//        --class com.xh.spark.SparkYarnRDD \
//        --master yarn --executor-memory 1G --num-executors 1 --driver-memory 1g --executor-cores 1 \
//        --deploy-mode cluster \
//          /home/demoyarn3.jar \
//        10
//
//
//
//        spark-submit \
//        --class com.xh.spark.SparkYarnRDD \
//        --master yarn \
//        --deploy-mode client \
//          /home/demoyarn3.jar \
//        10


        String appName = "test";
        // 打成jar包执行 不能 指定 yarn-cluster
        SparkConf conf = new SparkConf().setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
//        Integer sum = distData.reduce(new PlusFunction());
        Integer sum = distData.reduce((a, b) -> a + b);
        System.out.println(sum);
    }
}
