package spark.jar;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkRDD {

    /**
     *
     * @param args
     */
    public static void main(String[] args) {


//        spark-submit \
//        --class com.xh.spark.SparkRDD \
//        --master local[2] \
//  	    /home/demo2.jar \
//        10


        String appName = "test";
        String master = "local";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
//        Integer sum = distData.reduce(new PlusFunction());
        Integer sum = distData.reduce((a, b) -> a + b);
        System.out.println(sum);
    }
}
