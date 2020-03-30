package spark.function;

import org.apache.spark.api.java.function.Function2;

/**
 * 加法
 */
public class PlusFunction implements Function2<Integer, Integer,Integer> {


    @Override
    public Integer call(Integer a, Integer b) throws Exception {
        return a + b;
    }
}
