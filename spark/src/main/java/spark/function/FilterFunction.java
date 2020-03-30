package spark.function;

import org.apache.spark.api.java.function.Function;


/**
 * 过滤
 */
public class FilterFunction implements Function<String,Boolean> {


    private String key = "xu";


    @Override
    public Boolean call(String s) throws Exception {
        return  s.contains(key);
    }
}
