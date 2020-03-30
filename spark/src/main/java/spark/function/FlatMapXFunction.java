package spark.function;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;


/**
 * FlatMap
 */
public class FlatMapXFunction implements FlatMapFunction<String,String> {

    @Override
    public Iterator<String> call(String s) throws Exception {
        return Arrays.asList(s.split(" ")).iterator();
    }


}
