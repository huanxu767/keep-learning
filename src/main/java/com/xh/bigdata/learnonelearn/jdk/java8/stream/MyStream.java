package com.xh.bigdata.learnonelearn.jdk.java8.stream;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MyStream {

    public static void main(String[] args) {

        //Stream 提供了新的方法 'forEach' 来迭代流中的每个数据。以下代码片段使用 forEach 输出了5个随机数：
        //limit 方法用于获取指定数量的流。
        //sorted 方法用于对流进行排序
        Random random = new Random();
        random.ints().limit(5).sorted().forEach(System.out::println);

        //map 方法用于映射每个元素到对应的结果，以下代码片段使用 map 输出了元素对应的平方数：
        List<Integer> numbers = Arrays.asList(1, 3, 4, 2, 1, 2, 4);
        List<Integer> squaresList = numbers.stream().map(i -> i * i).distinct().collect(Collectors.toList());
        System.out.println("map");
        System.out.println(squaresList);

        //filter 方法用于通过设置的条件过滤出元素。以下代码片段使用 filter 方法过滤出空字符串：
        System.out.println("filter");
        List<String> strings = Arrays.asList("abc", "", "bc", "efg", "abcd", "", "jkl");
        long size = strings.stream().filter(s -> !s.isEmpty()).count();
        System.out.println(size);

        //并行（parallel）程序
        //parallelStream 是流并行处理程序的代替方法。 使用parallelStream后，结果并不按照集合原有顺序输出
        List<String> stringAll = Arrays.asList("abc", "", "bc", "efg", "abcd", "", "jkl");
        List<String> parallelStreamString = stringAll.parallelStream().filter(s -> !s.isEmpty()).collect(Collectors.toList());
        System.out.println("parallel：" + parallelStreamString);
        //Collectors 类实现了很多归约操作，例如将流转换成集合和聚合元素。Collectors 可用于返回列表或字符串：
        List<String> stringC = Arrays.asList("abc", "", "bc", "efg", "abcd", "", "jkl");
        List<String> filtered = stringC.stream().filter(string -> !string.isEmpty()).collect(Collectors.toList());
        String mergedString = strings.stream().filter(string -> !string.isEmpty()).collect(Collectors.joining(", "));
        System.out.println("合并字符串: " + mergedString);
        //统计
        List<Integer> num = Arrays.asList(4, 2, 2, 4);
        IntSummaryStatistics stats = num.stream().mapToInt(x -> x).summaryStatistics();
        System.out.println("列表中最大的数 : " + stats.getMax());
        System.out.println("列表中最小的数 : " + stats.getMin());
        System.out.println("所有数之和 : " + stats.getSum());
        System.out.println("平均数 : " + stats.getAverage());


        //Reduce 逐个相加
        Optional accResult = Stream.of(1, 2, 3, 4)
                .reduce((acc, item) -> {
                    System.out.println("acc : " + acc);
                    acc += item;
                    System.out.println("item: " + item);
                    System.out.println("acc+ : " + acc);
                    System.out.println("--------");
                    return acc;
                });
        System.out.println("accResult: " + accResult.get());

        // 与第一种变形相同的是都会接受一个BinaryOperator函数接口，
        // 不同的是其会接受一个identity参数，用来指定Stream循环的初始值。
        // 如果Stream为空，就直接返回该值。另一方面，该方法不会返回Optional，因为该方法不会出现null。
        int accIntResult = Stream.of(1, 2, 3, 4)
                .reduce(10, (acc, item) -> {
                    System.out.println("acc : " + acc);
                    acc += item;
                    System.out.println("item: " + item);
                    System.out.println("acc+ : " + acc);
                    System.out.println("--------");
                    return acc;
                });
        System.out.println("accIntResult: " + accIntResult);
        System.out.println("--------");


    }
}
