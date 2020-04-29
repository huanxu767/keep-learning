package com.xh.bigdata.learnonelearn.jdk.java8.stream;

import java.util.Optional;

public class Optional1 {
    public static void main(String[] args) {
        Optional<String> optional = Optional.of("xh");
        boolean flag = optional.isPresent();
        System.out.println(flag);
        String value = optional.get();
        System.out.println(value);
        String orElse = optional.orElse("fallback");
        System.out.println(orElse);
        optional.ifPresent((s) -> System.out.println(s.charAt(0)));     // "b"
    }
}
