package com.xh.flink.pojo;

import lombok.*;

@Builder
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Foo{
    private Integer id;
    private String name;
    private String other;
}