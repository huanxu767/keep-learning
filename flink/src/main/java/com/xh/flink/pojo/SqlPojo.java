package com.xh.flink.pojo;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class SqlPojo {
    private int id;
    private int age;
    private String name;

}
