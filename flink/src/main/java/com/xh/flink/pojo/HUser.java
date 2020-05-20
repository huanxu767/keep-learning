package com.xh.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HUser {
    public static final String TABLE_NAME = "flink_demo";
    public static final String Column_Family = "foo";
    private String rowKey;
    private String name;
}