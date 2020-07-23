package com.xh.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 游客信息
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Tourist {

    /**
     * 游客编号
     */
    private String id;
    /**
     * 游客名字
     */
    private String name;

    /**
     * 年龄
     */
    private int age;

}
