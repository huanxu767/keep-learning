package com.xh.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Date;

/**
 * 游客订单
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TouristOrder {

    /**
     * 订单编号
     */
    private String id;

    /**
     * 游客编号
     */
    private String touristId;

    /**
     * 票钱
     */
    private double money;

    /**
     * 购票时间
     */
    private Date createTime;
}
