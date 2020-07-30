package com.xh.flink.creditdemo.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CreditApply implements Serializable {

    /**
     * 授信流水号
     */
    private String qryCreditId;


    /**
     * 用户身份证姓名
     */
    private String name;

    /**
     * 用户身份证号码
     */
    private String idCard;

    /**
     * 省份
     */
    private String province;

    /**
     * 申请时间
     */
    private String ts;

}
