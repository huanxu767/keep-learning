package com.xh.flink.creditdemo.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CreditReport {

    /**
     * 授信流水号
     */
    private String qryCreditId;

    /**
     * 身份证号
     */
    private String idCard;

    /**
     * 对内原因
     */
    private String intervalReason;

    /**
     * 授信结果：0-处理中；1-成功；2-失败
     */
    private String status;
    /**
     * 真实授信额度
     */
    private Double amt;

    /**
     * 创建时间
     */
    private Date createTime;

}
