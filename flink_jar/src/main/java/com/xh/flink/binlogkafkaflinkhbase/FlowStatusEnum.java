package com.xh.flink.binlogkafkaflinkhbase;

import lombok.Getter;

/**
 * @author buildupchao
 * @date 2020/02/02 19:05
 * @since JDK 1.8
 */
@Getter
public enum FlowStatusEnum implements ICode {
    /**
     * 初始状态（新添加）
     */
    FLOW_STATUS_INIT(0, "初始状态"),
    /**
     * 就绪状态，初始采集后，可以将状态改为就绪状态
     */
    FLOW_STATUS_READY(1, "就绪状态"),
    /**
     * 运行状态（增量采集正在运行）
     */
    FLOW_STATUS_RUNNING(2, "运行状态");

    private Integer code;
    private String message;

    FlowStatusEnum(Integer code, String message) {
        this.code = code;
        this.message = message;
    }
}
