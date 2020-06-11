package com.xh.flink.binlogkafkaflinkhbase;

import lombok.Getter;

/**
 * @author buildupchao
 * @date 2020/02/02 19:09
 * @since JDK 1.8
 */
@Getter
public enum HBaseStorageModeEnum implements ICode {
    STRING(0, "STRING"),
    NATIVE(1, "NATIVE"),
    PHONEIX(2, "PHONEIX");

    private Integer code;
    private String message;

    HBaseStorageModeEnum(Integer code, String message) {
        this.code = code;
        this.message = message;
    }
}
