package com.xh.kudu.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 传输配置表管理
 */
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TransmissionTableConfig {
    private int id;
    private String database;
    private String table;
    private boolean transmissionFlag ;
    private boolean hasTransmission ;
    private String structuralDifferences ;

}
