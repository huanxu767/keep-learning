package com.xh.flink.binlogkafkaflinkhbase.support;

import com.xh.flink.binlogkafkaflinkhbase.FlowStatusEnum;
import com.xh.flink.binlogkafkaflinkhbase.HBaseStorageModeEnum;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * <p>
 *     每张表一个Flow
 * </p>
 * <p>
 *     {
 *         mode: STRING               # HBase中的存储类型，默认统一存为String，可选：#PHONEIX #NATIVE #STRING (#NATIVE:以Java类型为主，PHONEIX：将类型转换为Phoneix对应的类型)
 *         destination: example       # 对应 canal destination/MQ topic名称
 *         database: mytest           # 数据库名/schema名
 *         table: person              # 表名
 *         hbaseTable: MYTEST.PERSON  # HBase表名
 *         family: CF                 # 默认统一Column Family名称
 *         uppercaseQualifier: true   # 字段名转大写，默认为true
 *         commitBatch: 3000          # 批量提交的大小，ETL中用到
 *         #rowKey: id,type           # 复合字段rowKey不能和columns中的rowKey并存
 *         columns:                   # 字段映射，如果不配置将自动映射所有字段，并取第一个字段为rowKey，HBase字段名以mysql字段名为主
 *         id: ROWKE
 *         name: CF:NAME
 *         email: EMAIL               # 如果column family为默认CF，则可以省略
 *         type:                      # 如果HBase字段和mysql字段名一致，则可以省略
 *         c_time:
 *         birthday:
 *     }
 * </p>
 *
 * @author buildupchao
 * @date 2020/02/02 19:23
 * @since JDK 1.8
 */
@Data
@ToString
public class Flow implements Serializable {

    private Integer flowId;

    /**
     * HBase中的存储类型，默认统一存为String
     */
    private int mode = HBaseStorageModeEnum.STRING.getCode();
    /**
     * 数据库名/schema名
     */
    private String databaseName;
    /**
     * mysql表名
     */
    private String tableName;
    /**
     * hbase表名
     */
    private String hbaseTable;
    /**
     * 默认统一Column Family名称
     */
    private String family;
    /**
     * 字段名转大写，默认为true
     */
    private boolean uppercaseQualifier = true;
    /**
     * 批量提交的大小，ETL中用到
     */
    private int commitBatch;
    /**
     * 组成rowkey的字段名，必须用逗号分隔
     */
    private String rowKey;

    /**
     * 排除的key，必须用逗号分隔
     */
    private String excludeColumns;
    /**
     * 状态
     */
    private int status = FlowStatusEnum.FLOW_STATUS_INIT.getCode();
}
