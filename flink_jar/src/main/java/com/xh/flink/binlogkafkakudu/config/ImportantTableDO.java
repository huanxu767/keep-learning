package com.xh.flink.binlogkafkakudu.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * 重要表
 * 
 * @author xuhuan
 * @email huanxu767@qq.com
 * @date 2021-05-08 10:07:11
 */
@Data
@NoArgsConstructor
public class ImportantTableDO implements Serializable {
	private static final long serialVersionUID = 1L;
	//编号
	private Long id;
	//表名
	private String tableName;
	//库名
	private String dbName;
	//表备注
	private String tableComment;
	//是否删除1生效 0失效
	private Integer valid;
	private String syncTargetTable;
	private String syncPrimaryKey;
	private String syncColumn;
	private int syncStatus;
	//创建时间
	private Date createTime;
	//更新时间
	private Date updateTime;

}
