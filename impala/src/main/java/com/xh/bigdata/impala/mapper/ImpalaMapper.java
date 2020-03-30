package com.xh.bigdata.impala.mapper;

import org.apache.ibatis.annotations.Mapper;

import java.util.List;
import java.util.Map;

@Mapper
public interface ImpalaMapper {

    List<Map> getBlackList();

    List<Map> testFintech();
}
