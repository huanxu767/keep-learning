package com.xh.bigdata.impala.controller;

import com.xh.bigdata.impala.mapper.ImpalaMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class TestController {

    @Autowired
    private ImpalaMapper impalaMapper;

    @RequestMapping("/test")
    public String test() {
        List list = impalaMapper.getBlackList();
        System.out.println(list);
        return list.size() + "";
    }

    @RequestMapping("/test2")
    public String test2() {
        List list = impalaMapper.testFintech();
//        System.out.println(list);
        return list.toString();
    }

}
