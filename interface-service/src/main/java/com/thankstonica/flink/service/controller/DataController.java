package com.thankstonica.flink.service.controller;

import com.thankstonica.flink.service.bean.ResponseCodePropertyConfig;
import com.thankstonica.flink.service.bean.ResponseEntity;
import com.thankstonica.flink.service.constant.ResponseConstant;
import com.thankstonica.flink.service.process.DataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/controller")
public class DataController {

    @Autowired
    private ResponseCodePropertyConfig config;

    @Autowired
    private DataService service;

    @PostMapping("/sendData/{dataType}")
    public Object collect(@PathVariable("dataType") String dataType , HttpServletRequest request ) throws  Exception{
        service.process(dataType,request);
        return new ResponseEntity(ResponseConstant.CODE_0000,config.getMsg(ResponseConstant.CODE_0000),dataType);
    }
}
