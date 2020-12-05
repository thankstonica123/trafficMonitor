package com.thankstonica.flink.service.process;

import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;

public interface DataService {

    void process(String dataType, HttpServletRequest request) throws  Exception;
}
