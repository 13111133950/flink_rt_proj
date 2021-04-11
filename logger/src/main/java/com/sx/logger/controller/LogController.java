package com.sx.logger.controller;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName LogController
 * @Author Kurisu
 * @Description
 * @Date 2021-3-16 11:12
 * @Version 1.0
 **/
@RestController
@Slf4j
public class LogController {
    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;
    @RequestMapping("/applog")
    public String log(@RequestParam("param") String jsonStr){
        log.info(jsonStr);
        kafkaTemplate.send("ods_base_log",jsonStr);
        return "success";
    }
}
