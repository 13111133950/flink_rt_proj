package com.sx.app.dwd;

import com.sx.common.BaseApp;
import com.sx.util.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @ClassName dwdApp
 * @Author Kurisu
 * @Description
 * @Date 2021-3-16 13:58
 * @Version 1.0
 **/
public class DWDApp extends BaseApp {
    public DWDApp(StreamExecutionEnvironment env){
        this.env = env;
    }
    @Override
    public void execute() {
        LogApp logApp = new LogApp(env);
        logApp.execute();

//        DbApp dbApp = new DbApp(env);
//        dbApp.execute();

    }
}
