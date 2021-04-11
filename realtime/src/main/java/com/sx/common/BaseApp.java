package com.sx.common;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName BaseApp
 * @Author Kurisu
 * @Description
 * @Date 2021-3-16 13:53
 * @Version 1.0
 **/
public abstract class BaseApp {
    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tabEnv;
    public abstract void execute();
}
