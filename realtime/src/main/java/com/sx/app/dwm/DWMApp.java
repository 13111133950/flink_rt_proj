package com.sx.app.dwm;

import com.sx.common.BaseApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName DWSApp
 * @Author Kurisu
 * @Description
 * @Date 2021-3-22 16:45
 * @Version 1.0
 **/
public class DWMApp extends BaseApp {
    public DWMApp(StreamExecutionEnvironment env){
        this.env = env;
    }
    @Override
    public void execute() {
//        UVApp uvApp = new UVApp(env);
//        uvApp.execute();
//
//        UserJumpApp userJumpApp = new UserJumpApp(env);
//        userJumpApp.execute();

        OrderWideApp orderWideApp = new OrderWideApp(env);
        orderWideApp.execute();

        PayWideApp payWideApp = new PayWideApp(env);
        payWideApp.execute();
    }
}
