package com.sx.app;

import com.sx.app.dwd.DWDApp;
import com.sx.app.dwm.DWMApp;
import com.sx.app.dws.DWSApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @ClassName Application
 * @Author Kurisu
 * @Description
 * @Date 2021-3-16 13:49
 * @Version 1.0
 **/
public class Application {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        //TODO 0.定义Table流环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);
//        //TODO 最大空闲时间   空闲检测
//        tabEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(20));
        env.setParallelism(4);
        //TODO 1 env环境配置
//        //开启checkpoint的时候，设置checkpoint的运行周期，每隔5秒钟进行一次checkpoint
//        env.enableCheckpointing(5000);
//        //当作业被cancel的时候，保留以前的checkpoint，避免数据的丢失
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //设置同一个时间只能有一个检查点，检查点的操作是否可以并行，1不能并行
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // checkpoint的HDFS保存位置
//        if (SystemUtils.IS_OS_WINDOWS) {
//            env.setStateBackend(new FsStateBackend("file:///F:/checkpoint"));
//        } else {
//            env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/flink/checkpoint/"));
//        }
//        // 配置两次checkpoint的最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
//        // 配置checkpoint的超时时长
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//
//        //指定重启策略，默认的是不停的重启
//        //程序出现异常的时候，会进行重启，重启五次，每次延迟3秒钟，如果超过了3次，程序退出
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000));
//        System.setProperty("HADOOP_USER_NAME","atguigu");


        DWDApp dwdApp = new DWDApp(env);
        dwdApp.execute();

        DWMApp dwmApp = new DWMApp(env);
        dwmApp.execute();

        DWSApp dwsApp = new DWSApp(env,tabEnv);
        dwsApp.execute();




        env.execute();
    }
}
