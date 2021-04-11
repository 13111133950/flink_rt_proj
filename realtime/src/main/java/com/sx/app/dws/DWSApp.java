package com.sx.app.dws;

import com.sx.common.BaseApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName DWSApp
 * @Author Kurisu
 * @Description
 * @Date 2021-3-23 19:30
 * @Version 1.0
 **/
public class DWSApp extends BaseApp {
    public DWSApp(StreamExecutionEnvironment env, StreamTableEnvironment tabEnv){
        this.env = env;
        this.tabEnv = tabEnv;
    }
    @Override
    public void execute() {
        VisitorStatsApp  visitorStatsApp = new VisitorStatsApp (env);
        visitorStatsApp.execute();

//        ProductStatsApp productStatsApp = new ProductStatsApp(env);
//        productStatsApp.execute();
//
//        ProvinceStatsSqlApp provinceStatsSqlApp = new ProvinceStatsSqlApp(env, tabEnv);
//        provinceStatsSqlApp.execute();
//
//        KeywordStatsSqlApp keywordStatsSqlApp = new KeywordStatsSqlApp(env, tabEnv);
//        keywordStatsSqlApp.execute();
//
//        KeywordStateProductSqlApp keywordStateProductSqlApp = new KeywordStateProductSqlApp(env, tabEnv);
//        keywordStateProductSqlApp.execute();


    }
}
