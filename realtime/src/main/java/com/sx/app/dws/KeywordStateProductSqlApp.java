package com.sx.app.dws;

import com.sx.bean.KeywordStats;
import com.sx.common.BaseApp;
import com.sx.func.KeywordProductC2RUDTF;
import com.sx.util.ClickHouseUtil;
import com.sx.util.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName KeywordStateProductSqlApp
 * @Author Kurisu
 * @Description
 * @Date 2021-4-2 16:11
 * @Version 1.0
 **/
public class KeywordStateProductSqlApp extends BaseApp {
    public KeywordStateProductSqlApp(StreamExecutionEnvironment env, StreamTableEnvironment tabEnv) {
        this.env = env;
        this.tabEnv = tabEnv;
    }

    @Override
    public void execute() {
        //TODO 2.注册自定义函数
        tabEnv.createTemporarySystemFunction("keywordProductC2R",  KeywordProductC2RUDTF.class);
        //tabEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //TODO 3.将数据源定义为动态表
        String groupId = "keyword_stats_app";
        String productStatsSourceTopic ="dws_product_stats";

        tabEnv.executeSql("CREATE TABLE product_stats (spu_name STRING, " +
                "click_ct BIGINT," +
                "cart_ct BIGINT," +
                "order_ct BIGINT ," +
                "stt STRING,edt STRING ) " +
                "  WITH ("+ KafkaUtil.getKafkaDDL(productStatsSourceTopic,groupId)+")");

        //TODO 6.聚合计数
        Table keywordStatsProduct = tabEnv.sqlQuery("select keyword,ct,source, " +
                "DATE_FORMAT(stt,'yyyy-MM-dd HH:mm:ss')  stt," +
                "DATE_FORMAT(edt,'yyyy-MM-dd HH:mm:ss') as edt, " +
                "UNIX_TIMESTAMP()*1000 ts from product_stats  , " +
                "LATERAL TABLE(ik_analyze(spu_name)) as T(keyword) ," +
                "LATERAL TABLE(keywordProductC2R( click_ct ,cart_ct,order_ct)) as T2(ct,source)");

        //TODO 7.转换为数据流
        DataStream<KeywordStats> keywordStatsProductDataStream =
                tabEnv.<KeywordStats>toAppendStream(keywordStatsProduct, KeywordStats.class);

        keywordStatsProductDataStream.print("DWS商品关键词>");
        //TODO 8.写入到ClickHouse
        keywordStatsProductDataStream.addSink(
                ClickHouseUtil.<KeywordStats>getJdbcSink(
                        "insert into keyword_stats(keyword,ct,source,stt,edt,ts)  " +
                                "values(?,?,?,?,?,?)"));
    }
}
