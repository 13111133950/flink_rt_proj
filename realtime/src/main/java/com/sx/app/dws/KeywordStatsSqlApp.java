package com.sx.app.dws;

import com.sx.bean.KeywordStats;
import com.sx.common.BaseApp;
import com.sx.common.GloballConstant;
import com.sx.func.KeywordUDTF;
import com.sx.util.ClickHouseUtil;
import com.sx.util.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName KeywordStatsSqlApp
 * @Author Kurisu
 * @Description
 * @Date 2021-4-2 16:12
 * @Version 1.0
 **/
public class KeywordStatsSqlApp extends BaseApp {
    public KeywordStatsSqlApp(StreamExecutionEnvironment env, StreamTableEnvironment tabEnv) {
        this.env = env;
        this.tabEnv = tabEnv;
    }

    @Override
    public void execute() {
        //TODO 2.注册自定义函数
        tabEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        //TODO 3.将数据源定义为动态表
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";

/*
CREATE TABLE page_view (
    common MAP<STRING,STRING>,
    page MAP<STRING,STRING>,
    ts BIGINT,
    rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')),
    WATERMARK FOR  rowtime  AS  rowtime - INTERVAL '2' SECOND)
    WITH (
 */

        tabEnv.executeSql("CREATE TABLE page_view " +
                "(common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>,ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
                "WATERMARK FOR  rowtime  AS  rowtime - INTERVAL '2' SECOND) " +
                "WITH ("+ KafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId)+")");


        //TODO 4.过滤数据
        Table fullwordView = tabEnv.sqlQuery("select page['item'] fullword ," +
                "rowtime from page_view  " +
                "where page['page_id']='good_list' " +
                "and page['item'] IS NOT NULL ");


        //TODO 5.利用udtf将数据拆分
        Table keywordView = tabEnv.sqlQuery("select keyword,rowtime  from " + fullwordView + " ," +
                " LATERAL TABLE(ik_analyze(fullword)) as T(keyword)");

        //TODO 6.根据各个关键词出现次数进行ct
        Table keywordStatsSearch = tabEnv.sqlQuery("select keyword,count(*) ct, '"
                + GloballConstant.KEYWORD_SEARCH + "' source ," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts from   " + keywordView
                + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword");

        //TODO 7.转换为数据流
        DataStream<KeywordStats> keywordStatsSearchDataStream =
                tabEnv.<KeywordStats>toAppendStream(keywordStatsSearch, KeywordStats.class);

        keywordStatsSearchDataStream.print("DWS关键词>>>");
        //TODO 8.写入到ClickHouse
        keywordStatsSearchDataStream.addSink(
                ClickHouseUtil.<KeywordStats>getJdbcSink(
                        "insert into keyword_stats(keyword,ct,source,stt,edt,ts)  " +
                                " values(?,?,?,?,?,?)"));
    }
}
