package com.sx.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sx.bean.VisitorStats;
import com.sx.common.BaseApp;
import com.sx.util.ClickHouseUtil;
import com.sx.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @ClassName VisitTopicApp
 * @Author Kurisu
 * @Description
 * @Date 2021-3-23 19:35
 * @Version 1.0
 **/
public class VisitorStatsApp  extends BaseApp {
    private static final String pageViewSourceTopic = "dwd_page_log";
    private static final String uniqueVisitSourceTopic = "dwm_unique_visit";
    private static final String userJumpDetailSourceTopic = "dwm_user_jump_detail";
    private static final String groupId = "visitor_stats_app";
    public VisitorStatsApp (StreamExecutionEnvironment env){
        this.env = env;
    }
    @Override
    public void execute() {
        //TODO 1.从kafka获取相关数据
        DataStreamSource<String> pageViewDStream = env.addSource(KafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> uniqueVisitDStream = env.addSource(KafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> userJumpDStream = env.addSource(KafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId));

        //TODO 2.封装为同一结构 并指定watermark
        //TODO 2.对读取的流进行结构转换
        //2.1 转换pv流/sv
        SingleOutputStreamOperator<VisitorStats> pageViewStatsDstream = pageViewDStream.map(
                json -> {
                    JSONObject jsonObj = JSON.parseObject(json);
                    VisitorStats visitorStats = new VisitorStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            0L, 1L, 0L, 0L, jsonObj.getJSONObject("page").getLong("during_time"), jsonObj.getLong("ts"));

                    //判断是否为sv
                    String lastPage = jsonObj.getJSONObject("page").getString("last_page_id");
                    if(lastPage!=null && lastPage.length()>0){
                        visitorStats.setSv_ct(1l);
                    }
                    return visitorStats;
                });

        //2.2转换uv流
        SingleOutputStreamOperator<VisitorStats> uniqueVisitStatsDstream = uniqueVisitDStream.map(
                json -> {
                    JSONObject jsonObj = JSON.parseObject(json);
                    return new VisitorStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            1L, 0L, 0L, 0L, 0L, jsonObj.getLong("ts"));
                });

        //2.3 转换跳转流
        SingleOutputStreamOperator<VisitorStats> userJumpStatDstream = userJumpDStream.map(json -> {
            JSONObject jsonObj = JSON.parseObject(json);
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L, 0L, 0L, 1L, 0L, jsonObj.getLong("ts"));
        });
        //TODO 3.UNION多条流  指定水位线
        DataStream<VisitorStats> unionDS = pageViewStatsDstream.union(uniqueVisitStatsDstream, userJumpStatDstream)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((visit,ts)->visit.getTs()));

        //TODO 4.分组聚合
        SingleOutputStreamOperator<VisitorStats> aggDS = unionDS.keyBy(new KeySelector<VisitorStats, Tuple4<String,String,String,String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return Tuple4.of(value.getVc(),value.getCh(),value.getAr(),value.getIs_new());
            }
        })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new Reduce(), new Win());

        aggDS.print("访客主题");

        //TODO 5.写入clickhouse
        aggDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));
    }
    private static class Reduce implements ReduceFunction<VisitorStats> {
        @Override
        public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
            value1.setUv_ct(value1.getUv_ct()+value2.getUv_ct());
            value1.setUj_ct(value1.getUj_ct()+value2.getUj_ct());
            value1.setSv_ct(value1.getSv_ct()+value2.getSv_ct());
            value1.setDur_sum(value1.getDur_sum()+value2.getDur_sum());
            return value1;
        }
    }

    private class Win implements WindowFunction<VisitorStats,VisitorStats,Tuple4<String,String,String,String>, TimeWindow> {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        @Override
        public void apply(Tuple4<String, String, String, String> key, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
            VisitorStats visitorStats = input.iterator().next();
            visitorStats.setStt(df.format(window.getStart()));
            visitorStats.setEdt(df.format(window.getEnd()));
            out.collect(visitorStats);
        }
    }


}
