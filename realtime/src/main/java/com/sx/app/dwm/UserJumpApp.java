package com.sx.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.sx.common.BaseApp;
import com.sx.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @ClassName UserJumpApp
 * @Author Kurisu
 * @Description
 * @Date 2021-3-22 16:45
 * @Version 1.0
 **/
public class UserJumpApp extends BaseApp {
    private final String groupId = "userJumpDetailApp";
    private final String sourceTopic = "dwd_page_log";
    private final String sinkTopic = "dwm_user_jump_detail";
    private static final OutputTag<String> timeOutTag = new OutputTag<String>("timeout"){};
    public UserJumpApp(StreamExecutionEnvironment env){
        this.env = env;
    }
    @Override
    public void execute() {
        //TODO 1.从kafka读取数据
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaSource(sourceTopic, groupId));

        //TODO 2.转换成JSON
        KeyedStream<JSONObject, String> transDS = kafkaDS.map(JSONObject::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((obj,timestamp) -> obj.getLong("ts")))
                .keyBy(t -> t.getJSONObject("common").getString("mid"));

        //TODO 3.使用CEP筛选符合条件的数据
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String lastPage = jsonObj.getJSONObject("page").getString("last_page_id");
                        return lastPage == null || lastPage.length() == 0;
                    }
                }).next("second")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        return jsonObj.getJSONObject("page") != null && jsonObj.getJSONObject("page").size() > 0;
                    }
                }).within(Time.seconds(5));
        //将Pattern应用到流上
        SingleOutputStreamOperator<String> CEPDS = CEP.pattern(transDS, pattern)
                .process(new PatternFun());

        //TODO 4.取数筛选后侧输出流的数据  写入kafka
        DataStream<String> sideOutputDS = CEPDS.getSideOutput(timeOutTag);
        sideOutputDS.print("跳出记录>>>>>");
        sideOutputDS.addSink(KafkaUtil.getKafkaSink(sinkTopic));

    }

    private static class PatternFun extends PatternProcessFunction<JSONObject,String> implements TimedOutPartialMatchHandler<JSONObject> {
        @Override
        public void processMatch(Map<String, List<JSONObject>> match, Context ctx, Collector<String> out) throws Exception {
            //不做处理
        }

        @Override
        public void processTimedOutMatch(Map<String, List<JSONObject>> match, Context ctx) throws Exception {
            ctx.output(timeOutTag,match.get("first").get(0).toJSONString());
        }
    }
}
