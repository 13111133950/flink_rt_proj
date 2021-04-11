package com.sx.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sx.common.BaseApp;
import com.sx.util.KafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * @ClassName LogApp
 * @Author Kurisu
 * @Description
 * @Date 2021-3-16 14:17
 * @Version 1.0
 **/
public class LogApp extends BaseApp {
    private static OutputTag<String> pageTag = new OutputTag<String>("page"){};
    private static OutputTag<String> displayTag = new OutputTag<String>("display"){};
    private static final String PAGE_TOPIC = "dwd_page_log";
    private static final String DISPLAY_TOPIC = "dwd_display_log";
    private static final String START_TOPIC = "dwd_start_log";
    public LogApp(StreamExecutionEnvironment env) {
        this.env = env;
    }

    @Override
    public void execute() {
        FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource("ods_base_log", "ods_log");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //TODO 1.识别新老访客
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(JSON::parseObject);
        KeyedStream<JSONObject, String> keyByDS = jsonDS.keyBy(t -> t.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> etlDS = keyByDS.map(new Map());

        //TODO 2.侧输出流实现数据拆分
        SingleOutputStreamOperator<String> splitDS = etlDS.process(new Split());

        //TODO 3.分流数据写入kafka
        DataStream<String> pageDS = splitDS.getSideOutput(pageTag);
        DataStream<String> displayDS = splitDS.getSideOutput(displayTag);

        splitDS.addSink(KafkaUtil.getKafkaSink(START_TOPIC));
        pageDS.addSink(KafkaUtil.getKafkaSink(PAGE_TOPIC));
        displayDS.addSink(KafkaUtil.getKafkaSink(DISPLAY_TOPIC));
    }

    private static class Map extends RichMapFunction<JSONObject, JSONObject> {
        private SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        private ValueState<String> lastDate;
        @Override
        public void open(Configuration parameters) throws Exception {
            lastDate = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastDay", String.class));
        }

        @Override
        public JSONObject map(JSONObject value) throws Exception {
            JSONObject common = value.getJSONObject("common");
            String is_new = common.getString("is_new");
            Long ts = value.getLong("ts");
            String date = df.format(ts);
            if("1".equals(is_new) && lastDate.value()!=null && lastDate.value()!=date){
                common.put("is_new","0");
            }
            return value;
        }
    }

    private static class Split extends ProcessFunction<JSONObject,String> {
        @Override
        public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
            //1、判断是否为启动日志
            JSONObject start = jsonObj.getJSONObject("start");
            if(start!=null && start.size()>0){
                out.collect(jsonObj.toJSONString());
            }else{
                //2、不是启动日志即为页面日志
                ctx.output(pageTag,jsonObj.toString());
                //3、display不为空为曝光日志
                JSONArray displays = jsonObj.getJSONArray("displays");
                if(displays!=null && displays.size()>0){
                    String page_id = jsonObj.getJSONObject("page").getString("page_id");
                    Long ts = jsonObj.getLong("ts");
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject displayObj = displays.getJSONObject(i);
                        displayObj.put("page_id", page_id);
                        displayObj.put("ts", ts);
                        ctx.output(displayTag, displayObj.toJSONString());
                    }
                }
            }
        }
    }
}
