package com.sx.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.sx.common.BaseApp;
import com.sx.util.KafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ClassName UniqueVisitApp
 * @Author Kurisu
 * @Description
 * @Date 2021-3-8 13:49
 * @Version 1.0
 **/
public class UniqueVisitApp extends BaseApp {
    private StreamExecutionEnvironment env;

    public UniqueVisitApp(StreamExecutionEnvironment env) {
        this.env = env;
    }
    @Override
    public void execute() {
        //TODO 2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "unique_visit_app_group";
        String sinkTopic = "dwm_unique_visit";
        FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);

        //TODO 3.对读取到的数据进行结构的换换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(JSON::parseObject);

        //TODO 4.按照设备id进行分组
        KeyedStream<JSONObject, String> keybyWithMidDS = jsonObjDS.keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );
        //TODO 5.过滤得到UV
        SingleOutputStreamOperator<JSONObject> filteredDS = keybyWithMidDS.filter(
                new RichFilterFunction<JSONObject>() {
                    //定义状态
                    ValueState<String> lastVisitDateState = null;
                    //定义日期工具类
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化日期工具类
                        sdf = new SimpleDateFormat("yyyyMMdd");
                        //初始化状态
                        ValueStateDescriptor<String> lastVisitDateStateDes =
                                new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        //因为我们统计的是日活DAU，所以状态数据只在当天有效 ，过了一天就可以失效掉
                        //long ts = ((System.currentTimeMillis() + 8*60*60*1000) / (24*60*60*1000) + 1) * (24*60*60*1000) - 8*60*60*1000;
                        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                        lastVisitDateStateDes.enableTimeToLive(stateTtlConfig);
                        this.lastVisitDateState = getRuntimeContext().getState(lastVisitDateStateDes);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        //首先判断当前页面是否从别的页面跳转过来的
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId != null && lastPageId.length() > 0) {
                            return false;
                        }

                        //获取当前访问时间
                        Long ts = jsonObj.getLong("ts");
                        //将当前访问时间戳转换为日期字符串
                        String logDate = sdf.format(new Date(ts));
                        //获取状态日期
                        String lastVisitDate = lastVisitDateState.value();

                        //用当前页面的访问时间和状态时间进行对比
                        if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                            System.out.println("已访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                            return false;
                        } else {
                            //System.out.println("未访问：lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                            lastVisitDateState.update(logDate);
                            return true;
                        }
                    }
                }
        );

        //filteredDS.print(">>>>>");

        //TODO 6. 向kafka中写回，需要将json转换为String
        //6.1 json->string
        SingleOutputStreamOperator<String> kafkaDS = filteredDS.map(JSONAware::toJSONString);

        //6.2 写回到kafka的dwm层
        kafkaDS.addSink(KafkaUtil.getKafkaSink(sinkTopic));
    }
}
