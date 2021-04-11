package com.sx.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sx.common.BaseApp;
import com.sx.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Charsets;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

/**
 * @ClassName UVApp
 * @Author Kurisu
 * @Description
 * @Date 2021-3-22 16:44
 * @Version 1.0
 **/
public class UVApp extends BaseApp {
    private final String groupId = "uv_app";
    private final String sourceTopic = "dwd_page_log";
    private final String sinkTopic = "dwm_unique_visit";

    public UVApp(StreamExecutionEnvironment env) {
        this.env = env;
    }

    @Override
    public void execute() {
        //TODO 1.从Kafka中读取数据
        FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //TODO 2.计算UV
        //2.1转换成JSON，并过滤last_page_id不为空的数据，flatMap = map+filter
        SingleOutputStreamOperator<JSONObject> ETLDS = kafkaDS.flatMap(new Trans())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((obj,timestamp) -> obj.getLong("ts")));

        //2.2利用全窗口+布隆过滤器去重
        //触发器触发条件：每条数据触发计算，触发计算后移除数据，减少状态存储压力
        //为什么用windowAll  为什么不keyBy+普通开窗
        SingleOutputStreamOperator<String> distinctDS = ETLDS.windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(new MyTrigger())
                .evictor(new MyEvictor())
                .process(new Distinct());

        //TODO 3.写入DWM层
        distinctDS.print("去重后的UV>>>");
        distinctDS.addSink(KafkaUtil.getKafkaSink(sinkTopic));
    }

    private static class Distinct extends ProcessAllWindowFunction<JSONObject,String, TimeWindow> {
        private ValueState<BloomFilter> bfState;

        @Override
        public void open(Configuration parameters) throws Exception {
            bfState = getRuntimeContext().getState(new ValueStateDescriptor<BloomFilter>("bfState", Types.GENERIC(BloomFilter.class)));
        }

        @Override
        public void process(Context context, Iterable<JSONObject> elements, Collector<String> out) throws Exception {
//            测试Evictor是否生效
//            elements.forEach(System.out::println);
//            System.out.println("--------------");


            JSONObject jsonObj = elements.iterator().next();
            BloomFilter bloomFilter = bfState.value();
            String mid = jsonObj.getJSONObject("common").getString("mid");
            if (bloomFilter == null) {
                bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 1000000, 0.001);
            }
            boolean isNotExsist = bloomFilter.put(mid);
            if (isNotExsist) {
                out.collect(jsonObj.toJSONString());
                bfState.update(bloomFilter);
            }{
                System.out.println(mid+"已登录<<<<<<<<<<<<<<<<<<");
            }
        }
    }

    private static class Trans implements FlatMapFunction<String, JSONObject> {
        @Override
        public void flatMap(String value, Collector<JSONObject> out) throws Exception {
            JSONObject obj = JSON.parseObject(value);
            String lastPage = obj.getJSONObject("page").getString("last_page_id");
            if (lastPage == null || lastPage.length() == 0) {
                out.collect(obj);
            }
        }
    }

    private static class MyTrigger extends Trigger<JSONObject,TimeWindow> {
        @Override
        public TriggerResult onElement(JSONObject element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    private static class MyEvictor implements Evictor<JSONObject, TimeWindow> {
        @Override
        public void evictBefore(Iterable<TimestampedValue<JSONObject>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<JSONObject>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
            Iterator<TimestampedValue<JSONObject>> iterator = elements.iterator();
            iterator.next();
            iterator.remove();

        }
    }
}
