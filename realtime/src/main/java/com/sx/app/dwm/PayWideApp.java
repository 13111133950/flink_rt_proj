package com.sx.app.dwm;

import com.alibaba.fastjson.JSON;
import com.sx.bean.OrderWide;
import com.sx.bean.PaymentInfo;
import com.sx.bean.PaymentWide;
import com.sx.common.BaseApp;
import com.sx.util.KafkaUtil;
import com.sx.util.MyDateUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName PayWideApp
 * @Author Kurisu
 * @Description
 * @Date 2021-3-22 16:45
 * @Version 1.0
 **/
public class PayWideApp extends BaseApp {
    private static final String groupId = "payment_wide_group";
    private static final String paymentInfoSourceTopic = "dwd_payment_info";
    private static final String orderWideSourceTopic = "dwm_order_wide";
    private static final String paymentWideSinkTopic = "dwm_payment_wide";
    public PayWideApp(StreamExecutionEnvironment env){
        this.env = env;
    }
    @Override
    public void execute() {
        //TODO 1.从kafka获取 订单宽表 支付表数据
        DataStreamSource<String> paySourceDS = env.addSource(KafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideSourceDS = env.addSource(KafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));

        //TODO 2.结构转换 封装为POJO 增加watermark
        SingleOutputStreamOperator<PaymentInfo> payDS = paySourceDS
                .map(jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((pay,timestamp) -> MyDateUtil.getTs(pay.getCallback_time())));
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideSourceDS.map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((order,timestamp) -> MyDateUtil.getTs(order.getCreate_time())));


        //TODO 3.双流JOIN
        SingleOutputStreamOperator<String> joinDS = payDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.seconds(-1800), Time.seconds(0))
                .process(new JoinFunc());
//        joinDS.print("payJoin>>");
        //TODO 4.写入kafka
        joinDS.addSink(KafkaUtil.getKafkaSink(paymentWideSinkTopic));

    }

    private static class JoinFunc extends ProcessJoinFunction<PaymentInfo, OrderWide, String> {
        @Override
        public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<String> out) throws Exception {
            out.collect(JSON.toJSONString(new PaymentWide(left,right)));
        }
    }
}
