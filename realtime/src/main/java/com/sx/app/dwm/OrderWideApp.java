package com.sx.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sx.bean.OrderDetail;
import com.sx.bean.OrderInfo;
import com.sx.bean.OrderWide;
import com.sx.common.BaseApp;
import com.sx.func.GetAsyncDim;
import com.sx.util.KafkaUtil;
import com.sx.util.MyDateUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName OrderWideApp
 * @Author Kurisu
 * @Description
 * @Date 2021-3-22 16:45
 * @Version 1.0
 **/
public class OrderWideApp extends BaseApp {
    private static final String orderInfoSourceTopic = "dwd_order_info";
    private static final String orderDetailSourceTopic = "dwd_order_detail";
    private static final String orderWideSinkTopic = "dwm_order_wide";
    private static final String groupId = "order_wide_group";

    public OrderWideApp(StreamExecutionEnvironment env) {
        this.env = env;
    }

    @Override
    public void execute() {
        //TODO 1.从kafka读取 订单、订单明细数据
        DataStream<String> orderInfoDS = env.addSource(KafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId));
        DataStream<String> orderDetailDS = env.addSource(KafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId));

        //TODO 2.将jsonStr封装为POJO   增加时间戳供watermark使用（精确到秒及以下）
        DataStream<OrderInfo> orderInfoDStream = orderInfoDS.map(new order())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((order,timestamp)->order.getCreate_ts()));
        DataStream<OrderDetail> orderDetailDStream = orderDetailDS.map(new detail())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((detail,timestamp)->detail.getCreate_ts()));

        //TODO 3.双流JOIN
        SingleOutputStreamOperator<OrderWide> orderWideDstream = orderInfoDStream.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDStream.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new JoinFunc());

        //TODO 4.封装异步IO工具类关联各维度

        //TODO 5.关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDstream = AsyncDataStream.unorderedWait(
                orderWideDstream, new GetAsyncDim<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject)  {
                        SimpleDateFormat formattor = new SimpleDateFormat("yyyy-MM-dd");
                        String birthday = jsonObject.getString("BIRTHDAY");
                        Date date = null;
                        try {
                            date = formattor.parse(birthday);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                        Long curTs = System.currentTimeMillis();
                        Long betweenMs = curTs - date.getTime();
                        Long ageLong = betweenMs / 1000L / 60L / 60L / 24L / 365L;
                        Integer age = ageLong.intValue();
                        orderWide.setUser_age(age);
                        orderWide.setUser_gender(jsonObject.getString("GENDER"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getUser_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 6.关联省市维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDstream = AsyncDataStream.unorderedWait(
                orderWideWithUserDstream, new GetAsyncDim<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setProvince_name(jsonObject.getString("NAME"));
                        orderWide.setProvince_3166_2_code(jsonObject.getString("ISO_3166_2"));
                        orderWide.setProvince_iso_code(jsonObject.getString("ISO_CODE"));
                        orderWide.setProvince_area_code(jsonObject.getString("AREA_CODE"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getProvince_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 7.关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDstream = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDstream, new GetAsyncDim<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject)  {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 8.关联SPU商品维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDstream = AsyncDataStream.unorderedWait(
                orderWideWithSkuDstream, new GetAsyncDim<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject)  {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 9.关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3Dstream = AsyncDataStream.unorderedWait(
                orderWideWithSpuDstream, new GetAsyncDim<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject)  {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 10.关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDstream = AsyncDataStream.unorderedWait(
                orderWideWithCategory3Dstream, new GetAsyncDim<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

//        orderWideWithTmDstream.print("OrderWide>>>");
        //TODO 11.将订单和订单明细Join之后以及维度关联的宽表写到Kafka的dwm层
        orderWideWithTmDstream.map(JSON::toJSONString)
                .addSink(KafkaUtil.getKafkaSink(orderWideSinkTopic));
    }

    private static class order extends RichMapFunction<String, OrderInfo> {
        @Override
        public OrderInfo map(String jsonString)  {
            OrderInfo orderInfo = JSON.parseObject(jsonString, OrderInfo.class);
            orderInfo.setCreate_ts(MyDateUtil.getTs(orderInfo.getCreate_time()));
            return orderInfo;
        }
    }

    private static class detail extends RichMapFunction<String, OrderDetail> {
        @Override
        public OrderDetail map(String jsonString) {
            OrderDetail orderDetail = JSON.parseObject(jsonString, OrderDetail.class);
            orderDetail.setCreate_ts(MyDateUtil.getTs(orderDetail.getCreate_time()));
            return orderDetail;
        }
    }

    private static class JoinFunc extends ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide> {
        @Override
        public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
            out.collect(new OrderWide(left,right));
        }
    }
}
