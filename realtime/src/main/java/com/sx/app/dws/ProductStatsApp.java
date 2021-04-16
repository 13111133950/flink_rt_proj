package com.sx.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.sx.bean.OrderWide;
import com.sx.bean.PaymentWide;
import com.sx.bean.ProductStats;
import com.sx.common.BaseApp;
import com.sx.common.GloballConstant;
import com.sx.func.GetAsyncDim;
import com.sx.util.ClickHouseUtil;
import com.sx.util.KafkaUtil;
import com.sx.util.MyDateUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName ProductStatsApp
 * @Author Kurisu
 * @Description
 * @Date 2021-3-24 08:25
 * @Version 1.0
 **/
public class ProductStatsApp extends BaseApp {
    private static final String groupId = "product_stats_app";
    private static final String pageViewSourceTopic = "dwd_page_log";
    private static final String displaySourceTopic = "dwd_display_log";
    private static final String orderWideSourceTopic = "dwm_order_wide";
    private static final String paymentWideSourceTopic = "dwm_payment_wide";
    private static final String cartInfoSourceTopic = "dwd_cart_info";
    private static final String favorInfoSourceTopic = "dwd_favor_info";
    private static final String refundInfoSourceTopic = "dwd_order_refund_info";
    private static final String commentInfoSourceTopic = "dwd_comment_info";
    public ProductStatsApp(StreamExecutionEnvironment env){
        this.env = env;
    }
    @Override
    public void execute() {
        //TODO 1.从kafka流获取相关数据
        DataStreamSource<String> pageViewDStream = env.addSource(KafkaUtil.getKafkaSource(pageViewSourceTopic,groupId));
        DataStreamSource<String> displayDStream = env.addSource(KafkaUtil.getKafkaSource(displaySourceTopic,groupId));
        DataStreamSource<String> favorInfoDStream = env.addSource(KafkaUtil.getKafkaSource(favorInfoSourceTopic,groupId));
        DataStreamSource<String> orderWideDStream= env.addSource(KafkaUtil.getKafkaSource(orderWideSourceTopic,groupId));
        DataStreamSource<String> paymentWideDStream= env.addSource(KafkaUtil.getKafkaSource(paymentWideSourceTopic,groupId));
        DataStreamSource<String> cartInfoDStream= env.addSource(KafkaUtil.getKafkaSource(cartInfoSourceTopic,groupId));
        DataStreamSource<String> refundInfoDStream= env.addSource(KafkaUtil.getKafkaSource(refundInfoSourceTopic,groupId));
        DataStreamSource<String> commentInfoDStream= env.addSource(KafkaUtil.getKafkaSource(commentInfoSourceTopic,groupId));

        //TODO 2.转换成统一结构
        //2.0转换页面流数据
        SingleOutputStreamOperator<ProductStats> pageStatsDstream = pageViewDStream.flatMap(
                new FlatMapFunction<String, ProductStats>() {
                    @Override
                    public void flatMap(String jsonstr, Collector<ProductStats> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonstr);
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        String pageId = pageJsonObj.getString("page_id");
                        Long ts = jsonObj.getLong("ts");
                        if (pageId.equals("good_detail")) {
                            Long skuId = pageJsonObj.getLong("item");
                            ProductStats productStats = ProductStats
                                    .builder().sku_id(skuId).
                                    click_ct(1L).ts(ts).build();
                            out.collect(productStats);
                        }
                    }
                });
        //2.1转换曝光流数据
        SingleOutputStreamOperator<ProductStats> displayStatsDstream = displayDStream.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String jsonstr, Collector<ProductStats> out) throws Exception {
                JSONObject display = JSON.parseObject(jsonstr);
                //判断曝光的是商品
                if (display.getString("item_type").equals("sku_id")) {
                    Long skuId = display.getLong("item");
                    Long ts = display.getLong("ts");
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(skuId).display_ct(1L).ts(ts).build();
                    out.collect(productStats);
                }
            }
        });

        //2.2转换下单流数据
        SingleOutputStreamOperator<ProductStats> orderWideStatsDstream = orderWideDStream.map(
                json -> {
                    OrderWide orderWide = JSON.parseObject(json, OrderWide.class);
                    String create_time = orderWide.getCreate_time();
                    Long ts = MyDateUtil.getTs(create_time);
                    return ProductStats.builder().sku_id(orderWide.getSku_id())
                            .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                            .order_sku_num(orderWide.getSku_num())
                            .order_amount(orderWide.getSplit_total_amount()).ts(ts).build();
                });

        //2.3转换收藏流数据
        SingleOutputStreamOperator<ProductStats> favorStatsDstream = favorInfoDStream.map(
                json -> {
                    JSONObject favorInfo = JSON.parseObject(json);
                    Long ts = MyDateUtil.getTs(favorInfo.getString("create_time"));
                    return ProductStats.builder().sku_id(favorInfo.getLong("sku_id"))
                            .favor_ct(1L).ts(ts).build();
                });

        //2.4转换购物车流数据
        SingleOutputStreamOperator<ProductStats> cartStatsDstream = cartInfoDStream.map(
                json -> {
                    JSONObject cartInfo = JSON.parseObject(json);
                    Long ts = MyDateUtil.getTs(cartInfo.getString("create_time"));
                    return ProductStats.builder().sku_id(cartInfo.getLong("sku_id"))
                            .cart_ct(1L).ts(ts).build();
                });

        //2.5转换支付流数据
        SingleOutputStreamOperator<ProductStats> paymentStatsDstream = paymentWideDStream.map(
                json -> {
                    PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
                    Long ts = MyDateUtil.getTs(paymentWide.getPayment_create_time());
                    return ProductStats.builder().sku_id(paymentWide.getSku_id())
                            .payment_amount(paymentWide.getSplit_total_amount())
                            .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                            .ts(ts).build();
                });

        //2.6转换退款流数据
        SingleOutputStreamOperator<ProductStats> refundStatsDstream = refundInfoDStream.map(
                json -> {
                    JSONObject refundJsonObj = JSON.parseObject(json);
                    Long ts = MyDateUtil.getTs(refundJsonObj.getString("create_time"));
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(refundJsonObj.getLong("sku_id"))
                            .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                            .refundOrderIdSet(
                                    new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                            .ts(ts).build();
                    return productStats;

                });

        //2.7转换评价流数据
        SingleOutputStreamOperator<ProductStats> commonInfoStatsDstream = commentInfoDStream.map(
                json -> {
                    JSONObject commonJsonObj = JSON.parseObject(json);
                    Long ts = MyDateUtil.getTs(commonJsonObj.getString("create_time"));
                    Long goodCt = GloballConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ? 1L : 0L;
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(commonJsonObj.getLong("sku_id"))
                            .comment_ct(1L).good_comment_ct(goodCt).ts(ts).build();
                    return productStats;
                });
        //TODO 3.UNION合流，添加watermark
        DataStream<ProductStats> productStatDetailDStream = pageStatsDstream.union(
                displayStatsDstream,orderWideStatsDstream, cartStatsDstream,
                paymentStatsDstream, refundStatsDstream,favorStatsDstream,
                commonInfoStatsDstream)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forMonotonousTimestamps()
                .withTimestampAssigner((product,ts)->product.getTs()));

        //TODO 4.分组开窗聚合
        SingleOutputStreamOperator<ProductStats> productStatsDstream = productStatDetailDStream
                .keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new Red(), new Win());

        //TODO 5.补充商品维度信息
        //5.1 补充SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDstream =
                AsyncDataStream.unorderedWait(productStatsDstream,
                        new GetAsyncDim<ProductStats>("DIM_SKU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject){
                                productStats.setSku_name(jsonObject.getString("SKU_NAME"));
                                productStats.setSku_price(jsonObject.getBigDecimal("PRICE"));
                                productStats.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                                productStats.setSpu_id(jsonObject.getLong("SPU_ID"));
                                productStats.setTm_id(jsonObject.getLong("TM_ID"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSku_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //5.2 补充SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream =
                AsyncDataStream.unorderedWait(productStatsWithSkuDstream,
                        new GetAsyncDim<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);


        //5.3 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream =
                AsyncDataStream.unorderedWait(productStatsWithSpuDstream,
                        new GetAsyncDim<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //5.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream =
                AsyncDataStream.unorderedWait(productStatsWithCategory3Dstream,
                        new GetAsyncDim<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        productStatsWithTmDstream.print("》》》");
        //TODO 6.写入clickhouse
        productStatsWithTmDstream.addSink(
                ClickHouseUtil.<ProductStats>getJdbcSink(
                        "insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 7.写回到Kafka的dws层
        productStatsWithTmDstream
                .map(productStat->JSON.toJSONString(productStat,new SerializeConfig(true)))
                .addSink(KafkaUtil.getKafkaSink("dws_product_stats"));

    }

    private static class Red implements ReduceFunction<ProductStats> {
        @Override
        public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
            stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
            stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
            stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
            stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
            stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
            stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
            stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
            stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
            stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

            stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
            stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
            stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

            stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
            stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

            stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
            stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
            return stats1;
        }
    }

    private static class Win implements WindowFunction<ProductStats,ProductStats,Long, TimeWindow> {
        @Override
        public void apply(Long key, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for (ProductStats productStats : input) {
                productStats.setStt(simpleDateFormat.format(window.getStart()));
                productStats.setEdt(simpleDateFormat.format(window.getEnd()));
                productStats.setTs(new Date().getTime());
                out.collect(productStats);
            }
        }
    }
}
