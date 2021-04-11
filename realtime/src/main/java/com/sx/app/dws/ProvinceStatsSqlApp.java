package com.sx.app.dws;

import com.sx.bean.ProvinceStats;
import com.sx.common.BaseApp;
import com.sx.util.ClickHouseUtil;
import com.sx.util.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName ProvinceStatsSqlApp
 * @Author Kurisu
 * @Description
 * @Date 2021-4-2 16:08
 * @Version 1.0
 **/
public class ProvinceStatsSqlApp extends BaseApp {
    public ProvinceStatsSqlApp(StreamExecutionEnvironment env, StreamTableEnvironment tabEnv) {
        this.env = env;
        this.tabEnv = tabEnv;
    }

    @Override
    public void execute() {
        //TODO 2.把数据源定义为动态表
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";
/*
CREATE TABLE ORDER_WIDE (
    province_id BIGINT,
    province_name STRING,
    province_area_code STRING,
    province_iso_code STRING,
    province_3166_2_code STRING,
    order_id STRING,
    split_total_amount DOUBLE,
    create_time STRING,
    rowtime AS TO_TIMESTAMP(create_time),
    WATERMARK FOR  rowtime  AS rowtime)
    WITH()
 */
        tabEnv.executeSql("CREATE TABLE ORDER_WIDE ( \n" +
                "    province_id BIGINT,\n" +
                "    province_name STRING,\n" +
                "    province_area_code STRING,\n" +
                "    province_iso_code STRING,\n" +
                "    province_3166_2_code STRING,\n" +
                "    order_id STRING,\n" +
                "    split_total_amount DECIMAL(18,2),\n" +
                "    create_time STRING,\n" +
                "    rowtime AS TO_TIMESTAMP(create_time),\n" +
                "    WATERMARK FOR  rowtime  AS rowtime )\n" +
                "    WITH(" + KafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

/*
select
    DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt,
    DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt ,
    province_id,
    province_name,
    province_area_code area_code,
    province_iso_code iso_code ,
    province_3166_2_code iso_3166_2 ,
    COUNT( DISTINCT  order_id) order_count, sum(split_total_amount) order_amount,
    UNIX_TIMESTAMP()*1000 ts
from  ORDER_WIDE
group by  TUMBLE(rowtime, INTERVAL '10' SECOND ),
    province_id,
    province_name,
    province_area_code,
    province_iso_code,
    province_3166_2_code
 */

        //TODO 3.聚合计算
        Table provinceStateTable = tabEnv.sqlQuery("select\n" +
                "    DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "    DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt ,\n" +
                "    province_id,\n" +
                "    province_name,\n" +
                "    province_area_code area_code,\n" +
                "    province_iso_code iso_code ,\n" +
                "    province_3166_2_code iso_3166_2 ,\n" +
                "    COUNT( DISTINCT  order_id) order_count, sum(split_total_amount) order_amount,\n" +
                "    UNIX_TIMESTAMP()*1000 ts\n" +
                "from  ORDER_WIDE \n" +
                "group by  TUMBLE(rowtime, INTERVAL '10' SECOND ),\n" +
                "    province_id,\n" +
                "    province_name,\n" +
                "    province_area_code,\n" +
                "    province_iso_code,\n" +
                "    province_3166_2_code ");

        //TODO 4.转换为数据流
        DataStream<ProvinceStats> provinceStatsDataStream =
                tabEnv.toAppendStream(provinceStateTable, ProvinceStats.class);

        provinceStatsDataStream.print("地区主题>>>");

        //TODO 5.写入到lickHouse
        provinceStatsDataStream.addSink(ClickHouseUtil.
                <ProvinceStats>getJdbcSink("insert into  province_stats  values(?,?,?,?,?,?,?,?,?,?)"));
    }
}
