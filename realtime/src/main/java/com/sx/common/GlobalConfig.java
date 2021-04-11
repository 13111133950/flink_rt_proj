package com.sx.common;

/**
 * @ClassName GlobalConfig
 * @Author Kurisu
 * @Description
 * @Date 2021-3-16 14:09
 * @Version 1.0
 **/
public class GlobalConfig {
    public static final String BOOTSTRAP_SERVER = "hadoop102:9092";
    public static final String MYSQL_USER = "root";
    public static final String MYSQL_PWD = "123456";


    //hbase相关
    public static final String HBASE_SCHEMA = "REALTIME";
    public static final String PHOENIX_SERVER="jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    //clickhouse相关
    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hadoop102:8123/default";
}
