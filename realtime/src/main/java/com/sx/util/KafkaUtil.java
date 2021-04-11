package com.sx.util;

import com.sx.common.GlobalConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @ClassName KafkaUtil
 * @Author Kurisu
 * @Description
 * @Date 2021-3-16 14:04
 * @Version 1.0
 **/
public class KafkaUtil {
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String group){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,GlobalConfig.BOOTSTRAP_SERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),props);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        return new FlinkKafkaProducer<String>(GlobalConfig.BOOTSTRAP_SERVER,topic,new SimpleStringSchema());
    }

    public static<T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> mySchema){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,GlobalConfig.BOOTSTRAP_SERVER);
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,5*60*1000+"");
        return new FlinkKafkaProducer<T>(
                "defalut_topic",
                mySchema,
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }

    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic,String groupId){
        String ddl="'connector' = 'kafka', " +
                " 'topic' = '"+topic+"',"   +
                " 'properties.bootstrap.servers' = '"+ GlobalConfig.BOOTSTRAP_SERVER +"', " +
                " 'properties.group.id' = '"+groupId+ "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset'  ";
        return  ddl;
    }
}
