package com.sx.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.sx.bean.TableProcess;
import com.sx.common.BaseApp;
import com.sx.common.GlobalConfig;
import com.sx.util.DimUtil;
import com.sx.util.KafkaUtil;
import io.debezium.data.Envelope;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @ClassName DbApp
 * @Author Kurisu
 * @Description
 * @Date 2021-3-16 14:18
 * @Version 1.0
 **/
public class DbApp extends BaseApp {
    private static MapStateDescriptor<String, TableProcess> broadState = new MapStateDescriptor<>("broadstate", String.class, TableProcess.class);
    private static OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase"){};
    public DbApp(StreamExecutionEnvironment env) {
        this.env = env;
    }

    @Override
    public void execute() {
        // TODO 1 从数据源获取数据
        FlinkKafkaConsumer<String> kafkaSource = KafkaUtil.getKafkaSource("ods_base_db_m", "ods_db");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        // TODO 2 ETL
        SingleOutputStreamOperator<JSONObject> etlDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                if (jsonObject.getString("table") != null && jsonObject.getJSONObject("data") != null
                        && jsonObject.getString("data").length() > 3) {
                    out.collect(jsonObject);
                }
            }
        });

        // TODO 3 利用CDC 实时获取Mysql配置信息
        Properties prop = new Properties();
        prop.setProperty("scan.startup.mode", "initial");//默认
        DebeziumSourceFunction<JSONObject> mysqlSource = MySQLSource.<JSONObject>builder()
                .hostname("hadoop102")
                .port(3306)
                .username(GlobalConfig.MYSQL_USER)
                .password(GlobalConfig.MYSQL_PWD)
                .databaseList("flink_rt")
                .tableList("flink_rt.table_process")  //库名+表名
                .deserializer(new DeserializationData())
                .debeziumProperties(prop)
                .build();
        DataStreamSource<JSONObject> mysqlDS = env.addSource(mysqlSource).setParallelism(1);
//        mysqlDS.print("广播数据>>>>");

        // TODO 4 动态分流
        //配置流广播
        BroadcastStream<JSONObject> broadDS= mysqlDS.broadcast(broadState);
        SingleOutputStreamOperator<JSONObject> splitDS = etlDS.connect(broadDS)
                .process(new SpiltStream());

//        splitDS.print("kafka流");
//        splitDS.getSideOutput(hbaseTag).print("hbase流");

        // TODO 5 sink输出 事实表>>>DWD   维度表>>>HBase
        splitDS.addSink(KafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                JSONObject dataObj = element.getJSONObject("data");
                String sink_table = element.getString("sink_table");
                return new ProducerRecord<>(sink_table,dataObj.toJSONString().getBytes());
            }
        }));

        splitDS.getSideOutput(hbaseTag).addSink(new HbaseDimSink());
    }

    private static class DeserializationData implements DebeziumDeserializationSchema<JSONObject> {
        //自定义反序列化方式，模拟maxwell格式
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<JSONObject> out) throws Exception {
            //获取主题信息,包含着数据库和表名  mysql_binlog_source.gmall-flink-200821.z_user_info
            String topic = sourceRecord.topic();
            String[] arr = topic.split("\\.");
            String db = arr[1];
            String tableName = arr[2];

            //获取操作类型 READ DELETE UPDATE CREATE
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);

            //获取值信息并转换为Struct类型
            //import org.apache.kafka.connect.data.Struct;
            Struct value = (Struct) sourceRecord.value();

            //获取变化后的数据
            Struct after = value.getStruct("after");

            //创建JSON对象用于存储数据信息
            JSONObject data = new JSONObject();
            //修复DELETE时 after为null的bug
            if(after!=null){
                for (Field field : after.schema().fields()) {
                    Object o = after.get(field);
                    data.put(field.name(), o);
                }
            }else{
                //删除的场景 获取变化前的数据
                Struct before = value.getStruct("before");
                for (Field field : before.schema().fields()) {
                    Object o = before.get(field);
                    data.put(field.name(), o);
                }
            }

            //创建JSON对象用于封装最终返回值数据信息
            JSONObject result = new JSONObject();
            result.put("operation", operation.toString().toLowerCase());
            result.put("data", data);
            result.put("database", db);
            result.put("table", tableName);

            //发送数据至下游
            out.collect(result);
        }

        @Override
        public TypeInformation<JSONObject> getProducedType() {
            return Types.GENERIC(JSONObject.class);
        }
    }

    private static class SpiltStream extends BroadcastProcessFunction<JSONObject, JSONObject, JSONObject> {
        HashSet<String> exsistTable = new HashSet<>();
        private Connection conn;
        @Override
        public void open(Configuration parameters) throws Exception {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GlobalConfig.PHOENIX_SERVER);
        }

        @Override
        public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
            String table = value.getString("table");
            String type = value.getString("type");
            //如果是使用Maxwell的初始化功能，那么type类型为bootstrap-insert,我们这里也标记为insert
            if (type.equals("bootstrap-insert")) {
                type = "insert";
                value.put("type", type);
            }
            ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(broadState);
            TableProcess process = broadcastState.get(table + ":" + type);
            if(process==null){
//                System.out.println(table + ":" + type+"在配置表中不存在，无法分流");
            }else{
                filterColumn(value,process.getSinkColumns());
                String sinkType = process.getSinkType();
                value.put("sink_table",process.getSinkTable());
                if("kafka".equals(sinkType)){
                    out.collect(value);
                }else if("hbase".equals(sinkType)){
                    ctx.output(hbaseTag,value);
                }
            }

        }

        private void filterColumn(JSONObject obj, String columns) {
            JSONObject data = obj.getJSONObject("data");
            String[] column = columns.split(",");
            List<String> col = Arrays.asList(column);
            Set<Map.Entry<String, Object>> entries = data.entrySet();
            entries.removeIf(next -> !col.contains(next.getKey()));
        }

        @Override
        public void processBroadcastElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
            String operation = value.getString("operation");
            TableProcess tableProcess = value.getObject("data", TableProcess.class);
            BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(broadState);
            String type = tableProcess.getOperateType();
            String table = tableProcess.getSourceTable();
            //根据广播内容 更新状态
            if("delete".equals(operation)){
                broadcastState.put(table+":"+type,null);
            }else{
                broadcastState.put(table+":"+type,tableProcess);
                //判断表是否在phoenix存在，不存在则创建
                String sinkType = tableProcess.getSinkType();
                if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(type)) {
                    boolean notExist = exsistTable.add(table);
                    //如果在内存Set集合中不存在这个表，那么在Phoenix中创建这种表
                    if (notExist) {
                        //检查在Phonix中是否存在这种表
                        //有可能已经存在，只不过是应用缓存被清空，导致当前表没有缓存，这种情况是不需要创建表的
                        //在Phoenix中，表的确不存在，那么需要将表创建出来
                        //输出目的地表名或者主题名
                        String sinkTable = tableProcess.getSinkTable();
                        //输出字段
                        String sinkColumns = tableProcess.getSinkColumns();
                        //表的主键
                        String sinkPk = tableProcess.getSinkPk();
                        //建表扩展语句
                        String sinkExtend = tableProcess.getSinkExtend();
                        checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
                    }
                }
            }

        }

        public void checkTable(String tableName,String columns,String pk,String ext){
            if(pk == null || pk.length()< 1){
                pk = "id";
            }
            //如果在配置表中，没有配置建表扩展 需要给一个默认建表扩展的值
            if (ext == null) {
                ext = "";
            }
            //拼接建表语句
            StringBuilder createSql = new StringBuilder("create table if not exists " +
                    GlobalConfig.HBASE_SCHEMA + "." + tableName + "(");

            //对建表字段进行切分
            String[] fieldsArr = columns.split(",");
            for (int i = 0; i < fieldsArr.length; i++) {
                String field = fieldsArr[i];
                //判断当前字段是否为主键字段
                if (pk.equals(field)) {
                    createSql.append(field).append(" varchar primary key ");
                } else {
                    createSql.append("info.").append(field).append(" varchar ");
                }
                if (i < fieldsArr.length - 1) {
                    createSql.append(",");
                }
            }
            createSql.append(")");
            createSql.append(ext);

            System.out.println("创建Phoenix表的语句:" + createSql);

            //获取Phoenix连接
            PreparedStatement ps = null;
            try {
                ps = conn.prepareStatement(createSql.toString());
                ps.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        throw new RuntimeException("Phoenix建表失败");
                    }
                }
            }
        }

        @Override
        public void close() throws Exception {
            if(conn!=null){
                conn.close();
            }
        }
    }

    private static class HbaseDimSink extends RichSinkFunction<JSONObject> {
        private Connection conn = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GlobalConfig.PHOENIX_SERVER);
        }

        @Override
        public void invoke(JSONObject jsonObject, SinkFunction.Context context) throws Exception {
            String tableName = jsonObject.getString("sink_table");
            JSONObject dataJsonObj = jsonObject.getJSONObject("data");
            if (dataJsonObj != null && dataJsonObj.size() > 0) {
                String upsertSql = genUpsertSql(tableName.toUpperCase(), jsonObject.getJSONObject("data"));
                try {
//                    System.out.println(upsertSql);
                    PreparedStatement ps = conn.prepareStatement(upsertSql);
                    ps.executeUpdate();
                    //phoenix默认手动提交事务
                    conn.commit();
                    ps.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException("执行sql失败！");
                }
                if ("update".equals(jsonObject.getString("type"))) {
                    DimUtil.deleteCached(tableName, dataJsonObj.getString("id"));
                }
            }
        }

        public String genUpsertSql(String tableName, JSONObject jsonObject) {
            Set<String> fields = jsonObject.keySet();
            String upsertSql = "upsert into " + GlobalConfig.HBASE_SCHEMA + "." + tableName + "(" + StringUtils.join(fields, ",") + ")";
            String valuesSql = " values ('" + StringUtils.join(jsonObject.values(), "','") + "')";
            return upsertSql + valuesSql;
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
