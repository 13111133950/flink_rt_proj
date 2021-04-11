package com.sx.bean;

import lombok.Data;

/**
 * @ClassName TableProcess
 * @Author Kurisu
 * @Description
 * @Date 2021-3-4 15:29
 * @Version 1.0
 **/
@Data
public class TableProcess {
    //动态分流Sink常量
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";
    //来源表
    String sourceTable;
    //操作类型 insert,update,delete
    String operateType;
    //输出类型 hbase kafka
    String sinkType;
    //输出表(主题)
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableProcess that = (TableProcess) o;

        if (sourceTable != null ? !sourceTable.equals(that.sourceTable) : that.sourceTable != null) return false;
        if (operateType != null ? !operateType.equals(that.operateType) : that.operateType != null) return false;
        if (sinkType != null ? !sinkType.equals(that.sinkType) : that.sinkType != null) return false;
        if (sinkTable != null ? !sinkTable.equals(that.sinkTable) : that.sinkTable != null) return false;
        if (sinkColumns != null ? !sinkColumns.equals(that.sinkColumns) : that.sinkColumns != null) return false;
        if (sinkPk != null ? !sinkPk.equals(that.sinkPk) : that.sinkPk != null) return false;
        return sinkExtend != null ? sinkExtend.equals(that.sinkExtend) : that.sinkExtend == null;
    }

    @Override
    public int hashCode() {
        int result = sourceTable != null ? sourceTable.hashCode() : 0;
        result = 31 * result + (operateType != null ? operateType.hashCode() : 0);
        result = 31 * result + (sinkType != null ? sinkType.hashCode() : 0);
        result = 31 * result + (sinkTable != null ? sinkTable.hashCode() : 0);
        result = 31 * result + (sinkColumns != null ? sinkColumns.hashCode() : 0);
        result = 31 * result + (sinkPk != null ? sinkPk.hashCode() : 0);
        result = 31 * result + (sinkExtend != null ? sinkExtend.hashCode() : 0);
        return result;
    }
}
