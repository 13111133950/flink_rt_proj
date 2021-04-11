package com.sx.func;

import com.sx.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @ClassName KeywordUDTF
 * @Author Kurisu
 * @Description
 * @Date 2021-3-9 13:46
 * @Version 1.0
 **/
@FunctionHint(output = @DataTypeHint("ROW<s STRING>"))
public class KeywordUDTF  extends TableFunction<Row> {
    public void eval(String value) {
        List<String> keywordList = KeywordUtil.analyze(value);
        for (String keyword : keywordList) {
            Row row = new Row(1);
            row.setField(0,keyword);
            collect(row);
        }
    }
}