package com.sx.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @ClassName MyDateUtil
 * @Author Kurisu
 * @Description
 * @Date 2021-3-23 08:50
 * @Version 1.0
 **/
public class MyDateUtil {
    private final static DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static Long getTs(String dateStr){
        LocalDateTime localDateTime = LocalDateTime.parse(dateStr, df);
        long ts = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return ts;
    }
}
