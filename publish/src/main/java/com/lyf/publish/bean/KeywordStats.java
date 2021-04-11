package com.lyf.publish.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName KeywordStats
 * @Author Kurisu
 * @Description
 * @Date 2021-3-9 21:15
 * @Version 1.0
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {
    private String stt;
    private String edt;
    private String keyword;
    private Long ct;
    private String ts;
}
