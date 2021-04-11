package com.lyf.publish.service;

import com.lyf.publish.bean.KeywordStats;

import java.util.List;

/**
 * @ClassName KeywordStatsService
 * @Author Kurisu
 * @Description
 * @Date 2021-3-9 21:16
 * @Version 1.0
 **/
public interface KeywordStatsService {
    public List<KeywordStats> getKeywordStats(int date, int limit);
}