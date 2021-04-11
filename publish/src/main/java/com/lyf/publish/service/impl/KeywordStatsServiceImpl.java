package com.lyf.publish.service.impl;

import com.lyf.publish.bean.KeywordStats;
import com.lyf.publish.mapper.KeywordStatsMapper;
import com.lyf.publish.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @ClassName KeywordStatsServiceImpl
 * @Author Kurisu
 * @Description
 * @Date 2021-3-9 21:17
 * @Version 1.0
 **/
@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {

    @Autowired
    KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return keywordStatsMapper.selectKeywordStats(date,limit);
    }
}