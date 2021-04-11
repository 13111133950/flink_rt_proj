package com.lyf.publish.service.impl;

import com.lyf.publish.bean.ProvinceStats;
import com.lyf.publish.mapper.ProvinceStatsMapper;
import com.lyf.publish.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @ClassName ProvinceStatsServiceImpl
 * @Author Kurisu
 * @Description
 * @Date 2021-3-9 21:09
 * @Version 1.0
 **/
@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {
    @Autowired
    ProvinceStatsMapper provinceStatsMapper;

    @Override
    public List<ProvinceStats> getProvinceStats(int date) {
        return provinceStatsMapper.selectProvinceStats(date);
    }
}
