package com.lyf.publish.service;

import com.lyf.publish.bean.ProvinceStats;

import java.util.List;

/**
 * @ClassName ProvinceStatsService
 * @Author Kurisu
 * @Description
 * @Date 2021-3-9 21:09
 * @Version 1.0
 **/
public interface ProvinceStatsService {
    public List<ProvinceStats> getProvinceStats(int date);
}