package com.lyf.publish.service;

import com.lyf.publish.bean.VisitorStats;

import java.util.List;

/**
 * @ClassName VisitorStatsService
 * @Author Kurisu
 * @Description
 * @Date 2021-3-9 21:13
 * @Version 1.0
 **/
public interface VisitorStatsService {
    public List<VisitorStats> getVisitorStatsByNewFlag(int date);

    public List<VisitorStats> getVisitorStatsByHour(int date);

    public Long getPv(int date);

    public Long getUv(int date);
}