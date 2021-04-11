package com.lyf.publish.service;


import com.lyf.publish.bean.ProductStats;

import java.math.BigDecimal;
import java.util.List;

/**
 * @ClassName ProductStatsService
 * @Author Kurisu
 * @Description
 * @Date 2021-3-9 16:10
 * @Version 1.0
 **/
public interface ProductStatsService {
    //获取当天交易额
    BigDecimal getGMV(int date);

    //统计某天不同SPU商品交易额排名
    public List<ProductStats> getProductStatsGroupBySpu(int date, int limit);

    //统计某天不同类别商品交易额排名
    public List<ProductStats> getProductStatsGroupByCategory3(int date,int limit);

    //统计某天不同品牌商品交易额排名
    public List<ProductStats> getProductStatsByTrademark(int date,int limit);
}
