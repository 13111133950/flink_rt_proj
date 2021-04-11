package com.lyf.publish.mapper;

import com.lyf.publish.bean.KeywordStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @ClassName KeywordStatsMapper
 * @Author Kurisu
 * @Description
 * @Date 2021-3-9 21:15
 * @Version 1.0
 **/
public interface KeywordStatsMapper {

    @Select("select keyword," +
            "sum(keyword_stats.ct * " +
            "multiIf(source='SEARCH',10,source='ORDER',3,source='CART',2,source='CLICK',1,0)) ct" +
            " from keyword_stats where toYYYYMMDD(stt)=#{date} group by keyword " +
            "order by sum(keyword_stats.ct) desc limit #{limit} ")
    public List<KeywordStats> selectKeywordStats(@Param("date") int date, @Param("limit") int limit);
}
