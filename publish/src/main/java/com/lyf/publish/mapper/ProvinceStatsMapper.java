package com.lyf.publish.mapper;
import com.lyf.publish.bean.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface ProvinceStatsMapper {
    //按地区查询交易额
    @Select("select province_name,sum(order_amount) order_amount " +
            "from province_stats where toYYYYMMDD(stt)=#{date} " +
            "group by province_id ,province_name  ")
    public List<ProvinceStats> selectProvinceStats(int date);

}