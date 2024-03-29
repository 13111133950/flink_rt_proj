package com.lyf.publish.mapper;

import com.lyf.publish.bean.ProductStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * @ClassName ProductStatsMapper
 * @Author Kurisu
 * @Description
 * @Date 2021-3-9 16:10
 * @Version 1.0
 **/

public interface ProductStatsMapper {
    //获取商品交易额
    //#{xxx}获取参数
    @Select("select sum(order_amount) order_amount  " +
            "from product_stats where toYYYYMMDD(stt)=#{date}")
    BigDecimal getGMV(int date);

    //统计某天不同SPU商品交易额排名
    @Select("select spu_id,spu_name,sum(order_amount) order_amount," +
            "sum(order_ct) order_ct from product_stats " +
            "where toYYYYMMDD(stt)=#{date} group by spu_id,spu_name " +
            "having order_amount>0 order by order_amount desc limit #{limit} ")
    List<ProductStats> getProductStatsGroupBySpu(@Param("date") int date, @Param("limit") int limit);

    //统计某天不同类别商品交易额排名
    @Select("select category3_id,category3_name,sum(order_amount) order_amount " +
            "from product_stats " +
            "where toYYYYMMDD(stt)=#{date} group by category3_id,category3_name " +
            "having order_amount>0  order by  order_amount desc limit #{limit}")
    List<ProductStats> getProductStatsGroupByCategory3(@Param("date")int date , @Param("limit") int limit);

    //统计某天不同品牌商品交易额排名
    @Select("select tm_id,tm_name,sum(order_amount) order_amount " +
            "from product_stats " +
            "where toYYYYMMDD(stt)=#{date} group by tm_id,tm_name " +
            "having order_amount>0  order by  order_amount  desc limit #{limit} ")
    List<ProductStats> getProductStatsByTrademark(@Param("date")int date,  @Param("limit") int limit);
}
