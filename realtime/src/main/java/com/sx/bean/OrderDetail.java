package com.sx.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @ClassName OrderDetail
 * @Author Kurisu
 * @Description
 * @Date 2021-3-22 21:07
 * @Version 1.0
 **/
@Data
public class OrderDetail {
    Long id;
    Long order_id ;
    Long sku_id;
    BigDecimal order_price ;
    Long sku_num ;
    String sku_name;
    String create_time;
    BigDecimal split_total_amount;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    Long create_ts;
}