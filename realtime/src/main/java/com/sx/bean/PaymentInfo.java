package com.sx.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @ClassName PaymentInfo
 * @Author Kurisu
 * @Description
 * @Date 2021-3-23 08:17
 * @Version 1.0
 **/
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}