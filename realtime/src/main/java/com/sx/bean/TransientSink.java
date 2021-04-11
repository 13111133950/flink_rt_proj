package com.sx.bean;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
/**
 * @ClassName TransientSink
 * @Author Kurisu
 * @Description
 * @Date 2021-3-24 08:22
 * @Version 1.0
 **/
@Target(FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}
