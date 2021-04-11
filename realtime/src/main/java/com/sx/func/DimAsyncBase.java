package com.sx.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @ClassName DimAsyncBase
 * @Author Kurisu
 * @Description
 * @Date 2021-3-22 22:39
 * @Version 1.0
 **/
public interface DimAsyncBase<T> {
    String getKey(T input);
    void join(T input,JSONObject obj);
}
