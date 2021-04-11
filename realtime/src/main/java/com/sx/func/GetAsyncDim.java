package com.sx.func;

import com.alibaba.fastjson.JSONObject;
import com.sx.util.DimUtil;
import com.sx.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @ClassName GetAsyncDim
 * @Author Kurisu
 * @Description
 * @Date 2021-3-22 22:13
 * @Version 1.0
 **/
public abstract class GetAsyncDim<T> extends RichAsyncFunction<T,T> implements DimAsyncBase<T>{
    private ExecutorService executorService = null;
    private String tableName;

    public GetAsyncDim(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = ThreadPoolUtil.getInstance();
    }
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                String key = getKey(input);
                JSONObject dimInfoObj = DimUtil.getDimInfo(tableName, key);
                join(input,dimInfoObj);
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("异步查询超时");
    }
}
