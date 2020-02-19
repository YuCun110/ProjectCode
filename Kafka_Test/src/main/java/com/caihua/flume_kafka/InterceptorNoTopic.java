package com.caihua.flume_kafka;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 2.在使用KafkaSink时没有指定Topic时，Flume拦截器添加头部信息"topic"
 */
public class InterceptorNoTopic implements Interceptor {
    //批次信息
    List<Event> list = new ArrayList<>();
    @Override
    public void initialize() {

    }
    //根据一条信息添加头部信息
    @Override
    public Event intercept(Event event) {
        //1.获取头部信息
        Map<String, String> headers = event.getHeaders();
        //2.数据
        String data = new String(event.getBody());
        //3.添加头信息
        if(data.contains("info")){
            headers.put("topic","info");
        }else{
            headers.put("topic","error");
        }
        return event;
    }
    //对一个批次的数据添加头部信息
    @Override
    public List<Event> intercept(List<Event> events) {
        //1.清空集合
        list.clear();
        //2.遍历
        for (Event event : events) {
            list.add(intercept(event));
        }
        return list;
    }

    @Override
    public void close() {

    }

    //静态内部类，传递类信息
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new InterceptorNoTopic();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
