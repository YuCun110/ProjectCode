package com.caihua.producer.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 在该案例中，我们以端口数据模拟日志，以数字（单个）和字母（单个）模拟不同类型的日志，我们需要自定义interceptor区分数字和字母，
 * 将其分别发往不同的分析系统（Channel）。
 */
public class CustomInterceptor implements Interceptor {
    private List<Event> list = new ArrayList<>();//全局的一个批次事件（使用前清空）

    @Override
    public void initialize() {

    }

    /**
     * 2.处理一个事件
     */
    @Override
    public Event intercept(Event event) {
        //1）获取头信息，和数据信息
        Map<String, String> header = event.getHeaders();
        String body = new String(event.getBody());

        //2）根据数据类型向头部信息中添加不同信息
        if(body.contains("info")){
            header.put("type","info");
        }else{
            header.put("type","error");
        }
        //3)返回事件
        return event;
    }

    /**
     * 3.处理多个事件（一个批次）
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        //1）清空集合
        list.clear();
        //2）遍历evens添加头部信息
        for (Event event : events) {
            list.add(intercept(event));
        }
        return list;
    }

    /**
     * 4.关闭资源
     */
    @Override
    public void close() {

    }

    /**
     * 5.静态内部类：帮助创建对象
     */
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new CustomInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
