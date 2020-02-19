package com.xilinshishan.flume_interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class Type_Interceptor implements Interceptor {
    List<Event> list = new ArrayList<>();
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //1)获取数据
        String str = new String(event.getBody());
        Map<String, String> headers = event.getHeaders();
        //2)判断数据的类型
        if(str.contains("start")){
            headers.put("topic","topic_start");
        }else{
            headers.put("topic","topic_event");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        //1)清空集合
        list.clear();
        //2)遍历，添加头信息
        for (Event event : events) {
            list.add(intercept(event));
        }
        //3)输出
        return list;
    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new Type_Interceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
