package com.xilinshishan.flume_interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 过滤数据：如果时start类：是否符合
 */
public class ETL_Interceptor implements Interceptor {
    List<Event> list = new ArrayList<>();

    /**
     * 1.初始化
     */
    @Override
    public void initialize() {

    }
    //2.处理单个事件
    @Override
    public Event intercept(Event event) {
        //1)抓取数据内容
        String str = new String(event.getBody());
        //2)判断是那种日志
        boolean isFlag = true;
        if(str.contains("start")){
            //处理启动日志
            isFlag = ETLUtil.start(str);
        }else if(str.contains("event")){
            //处理事件日志
            isFlag = ETLUtil.event(str);
        }
        //3)输出
        return isFlag?event:null;
    }

    /**
     * 3.处理多个事件
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        //1)清空集合
        list.clear();
        //2)判断数据格式
        for (Event event : events) {
            if(intercept(event) != null){
                list.add(event);
            }
        }
        return list;
    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new ETL_Interceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
