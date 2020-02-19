package com.xilinshishan.flume_interceptor;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 清洗数据
 */
public class ETLUtil {
    //清洗启动日志
    public static boolean start(String str) {
        return str.startsWith("{") && str.endsWith("}");
    }
    //清洗事件日志
    public static boolean event(String str){
        //1.对数据进行切分(转移)
        String[] split = str.split("\\|");
        //2.判断开头的时间戳格式是否正确
        if(split[0].length() != 13){
            return false;
        }
        //3.判断是否包含{}(不包含首尾空格)
        return start(split[1].trim());
    }
}
