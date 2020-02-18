package com.xilinshishan.hive_function;

import org.apache.avro.data.Json;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class BaseFieldUDF extends UDF {
    public String evaluate(String line,String key) {
        //1.拆分时间戳与JSON字符串
        String[] splits = line.split("\\|");
        //2.判断key是否为服务器时间
        if("st".equals(key)){
            return splits[0];
        }
        try {
            //3.创建JSON对象，封装Json字符串
            JSONObject json = new JSONObject(splits[1]);
            //4.判断key是否为特定的事件
            if("et".equals(key)){
                if(json.has("et")){
                    return json.getString("et");
                }else{
                    return "";
                }
            }else{
                //5.最后的选项：从公共字段取数据
                JSONObject cm = json.getJSONObject("cm");
                return cm.getString(key);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return "";
    }
}
