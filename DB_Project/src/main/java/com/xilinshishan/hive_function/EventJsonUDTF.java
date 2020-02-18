package com.xilinshishan.hive_function;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class EventJsonUDTF extends GenericUDTF {
    /**
     * 1.初始化：指定输出的参数的名称和参数类型
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //1)指定输出数据的列名
        ArrayList<String> fieldNames = new ArrayList<>();
        fieldNames.add("event_name");
        fieldNames.add("event_json");
        //2)输出数据类型的校验
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    ////输入1条记录，输出若干条结果
    @Override
    public void process(Object[] args) throws HiveException {
        //1)获取传入的值
        String input = args[0].toString();
        //2)判断数据的格式：是否为空
        if(StringUtils.isBlank(input)){
            return;
        }else{
            try {
                //3)将数据中的多个事件封装为Json数组
                JSONArray jsonArray = new JSONArray(input);
                //4)循环遍历事件
                for (int i = 0;i<jsonArray.length();i++){
                    //a.定义返回结果的容器
                    String[] result = new String[2];
                    //b.取出每一个事件
                    result[1] = jsonArray.getString(i);
                    //c.明确事件的类型
                    result[0] = jsonArray.getJSONObject(i).getString("en");
                    //5)返回结果
                    forward(result);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }
    //当没有记录处理的时候该方法会被调用，用来清理代码或者产生额外的输出
    @Override
    public void close() throws HiveException {

    }
}
