package hivefunction;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class TestUDTF extends GenericUDTF {
    private List<String> out = new ArrayList<>();
    /**
     * 1.初始化：
     *  输出数据的列名和类型；
     * @return
     */
    @Override
    public StandardStructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //1.输出数据的列名
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("col_name");
        //2.输出数据的类型校验
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        //3.输出
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    /**
     * 2.处理数据的方法：
     *  每条数据调用一次;
     */
    @Override
    public void process(Object[] args) throws HiveException {
        //1.取出传入的参数
        String line = args[0].toString();
        String splitKey = args[1].toString();
        //2.按照规则切割数据
        String[] splits = line.split(splitKey);
        //3.遍历输出
        for (String split : splits) {
            //写出操作（每次输出暂时写入收集器：缓冲区）
            out.clear();
            out.add(split);
            forward(out);
        }
    }
    @Override
    public void close() throws HiveException {

    }
}
