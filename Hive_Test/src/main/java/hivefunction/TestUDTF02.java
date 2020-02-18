package hivefunction;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class TestUDTF02 extends GenericUDTF {
    List<String> out = new ArrayList<>();
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //1.输出数据列名
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("key");
        fieldNames.add("value");
        //2.输出数据类型校验
        List<ObjectInspector> fieldIOs = new ArrayList<>();
        fieldIOs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldIOs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        //3.输出
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldIOs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        //1.获取数据
        String line = args[0].toString();

        //2.拆分数据
        String[] split = line.split(",");
        for (String s : split) {
            out.clear();
            String[] kv = s.split(":");
            out.add(kv[0]);
            out.add(kv[1]);
            forward(out);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
