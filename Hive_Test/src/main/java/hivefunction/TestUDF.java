package hivefunction;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class TestUDF extends UDF {
    public String evaluate(String str){
        if(str.equals("a")){
            return "_";
        }
        return null;
    }
}
