package com.xilishishan.hbase_mr03;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 将HBase表中的数据导入hdfs文件中
 */
public class HBase_Mapper extends TableMapper<Text, NullWritable> {
    private Text k = new Text();
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //1.定义一行数据存储的容器
        StringBuffer line = new StringBuffer();
        //2.解析行数据信息
        boolean isFlag = true;
        for (Cell cell : value.rawCells()) {
            if(isFlag){
                line.append(Bytes.toString(CellUtil.cloneRow(cell)));
                isFlag = false;
            }
            //line.append(Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t");
            line.append("\t" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        //3.输出
        k.set(line.toString());
        context.write(k,NullWritable.get());
    }
}
