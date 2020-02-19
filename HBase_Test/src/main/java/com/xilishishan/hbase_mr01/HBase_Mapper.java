package com.xilishishan.hbase_mr01;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 目标：将stu3表中的一部分数据，通过MR迁入到temp表中
 *  1.继承TableMapper：底层为Mapper<ImmutableBytesWritable, Result, KEYOUT, VALUEOUT>
 *      ImmutableBytesWritable:rowKey
 *      Result:一行数据
 */
public class HBase_Mapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //1.创建Put对象
        Put put = new Put(key.get());
        //2.添加数据
        for (Cell cell : value.rawCells()) {
            //根据列名筛选数据
            if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                put.add(cell);
            }
        }
//        for (Cell cell : value.rawCells()) {
//            if("name".equals(CellUtil.cloneQualifier(cell))){
//
//            }
//        }
        //3.输出
        context.write(key,put);
    }
}
