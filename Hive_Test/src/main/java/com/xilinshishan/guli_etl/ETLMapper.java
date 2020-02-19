package com.xilinshishan.guli_etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class ETLMapper extends Mapper<LongWritable, Text, NullWritable,Text> {
    private Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取行数据
        String line = value.toString();
        //2.清洗数据
        String newStr = ETLUtil.cleanStr(line);
        //3.删除长度小于9的数据
        if(newStr == null){
            return;
        }
        //4.输出
        v.set(newStr);
        context.write(NullWritable.get(),v);
    }
}
