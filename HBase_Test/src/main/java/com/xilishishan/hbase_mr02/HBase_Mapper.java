package com.xilishishan.hbase_mr02;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 目标：实现将HDFS中的数据写入到Hbase表中。
 */
public class HBase_Mapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    private String columnFamily;
    //接收传递的参数（列信息）
//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        Configuration conf = context.getConfiguration();
//        //接收列族信息
//        //columnFamily = conf.get("ColumnFamily");
//    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.拆分行数据
        String[] splits = value.toString().split("\t");
        //2.封装RowKey
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes(splits[0]));
        //3.封装Put对象
        Put put = new Put(Bytes.toBytes(splits[0]));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(splits[1]));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(splits[2]));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(splits[3]));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("chinese"),Bytes.toBytes(splits[4]));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("math"),Bytes.toBytes(splits[5]));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("english"),Bytes.toBytes(splits[6]));
        //4.输出
        context.write(immutableBytesWritable,put);
    }
}
