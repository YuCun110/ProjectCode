package com.xilishishan.hbase_mr02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class HBase_Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建Job对象
        Configuration config = HBaseConfiguration.create();
        //config.set("ColumnFamily",args[0]);//可以输入参数
        Job job = Job.getInstance(config);

        //2.关联Jar
        job.setJarByClass(HBase_Driver.class);
        //3.设置Mapper
        job.setMapperClass(HBase_Mapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        //4.设置Reducer
        TableMapReduceUtil.initTableReducerJob("stu3",HBase_Reducer.class,job);
        //5.设置输入数据路径
        FileInputFormat.setInputPaths(job,new Path("hdfs://hadoop102:9000/input_fruit/student.tsv"));
        //6.提交Job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
