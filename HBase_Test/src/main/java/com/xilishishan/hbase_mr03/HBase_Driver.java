package com.xilishishan.hbase_mr03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class HBase_Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //访问hdfs时，windows用户更改为Linux用户
        System.setProperty("HADOOP_USER_NAME","caihua");
        //1.创建Job对象
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf);
        //2.关联Jar
        job.setJarByClass(HBase_Driver.class);
        //3.设置Mapper
        //1)创建scan对象
        Scan scan = new Scan();
        //2)设置scan参数
        scan.setCaching(100);//设置一次读取的数量
        scan.setCacheBlocks(false);//设置是否开启缓存
        TableMapReduceUtil.initTableMapperJob("stu3",scan,HBase_Mapper.class, Text.class, NullWritable.class,job);
        //4.设置Reducer
        job.setReducerClass(HBase_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //5.设置数据输出数据路径
        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop102:9000/output_fruit"));
        //6.提交Job
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
