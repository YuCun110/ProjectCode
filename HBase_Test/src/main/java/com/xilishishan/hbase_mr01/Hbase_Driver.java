package com.xilishishan.hbase_mr01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class Hbase_Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建Job对象
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf);
        //2.关联Jar包
        job.setJarByClass(Hbase_Driver.class);

        //3.设置Mapper
        //1)创建scan对象
        Scan scan = new Scan();
        scan.setCaching(500);//设置一次读取的数量
        scan.setCacheBlocks(false);//设置是否开启缓存
        //2)设置Mapper
        TableMapReduceUtil.initTableMapperJob("stu3",scan,HBase_Mapper.class, ImmutableBytesWritable.class,Put.class,job);

        //4.设置Reducer
        TableMapReduceUtil.initTableReducerJob("temp",HBase_Reducer.class,job);
        //5.提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);
    }
}
