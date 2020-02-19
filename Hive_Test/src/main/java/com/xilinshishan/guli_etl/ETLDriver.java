package com.xilinshishan.guli_etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class ETLDriver implements Tool {
    private Configuration config;
    /**
     * 1.封装并运行Job
     */
    @Override
    public int run(String[] args) throws Exception {
        //1)创建job对象
        Job job = Job.getInstance(config);
        //2)关联Jar Mapper和Reducer
        job.setJarByClass(ETLDriver.class);
        job.setMapperClass(ETLMapper.class);
        //3)设置Mapper输出数据类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        //4)最终数据的输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //5)数据的输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //6)提交job
        boolean result = job.waitForCompletion(true);

        return result?0:1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.config = conf;
    }

    @Override
    public Configuration getConf() {
        return config;
    }

    public static void main(String[] args) {
        int run = 0;
        try {
            run = ToolRunner.run(new Configuration(), new ETLDriver(), args);
            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
