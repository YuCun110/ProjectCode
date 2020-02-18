package com.xilishishan.hbase_mr01;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 继承TableReducer，其底层为:Reducer<KEYIN, VALUEIN, KEYOUT, Mutation>
 *     Mutation：TableOutputFormat(为数据最终的输出的类型)；
 *     其子类为：Append,Delete,Increment,Put
 */
public class HBase_Reducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for (Put put : values) {
            context.write(NullWritable.get(),put);
        }
    }
}
