package com.xilishishan.hbase_phoenix;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 编写协处理器，实现在往A表插入数据的同时让HBase自身向B表中插入一条数据。
 */
public class PhoenixCoprocessor extends BaseRegionObserver {
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //创建连接
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        //创建表对象
        Table table = connection.getTable(TableName.valueOf("index"));
        //Table table = connection.getTable(TableName.valueOf("index"));//本地模式：注意死循环（在put中添加标记位）
        //插入数据

        table.put(put);
        //关闭资源
        table.close();
        connection.close();
    }
}
