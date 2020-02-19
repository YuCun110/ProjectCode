package com.xilishishan.hbase_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * DDL：表的增、删
 */
public class HBase_DDL {
    private static Configuration config;
    private static Connection connection;
    private static Admin admin;
    //静态代码块
    static{
        //1)创建配置文件信息
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        try {
            //2)创建连接
            connection = ConnectionFactory.createConnection(config);
            //3)创建客户端对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //1.判断表是否存在
    public static boolean isTableExist(String tName) throws IOException {
        return admin.tableExists(TableName.valueOf(tName));
    }

    //2.创建命名空间
    public static void createNameStamp(String nameSpace){
        //1)创建命名空间描述器
        NamespaceDescriptor build = NamespaceDescriptor.create(nameSpace).build();
        //2)创建命名空间
        try {
            admin.createNamespace(build);
        } catch (NamespaceExistException e) {
            System.out.println("命名空间已经存在！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //3.创建表
    public static void createTable(String tName,String... cfs) throws IOException {
        //1)检查列族是否为空
        if(cfs == null){
            System.out.println("列族为空！");
            return;
        }
        //2)检查表是否存在
        if(isTableExist(tName)){
            System.out.println(tName + "表为空！");
            return;
        }
        //3)创建表的描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tName));
        //4)添加列族信息
        for (String cf : cfs) {
            //a.创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            //b.添加列族信息
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        //添加协处理器
        hTableDescriptor.addCoprocessor("com.xilishishan.hbase_phoenix.PhoenixCoprocessor");

        //5)创建表
        admin.createTable(hTableDescriptor);
    }

    /**
     * 4.删除表
     */
    public static void dropTable(String tName) throws IOException {
        //1)检查表是否存在
        if(isTableExist(tName)){
            System.out.println("表不存在！");
            return;
        }
        //2)创建表对象
        TableName tableName = TableName.valueOf(tName);
        //3)下线表
        admin.disableTable(tableName);
        //4)删除表
        admin.deleteTable(tableName);
    }

    /**
     * 5.关闭资源
     */
    public static void close() throws IOException {
        if(admin != null){
            admin.close();
        }
        if(connection != null){
            connection.close();
        }
    }

    public static void main(String[] args) throws IOException {
        //创建表
        createTable("stu","info");
    }
}
