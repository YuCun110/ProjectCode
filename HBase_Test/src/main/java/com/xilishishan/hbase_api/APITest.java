package com.xilishishan.hbase_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class APITest {
    private static Configuration config;//配置信息
    private static Connection connection;//连接
    private static Admin admin;//客户端
    //静态代码块
    static{
        try {
            //1)设置配置信息
            config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            //2)创建连接
            connection = ConnectionFactory.createConnection(config);
            //3)创建客户端
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //------------------------------------------------------DDL------------------------------------------------------
    /**
     * 1.创建命名空间：NameSpace
     */
    public static void createNameSpace(String nameSpace){
        //1)创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        //2)创建命名空间
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println("命名空间已经存在！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 2.检查表是否存在
     */
    public static boolean isTableExist(String tName) throws IOException {
        return  admin.tableExists(TableName.valueOf(tName));
    }
    /**
     * 3.创建表
     */
    public static void createTable(String tName,String... columnFamilys) throws IOException {
        //1)检查列族信息是否为空
        if(columnFamilys.length <= 0){
            System.out.println("列族信息为空！");
            return;
        }
        //2)检查表是否存在
        if(isTableExist(tName)){
            System.out.println(tName + "表已经存在！");
            return;
        }
        //3)创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tName));
        //4)添加列族信息
        for (String columnFamily : columnFamilys) {
            //a.创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
            //b.添加
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        //5)创建表
        admin.createTable(hTableDescriptor);//可以在此添加分区信息
    }

    /**
     * 4.删除表
     */
    public static void dropTable(String tName) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)下线表
        admin.disableTable(TableName.valueOf(tName));
        //3)删除表
        admin.deleteTable(TableName.valueOf(tName));
    }
    //------------------------------------------------------DML------------------------------------------------------

    /**
     * 5.添加/修改数据
     */
    public static void putData(String tName,String rowKey,String columnFamily,String columnQualifier,String value) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建Put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //4)添加put操作
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), Bytes.toBytes(value));
        //5)执行put操作
        table.put(put);
        //6)关闭资源
        table.close();
    }

    /**
     * 6.查询数据(get)
     */
    public static void getData(String tName,String rowKey,String columnFamily,String columnQualifier) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        //4)添加get操作
        get.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnQualifier));
        //4)执行get操作,接收返回结果
        Result result = table.get(get);
        //5)解析数据,并打印输出
        for (Cell cell : result.rawCells()) {
            System.out.println("RowKey: " + Bytes.toString(CellUtil.cloneRow(cell))
                        + "; ColumnFamily: " + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "; ColumnQualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "; Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        //6)关闭连接
        table.close();
    }

    /**
     * 7.查询数据(scan)
     */
    public static void scanData(String tName,String columnFamily,String columnQualifier) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建scan对象
        Scan scan = new Scan();
        //4)添加scan操作
        scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier));
        //5)执行操作
        ResultScanner results = table.getScanner(scan);
        //6)解析数据，打印输出
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RowKey: " + Bytes.toString(CellUtil.cloneRow(cell))
                        + "; ColumnFamily: " + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "; ColumnQualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "; Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        //7)关闭连接
        table.close();
    }
    /**
     * 8.查询数据(scan):过滤操作
     */
    public static void scanDataWithFilter(String tName,String columnFamily,String columnQualifier,String rowKey,String value) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建scan对象
        Scan scan = new Scan();
        //4)创建过滤器对象
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);//and

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new SubstringComparator(rowKey));
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(value));

        filterList.addFilter(rowFilter);
        filterList.addFilter(singleColumnValueFilter);
        //5)添加过滤操作
        scan.setFilter(filterList);
        //6)执行过滤
        ResultScanner results = table.getScanner(scan);
        //7)解析数据，打印输出
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RowKey: " + Bytes.toString(CellUtil.cloneRow(cell))
                        + "; ColumnFamily: " + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "; ColumnQualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "; Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        //8)关闭连接
        table.close();
    }

    /**
     * 9.删除数据
     */
    public static void deleteData(String tName,String rowKey,String columnFamily,String columnQualifier) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //4)添加删除操作
        delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(columnQualifier));
        //delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnQualifier));
        //5)执行删除
        table.delete(delete);
        //6)关闭连接
        table.close();
    }
    //关闭操作
    public static void close() throws IOException {
        if(admin != null){
            admin.close();
        }
        if(connection != null){
            connection.close();
        }
    }

    public static void main(String[] args) throws IOException {
        //1.检查表是否存在
        //System.out.println(isTableExist("stu2"));
    }
}
