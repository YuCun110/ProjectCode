package com.xilishishan.hbase_api;

import com.google.inject.internal.cglib.core.$ClassNameReader;
import com.google.inject.internal.cglib.proxy.$CallbackFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * DML：表中数据的增、删、改、查（put,delete,get,scan）
 */
public class HBase_DML {
    private static Configuration config;
    private static Connection connection;
    private static Admin admin;
    //静态代码块
    static{
        //1)设置配置信息
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        try {
            //2)创建连接
            connection = ConnectionFactory.createConnection(config);
            //3)创建客户端
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 1.检查表是否存在
     */
    public static boolean isTableExist(String tName) throws IOException {
        return admin.tableExists(TableName.valueOf(tName));
    }

    /**
     * 2.新增数据
     */
    //不指定时间戳
    public static void putData(String tName,String rowKey,String cf,String cn,String value) throws IOException {
        //1)判断表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建Put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //4)进行put操作
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        //5)执行put
        table.put(put);
        //6)关闭连接
        table.close();
    }
    //指定时间戳
    public static void putData(String tName,String rowKey,String cf,String cn,String value,Long timestamp) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建Put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //4)添加Put操作
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),timestamp,Bytes.toBytes(value));
        //5)执行添加操作
        table.put(put);
        //6)关闭连接
        table.close();
    }
    /**
     * 3.查询数据(get方式)
     */
    public static void getData(String tName,String rowKey,String cf,String cn) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        //4)进行get操作
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        //5)执行get操作，并返回结果
        Result result = table.get(get);
        //6)对结果进行解析，并输出
        for (Cell cell : result.rawCells()) {
            System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell))
                        + "ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        //7)关闭资源
        table.close();
    }
    public static void getData(String tName,String rowKey,String cf) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        //4)进行get操作
        get.addFamily(Bytes.toBytes(cf));
        //5)执行get操作
        Result result = table.get(get);
        //6)对结果进行解析，并输出
        for (Cell cell : result.rawCells()) {
            System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell))
                    + "; ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                    + "; Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + "; Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        //7)关闭资源
        table.close();
    }
    public static void getData(String tName,String rowKey) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        //4)执行查询操作,接收返回值
        Result result = table.get(get);
        //5)解析结果，遍历输出
        for (Cell cell : result.rawCells()) {
            System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell))
                        + "; ColunmFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "; Qualifiter:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "; Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        //6)关闭连接
        table.close();
    }
    /**
     * 4.查询数据(scan方式)
     */
    //指定范围查询
    public static void scanData(String tName,String start,String stop) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建scan对象
        Scan scan = new Scan(Bytes.toBytes(start),Bytes.toBytes(stop));
        //4)扫描数据
        ResultScanner scanner = table.getScanner(scan);
        //5)解析数据，并输出
        for (Result results : scanner) {
            for (Cell cell : results.rawCells()) {
                System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell))
                            + "; ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                            + "; Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                            + "; Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        //6)关闭连接
        table.close();
    }
    //带有过滤器的查询
    public static void scanDataWithFilter(String tName,String cf,String cn,String value) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建scan对象
        Scan scan = new Scan();
        //4)创建过滤器
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);//过滤器集合(or的关系)

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new SubstringComparator("11"));//行键过滤
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(cn), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));//值过滤

        singleColumnValueFilter.setFilterIfMissing(true);//是否跳过不存在的列

        //5)添加过滤到过滤器集合
        filterList.addFilter(rowFilter);
        filterList.addFilter(singleColumnValueFilter);
        //6)添加过滤操作
        scan.setFilter(filterList);
        //7)过滤数据
        ResultScanner scanner = table.getScanner(scan);
        //8)解析数据，输出
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell))
                        + "; ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "; Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "; Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        //9)关闭连接
        table.close();
    }

    /**
     * 3.删除数据
     */
    public static void deleteData(String tName,String rowKey,String cf,String cn) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //4)添加删除操作
        delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));//删除当前时间符合条件的所有版本的数据
        //delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));//删除最近一次编辑符合条件的一条数据
        //5)执行删除
        table.delete(delete);
        //6)关闭连接
        table.close();
    }
    //指定时间戳
    public static void deleteData(String tName,String rowKey,String cf,String cn,Long timestamp) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //4)添加删除操作
        delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn),timestamp);//删除指定时间戳的所有版本的数据
        //delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),timestamp);//删除指定时间戳的一条数据
        //5)执行删除
        table.delete(delete);
        //6)关闭连接
        table.close();
    }
    public static void main(String[] args) throws IOException {
        //检查表是否存在
        //System.out.println(isTableExist("stu1"));

        //新增数据
        //putData("stu1","1004","info","name","wangshi",1576057228099L);

        //查询数据
        //getData("stu1","1003");
        //scanDataWithFilter("stu1","info","name","san");

        //删除数据
        deleteData("stu1","1001","info","name");
    }
}
