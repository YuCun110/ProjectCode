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
public class HBaseAPI {
    private static Configuration config;//创建配置信息
    private static Connection connection;
    private static Admin admin; //创建客户端对象
    /**
     * 1.静态代码块
     */
    static{
        //1).获取Configuration对象
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");

        try {
            //2).创建连接
            connection = ConnectionFactory.createConnection(config);
            //3).对表结构的操作DDL：getAdmin
            admin = connection.getAdmin();
            //4).对表数据的操作DML：getTable
            //connection.getTable();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * ------------------DDL------------------
     * 2.创建命名空间
     *  a.创建命名空间描述器：NamespaceDescriptor
     *  b.创建命名空间：createNamespace
     */
    public static void createNameSpace(String nameSpace){
        //a.创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        //b.创建命名空间
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            //不存在打印异常
            System.out.println(nameSpace + "命名空间已经存在！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 3.判断表是否存在
     */
    public static boolean isTableExist(String tName) throws IOException {
         return admin.tableExists(TableName.valueOf(tName));
    }

    /**
     * 4.创建表
     *  a.创建表信息描述器
     *  b.创建列族信息描述器
     *  c.在表描述器中添加列族信息
     *  d.根据表描述器创建表
     */
    public static void createTable(String tName,String... columnFimallys) throws IOException {
        //1)判断列族信息是否为空
        if(columnFimallys.length<=0){
            System.out.println("请正确添加列族信息！");
            return;
        }
        //2)判断表是否存在
        if(isTableExist(tName)){
            System.out.println(tName + "表已经存在！");
            return;
        }
        //3)创建表
        //a.创建表描述器
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tName));
        //b.循环添加列族信息
        for (String columnFimally : columnFimallys) {
            //创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFimally);
            //添加列族信息
            tableDescriptor.addFamily(hColumnDescriptor);
        }
        //c.创建表
        admin.createTable(tableDescriptor);
        //进行分区
//        byte[][] splits = new byte[5][];
//        splits[0] = Bytes.toBytes("aa");
//        splits[1] = Bytes.toBytes("bb");
//        ···
        //admin.createTable(tableDescriptor,Bytes.toBytes(0000),Bytes.toBytes(9999),10);
    }

    /**
     * 5.删除表
     *  a.下线表
     *  b.删除操作
     */
    public static void dropTable(String tName) throws IOException {
        //1)判断表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)删除表
        TableName tableName = TableName.valueOf(tName);
        //a.使表下线
        admin.disableTable(tableName);
        //b.执行删除操作
        admin.deleteTable(tableName);
    }

    /**
     * ------------------DML------------------
     * 6.新增/修改数据
     *  a.创建表对象Table
     *  b.创建Put操作对象
     *  c.添加Put操作的数据
     *  d.执行Put操作
     *  e.关闭连接
     */
    //不指定时间戳版本
    public static void updateData(String tName,String rowKey,String cf,String cl,String value) throws IOException {
        //1)判断表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建Put操作的对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //4)添加Put操作的数据（不指定时间版本）
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cl),Bytes.toBytes(value));
        //5)执行Put操作
        table.put(put);
        //6)关闭连接
        table.close();
    }
    //不指定时间戳版本
    public static void updateData(String tName,String rowKey,String cf,String cl,String value,Long timeStamp) throws IOException {
        //1)判断表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建Put操作的对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //4)添加Put操作的数据（指定时间版本）
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cl),timeStamp,Bytes.toBytes(value));
        //5)执行Put操作
        table.put(put);
        //6)关闭连接
        table.close();
    }

    /**
     * 7.查询数据（get方式）
     *  a.创建表对象Table
     *  b.创建Get对象
     *  c.添加get的详细信息
     *  d.执行查询，并接收返回结果
     *  e.解析结果，并打印输出
     *  f.关闭连接
     */
    public static void getData(String tName,String rowKey,String cf,String cl) throws IOException {
        //1)判断表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        //4)添加查询操作操作
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cl));
        //指定获取数据的最多的版本
        get.setMaxVersions(2);
        //5)执行查询操作
        Result result = table.get(get);
        //6)解析查询结果遍历打印
        for (Cell cell : result.rawCells()) {
            System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell))
                            + "; ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                            + "; Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                            + "; Value:" + Bytes.toString(CellUtil.cloneValue(cell))) ;

        }
        //7)关闭连接
        table.close();
    }
    public static void getData(String tName,String rowKey,String cf) throws IOException {
        //1)判断表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        //4)添加查询操作操作
        get.addFamily(Bytes.toBytes(cf));
        //指定获取数据的最多的版本
        get.setMaxVersions(2);
        //5)执行查询操作
        Result result = table.get(get);
        //6)解析查询结果遍历打印
        for (Cell cell : result.rawCells()) {
            System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell))
                    + "; ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                    + "; Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + "; Value:" + Bytes.toString(CellUtil.cloneValue(cell))) ;

        }
        //7)关闭连接
        table.close();
    }
    public static void getData(String tName,String rowKey) throws IOException {
        //1)判断表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        //4)添加查询操作操作
        //指定获取数据的最多的版本
        get.setMaxVersions(2);
        //5)执行查询操作
        Result result = table.get(get);
        //6)解析查询结果遍历打印
        for (Cell cell : result.rawCells()) {
            System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell))
                    + "; ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                    + "; Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + "; Value:" + Bytes.toString(CellUtil.cloneValue(cell))) ;

        }
        //7)关闭连接
        table.close();
    }

    /**
     * 8.查询数据（scan方式）
     */
    public static void scanData(String tName,String start,String stop) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)获取Scan对象
        Scan scan = new Scan(Bytes.toBytes(start),Bytes.toBytes(stop));
        //4)扫描数据
        ResultScanner results = table.getScanner(scan);
        //5)解析数据，控制台打印输出
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell))
                            + "ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                            + "Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                            + "Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        //6)关闭连接
        table.close();
    }
    public static void scanData(String tName) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)获取Scan对象
        Scan scan = new Scan();
        //4)扫描数据
        ResultScanner results = table.getScanner(scan);
        //5)解析数据，控制台打印输出
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell))
                        + "ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        //6)关闭连接
        table.close();
    }

    /**
     * 9.查询数据（scan方式带过滤器）
     *  常用的过滤器：ValueFilter(),QualifierFilter(),FamilyFilter(),RowFilter()
     */
    //scan方式带过滤器,rowKey过滤条件
    public static void scanDataWithFilter(String tName) throws IOException {
        //1)表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建scan对象
        Scan scan = new Scan();
        //4)创建Filter对象(rowKey包含2的数据)
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("2"));//(比较的操作，比较的内容)
        //5)添加过滤器
        scan.setFilter(rowFilter);
        //6)扫描数据
        ResultScanner scanner = table.getScanner(scan);
        //7)解析数据，遍历输出
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell))
                        + "ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + "Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        //8)关闭连接
        table.close();
    }
    //scan方式带过滤器,value过滤条件
    //SingleColumnValueFilter(包含)
    //SingleColumnValueExcludeFilter（不包含）
    public static void scanDataWithFilter(String tName,String cf,String cn,String value) throws IOException {
        //1)表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建scan对象
        Scan scan = new Scan(Bytes.toBytes("1001"),Bytes.toBytes("1004"));//添加过滤的范围，减少全表扫描
        //4)创建Filter对象(rowKey包含2的数据)
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(cn), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));
        singleColumnValueFilter.setFilterIfMissing(true);//是否跳过不存在的列
        //5)添加过滤器
        //scan.setFilter(singleColumnValueFilter);

        //过滤器集合FilterList:
        //MUST_PASS_ALL(and),MUST_PASS_ONE(or)
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("1001"));

        filterList.addFilter(singleColumnValueFilter);
        filterList.addFilter(rowFilter);

        scan.setFilter(filterList);//多条件过滤
        //6)查询数据
        ResultScanner scanner = table.getScanner(scan);
        //7)解析数据，遍历输出
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell))
                            + "ColumnFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))
                            + "Qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell))
                            + "Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    /**
     * 10.删除数据
     *  a.创建表的对象
     *  b.创建删除对象
     *  c.指定列族
     *  d.删除数据
     */
    public static void deleetData(String tName,String rowKey,String cf) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //4)指定列族删除
        delete.addFamily(Bytes.toBytes(cf));

        //5)删除数据
        table.delete(delete);
        //6)关闭连接
        table.close();
    }
    //delete.addColumns();删除小于等于（删除类型：type = DeleteColumn）
    //delete.addColumn();删除一条最新数据，或指定时间戳的数据，可能会出现问题（删除类型：type = Delete）
    public static void deleetData(String tName,String rowKey,String cf,String cn) throws IOException {
        //1)检查表是否存在
        if(!isTableExist(tName)){
            System.out.println(tName + "表不存在！");
            return;
        }
        //2)创建表对象
        Table table = connection.getTable(TableName.valueOf(tName));
        //3)创建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //4)指定列族:列删除
        //delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));//删除所有
        delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn),1575968954427L);
        //delete.addColumn();

        //5)删除数据
        table.delete(delete);
        //6)关闭连接
        table.close();
    }

    /**
     * 10.关闭资源的操作
     */
    public static void close(){
        //关闭表结构的操作资源
        if(admin != null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //关闭连接资源
        if(connection != null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        //查看表是否存在
        //System.out.println(isTableExist("student"));

        //创建命名空间
        //createNameSpace("test");

        //创建表
        //createTable("stu1","info");

        //删除表
        //dropTable("tName");

        //在表中添加数据
        //updateData("stu1","1001","info","name","zhangsan");//不指定时间戳版本
        //updateData("stu1","1001","info","name","zhangxiaosan",1575968954386L);//指定时间戳版本

        //查询(get)
        //getData("stu1","1001");

        //查询(scan)
        //scanData("stu1");

        //查询(scan带过滤器)
        //scanDataWithFilter("stu1","info","name","lisi");

        //删除数据
        deleetData("stu1","1003","info","sex");

        //关闭资源
        close();
    }
}
