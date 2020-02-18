package com.xilishishan.hbase_phoenix;

import java.sql.*;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * Phoenix JDBC操作
 */
public class PhoenixJDBC {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        String driver = "org.apache.phoenix.jdbc.PhoenixDriver";//驱动
        String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
        //1注册驱动
        Class.forName(driver);
        //2.连接数据库
        Connection conn = DriverManager.getConnection(url);
        //3.预编译SQL
        String sql = "select * from bigdata";
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        //4.查询返回值
        ResultSet resultSet = preparedStatement.executeQuery();
        //5.打印输出
        while (resultSet.next()){
            System.out.println(resultSet.getString(1)
                    + "\t" + resultSet.getString(2)
                    + "\t" + resultSet.getString(3)
                    + "\t" + resultSet.getString(4));
        }
        //6.关闭资源
        resultSet.close();
        preparedStatement.close();
        conn.close();
    }
}
