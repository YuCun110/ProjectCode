package com.xilishishan.hbase_kylin;

import java.sql.*;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class KylinJDBC {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        //1.连接信息
        //驱动
        String driver = "org.apache.kylin.jdbc.Driver";
        //URL
        String url = "jdbc:kylin://hadoop102:7070/FristProject";
        //用户名
        String userName = "ADMIN";
        //密码
        String passWd = "KYLIN";

        //2.注册驱动
        Class.forName(driver);
        //3.获取连接
        Connection connection = DriverManager.getConnection(url,userName,passWd);
        //4.预编译
        String sql = "SELECT deptno,sum(sal) FROM emp GROUP BY deptno";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        //5.执行
        ResultSet resultSet = preparedStatement.executeQuery();
        //6.打印输出
        while(resultSet.next()){
            System.out.println(resultSet.getDouble(1));
        }

        //7.关闭资源
        resultSet.close();
        preparedStatement.close();
        connection.close();
    }
}
