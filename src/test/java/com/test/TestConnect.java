package com.test;

import java.sql.*;

/**
 * @author BradySuen
 * @create_time 2021/9/16
 * @description
 **/
public class TestConnect {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        String URL="jdbc:mysql://doris.nana666.com:9030/";
        String USER="root";
        String PASSWORD="upup666";
        Connection conn;
        Statement st;
        //加载驱动器
        Class.forName("com.mysql.cj.jdbc.Driver");
        //获取连接
        conn = DriverManager.getConnection(URL,USER,PASSWORD);
        //连接数据库
        st = conn.createStatement();

        String sql = String.format("select count(*) from information_schema.tables where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s';",
                "test","test11");
        System.out.println(sql);
        ResultSet resultSet = st.executeQuery(sql);
        resultSet.next();
        int row = resultSet.getRow();

        System.out.println(resultSet.getString(1));


    }
}
