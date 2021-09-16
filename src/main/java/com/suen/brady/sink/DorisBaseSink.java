package com.suen.brady.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author BradySuen
 * @create_time 2021/9/14
 * @description
 **/
public class DorisBaseSink<T> extends RichSinkFunction<JSONObject> {
    //连接
    private DruidDataSource dataSource;
    private Connection conn;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("开始  base sink ---------");
        initConnect();
    }

    @Override
    public void close()  {
        try {
            super.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if(conn != null){
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if(dataSource != null){
            dataSource.close();
        }
        System.out.println("关闭  base sink ---------");
    }

    public void initConnect(){
        String URL = "jdbc:mysql://host:port/";
        String DRIVER = "com.mysql.cj.jdbc.Driver";
        String USER="user";
        String PASSWORD="password";
        //创建Druid连接池
        dataSource = new DruidDataSource();
        dataSource.setUrl(URL);
        dataSource.setDriverClassName(DRIVER);
        dataSource.setUsername(USER);
        dataSource.setPassword(PASSWORD);
        dataSource.setInitialSize(2);
        dataSource.setMaxActive(10);
        dataSource.setMinIdle(10);
        dataSource.setMaxWait(2000);
        dataSource.setPoolPreparedStatements(true);
        dataSource.setMaxOpenPreparedStatements(20);
    }

    public Connection getConnection(){
        try {
            conn =  dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

}
