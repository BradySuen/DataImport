package com.suen.brady.client;


import com.alibaba.fastjson.JSONObject;
import com.suen.brady.utils.SQLUtil;
import com.suen.brady.utils.SimpleName;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author BradySuen
 * @create_time 2021/9/13
 * @description   连接Doris并创建表
 **/
public class DorisClient {
    private static final Logger logger = LoggerFactory.getLogger(DorisClient.class);

    private static String URL="jdbc:mysql://doris.nana666.com:9030/";
    private static String USER="root";
    private static String PASSWORD="upup666";
    private static Connection conn;
    private static Statement st;

    static{
        try{
            //加载驱动器
            Class.forName("com.mysql.cj.jdbc.Driver");
            //获取连接
            conn = DriverManager.getConnection(URL,USER,PASSWORD);
            //连接数据库
            st = conn.createStatement();

        }catch(Exception e){
            logger.error("can not connect database ...");
            e.printStackTrace();
        }
    }


    //创建数据库
    public static void createDatabase(String databaseName){
        try{
            st.execute(String.format("create database if not exists %s ;",databaseName));
        }
        catch(Exception e){
            logger.error("create database failed !");
        }
    }


    //创建数据表
    /**
     * key
     *  ① 保证有序插入
     *  ② 保证key的前 3 列数据类型大小不超过32 即使用 int  bigint date 等类型，而不使用 varchar类型 (默认)
     * 分区字段，一般包含在key中
     * 这些列所对应的数据大小不能过大
     */

    /**
     * para：
     *      tableName : 需要创建表的表名称
     *      keyCols : doris表中作为前缀索引的key值,如果是unique,那么就是UniqueKey,key需要注意：
     *                ① 传入的key是在原格式下固定的,一般是内置的属性字段
     *                ② 保证key的前 3 列数据类型大小不超过32 即使用 int  bigint date 等类型，而不使用 varchar类型 (默认)
     *      partitionKey : 作为分区的分区字段,一定是key中的数据
     *      distributeKey : 作为分桶的分桶字段,一定是key中的数据
     *      jsonObject : 内置属性或者用户自定义的属性,或者两者皆有
     * example:
     *  如果有如下数据
     *      {
     *         "#account_id": "ABCDEFG-123-abc",
     *         "#distinct_id": "F53A58ED-E5DA-4F18-B082-7E1228746E88",
     *         "#event_name": "test",
     *         "properties": {
     *                          "#lib": "LogBus",
     *                          "#lib_version": "1.0.0",
     *                          "#screen_height": 1920,
     *                          "#screen_width": 1080,
     *                          "argString": "abc",
     *                          "argNum": 123,
     *                          "argBool": true
     *                  },
     *         "#type": "track",
     *         "#ip": "192.168.171.111",
     *         "#time": "2017-12-18 14:37:28.527"
     *
     *      }
     *   那么带有 "#" 号的可以看成是 key 数据
     *   properties 中的数据 作为 jsonObject
     */
    public static Boolean createTable(String databaseName,String tableName, JSONObject keyCols, String partitionKey,String distributeKey, JSONObject valueCols){
        try{
            //获取KeyList(key有序存在)
            List<String> keyList = new ArrayList<>(keyCols.keySet());
            //获取key的数据及其类型
            Map<String, String> keyDataAndType1 = SimpleName.getDataAndType(keyCols);

            //将key及其类型进行有序打包，顺序与keyList一致
            List<Map<String,String>> keyDataAndType = new ArrayList<Map<String,String>>();
            for (String key1 : keyList) {
                for (String key2 : keyDataAndType1.keySet()) {
                    if(key1.equals(key2)){
                        Map<String,String> map2 = new HashMap<String,String>();
                        map2.put(key1,keyDataAndType1.get(key1));
                        keyDataAndType.add(map2);
                    }
                }
            }

            //jsonObject包含了内置属性和用户自定义属性
            //获取数据以及数据类型并构造sql
            Map<String, String> valueDataAndType = SimpleName.getDataAndType(valueCols);

            String sql = String.format("create table if not exists %s.%s (", databaseName,tableName)
                    + SQLUtil.getColSQL(keyDataAndType)
                    + SQLUtil.getColSQL(valueDataAndType) + ")"
                    + "ENGINE=olap \n"
                    + SQLUtil.getUniqueSQL(keyList)
                    + SQLUtil.getPartitionSQL(partitionKey)
                    + SQLUtil.getdistributeSQL(distributeKey)
                    + SQLUtil.getAttributionSQL() + ";";
            System.out.println(sql);
            st.execute(sql);
            return true;
        }catch(Exception e){
            logger.error("create table {0} failed",tableName);
            return false;
        }finally {
            try {
                st.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static Boolean createTable(String databaseName,String tableName, JSONObject keyCols,String distributeKey,JSONObject jsonObject){
        return createTable(databaseName,tableName,keyCols,"partition_date",distributeKey,jsonObject);
    }
}
