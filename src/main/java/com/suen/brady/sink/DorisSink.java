package com.suen.brady.sink;

import com.alibaba.fastjson.JSONObject;
import com.suen.brady.client.DorisClient;
import com.suen.brady.utils.SQLUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author BradySuen
 * @create_time 2021/9/14
 * @description
 **/
public class DorisSink extends DorisBaseSink<JSONObject> {
    private static final Logger logger = LoggerFactory.getLogger(DorisSink.class);
    //定义一个状态，保存当前是否与数据库链接
    private ValueState<Boolean> flag;
    //记录是否成功创建数据库,默认没有创建
    private Boolean isSuccess = false;
    private String databaseName;
    private String tableName;
    private JSONObject keyCols;
    private String partitionKey;
    private String distributeKey;
    private JSONObject valueCols;
    private Connection conn = null;
    private String sql;
    private PreparedStatement preparedStatement;
    private List<String> keyList = new ArrayList<String>();

    //刷写重试
    private int maxRetries = 5;

    //构造函数
    public DorisSink(){};
    public DorisSink(String databaseName,String tableName, JSONObject keyCols, String partitionKey,String distributeKey, JSONObject valueCols){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.keyCols = keyCols;
        this.partitionKey = partitionKey;
        this.distributeKey = distributeKey;
        this.valueCols = valueCols;
    };

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Boolean> valueStateDescriptor = new ValueStateDescriptor<>("isExistsTable", Boolean.class);
        flag = getRuntimeContext().getState(valueStateDescriptor);

        System.out.println("1111111111111");
        //获取Connection
        conn = getConnection();
    }



    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        super.invoke(value,context);

        if(flag.value() == null || flag.value()){
            //创建数据库表
            logger.info("now ,let us begin to create table ...");
            creatDatabase(databaseName, tableName, keyCols, partitionKey, distributeKey, valueCols);
            flag.update(false);
        }

        keyList.addAll(value.keySet());

        //？？？
        sql = SQLUtil.getInsertSQL(databaseName,tableName,keyList);


        System.out.println(sql);


        preparedStatement = conn.prepareStatement(sql);
        int i =0;
        for (String key : keyList) {
            i++;
            preparedStatement.setObject(i,value.getString(key));
        }
        keyList.clear();
        flush();

    }


    @Override
    public void close() throws Exception {
        super.close();
        preparedStatement.close();
        conn.close();
    }


    public synchronized void flush(){
        logger.info("begin flush");
        for(int i = 0;i < maxRetries ;i++){
            try{
                //preparedStatement.addBatch();
                preparedStatement.execute();
                break;
            }catch(Exception e){
                logger.error("error try again");
                e.printStackTrace();
            }
        }
    }


    public boolean creatDatabase(String databaseName,String tableName, JSONObject keyCols, String partitionKey,String distributeKey, JSONObject valueCols){
        //判断是否已经创建了表
        if(!isSuccess){
            logger.info("create ... ");
            try{
                isSuccess = DorisClient.createTable(databaseName, tableName, keyCols, partitionKey, distributeKey, valueCols);
                logger.info("success ...");
                return isSuccess;
            }catch(Exception e){
                logger.error("oh no fail ...");
                e.printStackTrace();
            }
        }
        return isSuccess;
    }



}
