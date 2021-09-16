package com.suen.brady.sink;

import com.alibaba.fastjson.JSONObject;
import com.suen.brady.client.DorisClient;
import com.suen.brady.utils.SQLUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
    //定义一个状态，保存sql
    private ValueState<String> sql;
    //定义一个状态，保存列
    private ValueState<ArrayList<String>> colList;
    private Boolean isSuccess = false;
    private String databaseName;
    private String tableName;
    private JSONObject keyCols;
    private String partitionKey;
    private String distributeKey;
    private JSONObject valueCols;
    private Connection conn = null;
    private PreparedStatement preparedStatement;
    private final ArrayList<String> cols = new ArrayList<>();



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
        ValueStateDescriptor<String> sqlValueStateDescriptor = new ValueStateDescriptor<>("sql", String.class);
        ValueStateDescriptor<ArrayList<String>> colListValueStateDescriptor =
                    new ValueStateDescriptor("colList", ArrayList.class);

        flag = getRuntimeContext().getState(valueStateDescriptor);
        sql = getRuntimeContext().getState(sqlValueStateDescriptor);
        colList = getRuntimeContext().getState(colListValueStateDescriptor);

        System.out.println("开始2------");
        //获取Connection
        conn = getConnection();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        super.invoke(value,context);


        //获取状态，是否已经存在表
        if(flag.value() == null || !flag.value()){
            logger.info("check table ...");
            cols.addAll(value.keySet());
            colList.update(cols);
            sql.update(SQLUtil.getInsertSQL(databaseName,tableName,colList.value()));
            boolean existsTable = DorisClient.isExistsTable(databaseName, tableName);
            if(!existsTable){
                logger.info("create table ...");
                boolean isCreate = creatDatabase(databaseName, tableName, keyCols, partitionKey, distributeKey, valueCols);
                flag.update(isCreate);
            }else{
                flag.update(true);
            }
        }

        if(sql.value() != null ){
            preparedStatement = conn.prepareStatement(sql.value());
        }

        int i =0;
        System.out.println(colList);

        for (String key : colList.value()) {
            i++;
            preparedStatement.setObject(i,value.getString(key));
        }

        flush();
    }


    @Override
    public void close()  {
        try {
            super.close();
            if(preparedStatement != null){
                preparedStatement.close();
            }
            if(conn != null){
                conn.close();
            }
            System.out.println("结束   sink ---------");
        } catch (Exception e) {
            e.printStackTrace();
        }



    }


    public synchronized void flush(){
        //刷写重试
        final int maxRetries = 5;
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


