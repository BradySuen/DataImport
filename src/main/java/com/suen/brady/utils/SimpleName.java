package com.suen.brady.utils;

import com.alibaba.fastjson.JSONObject;
import com.suen.brady.statement.datatype.DataType;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author BradySuen
 * @create_time 2021/9/13
 * @description
 **/
public class SimpleName {

    public static Map<String,String> getDataAndType(JSONObject jsonObject){
        Map<String,String> attrMap  = new HashMap<String,String>();
        //获取json的key
        Set<String> keySet = jsonObject.keySet();
        for (String key : keySet) {

            //反射获取key对应的类型，如果是不确定的则反回 空字符串 数组类型反回 []
            String simpleName = jsonObject.get(key).getClass().getSimpleName();
            if("partition_date".equals(key)){simpleName = "Date";}
            String colType;
            switch(simpleName){
                case "Boolean" : colType = DataType.BOOLEAN.getValue();break;
                case "Integer" : colType = DataType.INT.getValue();break;
                case "Date" : colType = DataType.DATE.getValue();break;
                case "BigDecimal" : colType = DataType.DECIMAL.getValue();break;
                case "String" :
                    //获取数据大小 ,至少保证128的双倍
                    int len = jsonObject.get(key) != null ? (512 + jsonObject.get(key).toString().getBytes(StandardCharsets.UTF_8).length) * 2 : 20480;
                    colType = String.format("VARCHAR(%s)", len);break;
                case "Long" : colType = DataType.BIGINT.getValue();break;
                default :
                    colType = DataType.VARCHAR512.getValue();
            }
            attrMap.put(key, colType);
        }
            return attrMap;
    }
}
