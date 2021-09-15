package com.suen.brady.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author BradySuen
 * @create_time 2021/9/13
 * @description
 **/
public class SQLUtil {

    //获取字段体
    public static String getColSQL(Map<String, String> dataAndType){
        StringBuilder colSql = new StringBuilder();

        int len = dataAndType.keySet().size();
        for (String key : dataAndType.keySet()) {
            len --;
            if(len == 0){
                colSql.append("`").append(key).append("`").append(" ").append(dataAndType.get(key));
            }else{
                colSql.append("`").append(key).append("`").append(" ").append(dataAndType.get(key)).append(",").append("\n");
            }
        }
        return colSql.toString();
    }


    public static String getColSQL(List<Map<String, String>> dataAndTypeList){
        StringBuilder colSql = new StringBuilder();
        for (Map<String, String> dateAndType : dataAndTypeList) {
            for (String key : dateAndType.keySet()) {
                colSql.append("`").append(key).append("`")
                        .append(" ").append(dateAndType.get(key)).append(",").append("\n");
            }
        }
        return colSql.toString();
    }

    //获取uniqueSql
    public static String getUniqueSQL(List<String> keyCols){

        StringBuilder uniqueKey = new StringBuilder();

        for(int i = 0;i < keyCols.size();i++){
            if(i == keyCols.size() -1){
                uniqueKey.append(keyCols.get(i));
            }else{
                uniqueKey.append(keyCols.get(i)).append(",");
            }
        }
        return "unique key (" + uniqueKey.toString() + ") \n";
    }

    //获取分区sql
    public static String getPartitionSQL(String partitionKey){
        //获取当前时间
        long time = System.currentTimeMillis();
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
        String currentPartition = sdf1.format(new Date(time));
        String partitionName = "P" + sdf2.format(new Date(time));

        return String.format("partition by range(`%s`)", partitionKey) +
                " ( \n" +
                        String.format("partition %s VALUES LESS THAN (\"%s\")\n", partitionName,currentPartition) +
                ")\n";
    }


    //获取分桶SQL
    public static String getdistributeSQL(String distributeKey){
        return String.format("distributed by hash (`%s`) buckets 32\n", distributeKey) ;
    }

    //获取属性SQL
    public static String getAttributionSQL(){
        return  "properties(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_format\" = \"V2\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"DAY\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.prefix\" = \"P\",\n" +
                "\"dynamic_partition.buckets\" = \"10\",\n" +
                "\"in_memory\" = \"TRUE\" \n" +
                ")\n";
    }


    //获取insert 时的数据属性
    public static String getInsertSQL(String databaseName,String tableName,List<String> keyList){
        String sql = String.format("insert into %s.%s (", databaseName,tableName);

        StringBuilder keyColl = new StringBuilder();
        StringBuilder valueColl = new StringBuilder();
        int len = keyList.size();
        for (String key : keyList) {
            len -- ;
            if(len == 0){
                keyColl.append("`").append(key).append("`").append(")");
                valueColl.append("?").append(")");
            }else{
                keyColl.append("`").append(key).append("`").append(",");
                valueColl.append("?").append(",");
            }
        }

        sql += ( keyColl +  "values (" + valueColl );
        return sql;
    }
    

}
